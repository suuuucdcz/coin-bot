"""Liaison token -> lancement : métadonnées, réseaux sociaux, pool, market cap, historique du dev.

Toutes les sources sont interrogées en parallèle avec des délais courts : l'alerte doit partir vite.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from ..sources import dexscreener, pumpfun
from ..sources.helius import SolanaRPC
from .xlinks import profile_handle

log = logging.getLogger("enrich")

CACHE_S = 45
CROWDED_TX = 400        # ≥ 400 transactions = beaucoup de monde dessus
CROWDED_MC = 500_000    # ou MC ≥ 500 k$


@dataclass
class TokenInfo:
    mint: str
    name: str | None = None
    symbol: str | None = None
    creator: str | None = None
    created_ts: int | None = None
    supply_raw: int | None = None
    decimals: int = 0
    mint_authority: str | None = None
    freeze_authority: str | None = None
    twitter: str | None = None
    telegram: str | None = None
    website: str | None = None
    mc_usd: float | None = None
    liquidity_usd: float | None = None
    has_pool: bool = False
    pool_dex: str | None = None
    dex_url: str | None = None
    on_pump_curve: bool = False      # encore sur la bonding curve pump.fun
    dev_coins: list[dict] | None = None
    tx_count: int | None = None      # nb de transactions sur le token (plafonné à 1000)
    flags: list[str] = field(default_factory=list)

    @property
    def crowded(self) -> bool:
        """Déjà lancé ET callé : beaucoup de monde dessus -> trop tard, pas d'alerte « entrée »."""
        return bool((self.tx_count or 0) >= CROWDED_TX or (self.mc_usd or 0) >= CROWDED_MC)

    @property
    def age_s(self) -> int | None:
        return int(time.time()) - self.created_ts if self.created_ts else None

    def pct_supply(self, raw: int) -> float | None:
        return 100.0 * raw / self.supply_raw if self.supply_raw else None


_cache: dict[str, tuple[float, TokenInfo]] = {}


def purge_cache() -> None:
    """Vide les fiches expirées (le radar tourne des semaines sans redémarrer)."""
    now = time.time()
    for mint in [m for m, (t, _i) in _cache.items() if now - t >= CACHE_S]:
        _cache.pop(mint, None)


def x_handle(url: str | None) -> str | None:
    """Compte X d'un lien de PROFIL (x.com/AshbornCoin). None pour un tweet, une communauté, une recherche.

    Un lien vers un tweet (x.com/elonmusk/status/…) ne désigne PAS le compte du projet : les faux tokens
    collent souvent le tweet d'un gros compte ou le tweet d'annonce du vrai projet.
    """
    return profile_handle(url)


SERIAL_MIN_COINS = 3
SERIAL_MAX_ATH = 50_000


def _dev_flags(info: TokenInfo) -> None:
    """Presque tous les memecoins finissent à −99 % : un ancien token « mort » ne prouve pas un rug.
    Le drapeau n'est levé que pour un lanceur en série dont AUCUN token n'a jamais décollé."""
    coins = info.dev_coins or []
    morts = sum(1 for c in coins if c["rug"])
    best = max((c["ath"] or 0 for c in coins), default=0)
    flag = (f"lanceur en série : {len(coins)} anciens tokens, tous morts, aucun au-dessus de "
            f"{SERIAL_MAX_ATH // 1000} k$ d'ATH")
    if len(coins) >= SERIAL_MIN_COINS and morts == len(coins) and best < SERIAL_MAX_ATH and flag not in info.flags:
        info.flags.append(flag)


async def _json_meta(http: aiohttp.ClientSession, uri: str | None) -> dict:
    if not uri:
        return {}
    try:
        async with http.get(uri, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                return d if isinstance(d, dict) else {}
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        pass
    return {}


async def _oldest_ts(rpc: SolanaRPC, mint: str) -> int | None:
    """Date de création d'un mint = sa plus ancienne tx (None si trop d'historique = token ancien)."""
    sigs, truncated = await rpc.all_signatures(mint, max_pages=3)
    if truncated or not sigs:
        return None
    return sigs[-1].get("blockTime")


async def token_info(rpc: SolanaRPC, http: aiohttp.ClientSession, mint: str, creator_hint: str | None = None,
                     with_dev_history: bool = True) -> TokenInfo:
    hit = _cache.get(mint)
    if hit and time.time() - hit[0] < CACHE_S:
        info = hit[1]
        info.creator = info.creator or creator_hint
        if with_dev_history and info.dev_coins is None and info.creator:
            # La fiche en cache a été faite sans l'historique du dev : on le complète
            try:
                coins = await asyncio.wait_for(pumpfun.coins_by_creator(http, info.creator), 10)
            except Exception:
                coins = None
            if coins is not None:
                info.dev_coins = [c for c in coins if c["mint"] != mint]
                _dev_flags(info)
        return info
    info = TokenInfo(mint)

    async def safe(coro):
        try:
            return await asyncio.wait_for(coro, 10)
        except Exception as e:
            log.debug("enrichissement %s : %s", mint[:6], e)
            return None

    pf, dx, mi, asset, sigs = await asyncio.gather(
        safe(pumpfun.coin(http, mint)), safe(dexscreener.token_pairs(http, mint)),
        safe(rpc.mint_info(mint)), safe(rpc.asset(mint)), safe(rpc.signatures(mint, limit=1000)))
    info.tx_count = len(sigs) if sigs is not None else None

    if mi:
        info.supply_raw = int(mi.get("supply") or 0) or None
        info.decimals = mi.get("decimals") or 0
        info.mint_authority = mi.get("mintAuthority")
        info.freeze_authority = mi.get("freezeAuthority")

    meta_json: dict = {}
    if asset:
        md = (asset.get("content") or {}).get("metadata") or {}
        info.name, info.symbol = md.get("name") or None, md.get("symbol") or None
        if not pf:  # pump.fun a déjà les liens ; sinon on lit le JSON de métadonnées
            meta_json = await safe(_json_meta(http, (asset.get("content") or {}).get("json_uri"))) or {}
            ext = meta_json.get("extensions") or {}
            info.twitter = meta_json.get("twitter") or ext.get("twitter")
            info.telegram = meta_json.get("telegram") or ext.get("telegram")
            info.website = meta_json.get("website") or ext.get("website")

    if pf:
        info.name = pf.get("name") or info.name
        info.symbol = pf.get("symbol") or info.symbol
        info.creator = pf.get("creator")
        info.created_ts = pf.get("created") or None
        info.twitter = pf.get("twitter") or info.twitter
        info.telegram = pf.get("telegram") or info.telegram
        info.website = pf.get("website") or info.website
        info.mc_usd = pf.get("mc")
        info.on_pump_curve = not pf.get("complete")

    if dx:
        info.has_pool = True
        info.pool_dex = dx["dex"]
        info.dex_url = dx["url"]
        info.liquidity_usd = dx["liquidity"]
        if dx["mc"]:
            info.mc_usd = dx["mc"]  # DexScreener prioritaire : la MC pump.fun est parfois absurde
        info.twitter = info.twitter or dx["socials"].get("twitter")
        info.telegram = info.telegram or dx["socials"].get("telegram")
        info.website = info.website or dx["website"]
        if not info.created_ts and dx["pair_created"]:
            info.created_ts = dx["pair_created"]
    elif info.on_pump_curve:
        info.has_pool, info.pool_dex = True, "pump.fun (bonding curve)"

    if not info.created_ts:
        info.created_ts = await safe(_oldest_ts(rpc, mint))
    info.creator = info.creator or creator_hint

    if with_dev_history and info.creator:
        coins = await safe(pumpfun.coins_by_creator(http, info.creator))
        if coins is not None:
            info.dev_coins = [c for c in coins if c["mint"] != mint]

    # Drapeaux rouges
    _dev_flags(info)
    if info.mint_authority:
        info.flags.append("mint authority active (le dev peut imprimer des tokens)")
    if info.freeze_authority:
        info.flags.append("freeze authority active (le dev peut bloquer les ventes)")
    if info.mc_usd and info.liquidity_usd and info.mc_usd > 0 and info.liquidity_usd / info.mc_usd < 0.03:
        info.flags.append(f"liquidité très faible vs MC ({100 * info.liquidity_usd / info.mc_usd:.1f} %) : MC gonflée ?")

    _cache[mint] = (time.time(), info)
    return info
