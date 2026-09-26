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
    # Indicateurs de rug (vus en vrai sur des dizaines de tokens morts en quelques minutes)
    ath_usd: float | None = None
    ath_ts: int | None = None        # heure de l'ATH (quelques secondes après la création = achat groupé)
    curve_sol: float | None = None   # SOL réellement déposés par des acheteurs dans la bonding curve
    top10_pct: float | None = None   # part des 10 plus gros détenteurs (hors bonding curve / pool)
    dev_pct: float | None = None     # part encore détenue par le créateur
    pumpfun_ok: bool = False         # fiche pump.fun obtenue
    network: str | None = None       # résumé du réseau du dev (radar/analysis/network.py)

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
FACTORY_MIN_24H = 3        # 3 tokens ou plus créés par le même wallet en 24 h
SERIAL_MAX_ATH = 50_000


TOP10_MAX_PCT = 35
DEV_MAX_PCT = 10
CURVE_MIN_SOL = 2.0
BUNDLE_ATH_S = 15


async def holders(rpc: SolanaRPC, mint: str, supply_raw: int, creator: str | None) -> tuple[float, float] | None:
    """(part des 10 plus gros détenteurs, part du créateur), hors comptes de programmes (bonding curve, pool)."""
    comptes = await rpc.token_largest_accounts(mint)
    if not comptes:
        return None
    res = await rpc.call("getMultipleAccounts", [[a["address"] for a in comptes], {"encoding": "jsonParsed"}])
    par_proprio: dict[str, float] = {}
    for a, v in zip(comptes, (res or {}).get("value", [])):
        try:
            owner = v["data"]["parsed"]["info"]["owner"]
        except (TypeError, KeyError):
            continue
        par_proprio[owner] = par_proprio.get(owner, 0.0) + 100 * int(a["amount"]) / supply_raw
    if not par_proprio:
        return None
    proprios = list(par_proprio)
    res = await rpc.call("getMultipleAccounts", [proprios, {"encoding": "base64"}])
    SYSTEM = "11111111111111111111111111111111"
    humains: dict[str, float] = {}
    for o, v in zip(proprios, (res or {}).get("value", [])):
        if v is None or v.get("owner") == SYSTEM:   # wallet normal (pas une bonding curve ni un pool)
            humains[o] = par_proprio[o]
    top10 = sum(sorted(humains.values(), reverse=True)[:10])
    return round(top10, 1), round(humains.get(creator or "", 0.0), 1)


def _rug_flags(info: TokenInfo) -> None:
    """Signaux vus en vrai sur des tokens morts en quelques minutes (fermes de bots, bundles)."""
    def add(f: str) -> None:
        if f not in info.flags:
            info.flags.append(f)

    if info.top10_pct is not None and info.top10_pct >= TOP10_MAX_PCT:
        add(f"top 10 des détenteurs = {info.top10_pct:.0f} % de la supply (hors bonding curve) : risque de dump")
    if info.dev_pct is not None and info.dev_pct >= DEV_MAX_PCT:
        add(f"le dev détient encore {info.dev_pct:.0f} % de la supply")
    if info.ath_usd and info.mc_usd and info.mc_usd < 0.5 * info.ath_usd and info.created_ts:
        if info.ath_ts and info.ath_ts - info.created_ts <= BUNDLE_ATH_S:
            add(f"ATH atteint {max(0, info.ath_ts - info.created_ts)} s après la création puis −"
                f"{100 * (1 - info.mc_usd / info.ath_usd):.0f} % : achat groupé au lancement puis revente")
        elif info.age_s is not None and info.age_s < 6 * 3600:
            add(f"déjà −{100 * (1 - info.mc_usd / info.ath_usd):.0f} % depuis son ATH")
    if info.on_pump_curve and info.curve_sol is not None and info.curve_sol < CURVE_MIN_SOL \
            and info.age_s is not None and info.age_s > 180:
        add(f"presque aucun acheteur réel : {info.curve_sol:.1f} SOL dans la bonding curve après {info.age_s // 60} min")


def missing_data(info: TokenInfo) -> list[str]:
    """Ce qui manque pour juger un token. Sans ces données, il n'est JAMAIS présenté comme sûr."""
    manque = []
    if not info.creator:
        manque.append("créateur inconnu")
    if info.mc_usd is None:
        manque.append("market cap inconnue")
    if info.dev_coins is None:
        manque.append("historique du dev indisponible")
    if info.supply_raw and info.top10_pct is None:
        manque.append("répartition des détenteurs inconnue")
    if info.mint.endswith("pump") and not info.pumpfun_ok:
        manque.append("fiche pump.fun indisponible")
    return manque


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
    # Usine à tokens (vu en vrai : 6 tokens de chat en 2 h par le même wallet, tous retombés à 3 k$)
    recents = [c for c in coins if c.get("created") and time.time() - c["created"] < 24 * 3600]
    usine = f"lanceur en série : {len(recents) + 1} tokens créés en 24 h (usine à tokens)"
    if len(recents) + 1 >= FACTORY_MIN_24H and not any(f.startswith("lanceur en série : ") and "24 h" in f
                                                       for f in info.flags):
        info.flags.append(usine)


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


async def _deployer(rpc: SolanaRPC, mint: str) -> str | None:
    """Payeur de la première transaction réussie du contrat (None si trop d'historique)."""
    sigs, truncated = await rpc.all_signatures(mint, max_pages=3)
    if truncated or not sigs:
        return None
    for s in reversed(sigs):
        if s.get("err") is None:
            tx = await rpc.transaction(s["signature"])
            if tx:
                k = tx["transaction"]["message"]["accountKeys"][0]
                return k["pubkey"] if isinstance(k, dict) else k
    return None


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
        if with_dev_history and info.supply_raw and info.top10_pct is None:
            try:
                res = await asyncio.wait_for(holders(rpc, mint, info.supply_raw, info.creator), 10)
            except Exception:
                res = None
            if res:
                info.top10_pct, info.dev_pct = res
                _rug_flags(info)
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
        info.ath_usd, info.ath_ts, info.curve_sol = pf.get("ath"), pf.get("ath_ts"), pf.get("curve_sol")
        info.pumpfun_ok = True
        if pf.get("banned"):
            info.flags.append("token masqué par pump.fun (signalé)")

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
    if with_dev_history and not info.creator:
        # Hors pump.fun (vu en vrai : $ASH sur Raydium), pas de fiche avec le créateur : on prend le payeur
        # de la toute première transaction du contrat
        info.creator = await safe(_deployer(rpc, mint))

    if with_dev_history and info.creator:
        coins = await safe(pumpfun.coins_by_creator(http, info.creator))
        if coins is not None:
            info.dev_coins = [c for c in coins if c["mint"] != mint]

    # Concentration des détenteurs, pour tout token qui fait l'objet d'une alerte (vu en vrai : $ASH, contrat
    # créé la veille de son lancement, sortait « répartition inconnue » et n'arrivait pas dans ‼️)
    if with_dev_history and info.supply_raw:
        res = await safe(holders(rpc, mint, info.supply_raw, info.creator))
        if res:
            info.top10_pct, info.dev_pct = res

    # Drapeaux rouges
    _dev_flags(info)
    _rug_flags(info)
    if info.mint_authority:
        info.flags.append("mint authority active (le dev peut imprimer des tokens)")
    if info.freeze_authority:
        info.flags.append("freeze authority active (le dev peut bloquer les ventes)")
    if info.mc_usd and info.liquidity_usd and info.mc_usd > 0 and info.liquidity_usd / info.mc_usd < 0.03:
        info.flags.append(f"liquidité très faible vs MC ({100 * info.liquidity_usd / info.mc_usd:.1f} %) : MC gonflée ?")

    _cache[mint] = (time.time(), info)
    return info
