"""API pump.fun (lecture seule) : fiche d'un token, anciens tokens d'un créateur, listes de tokens."""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from .prices import sol_usd

log = logging.getLogger("pumpfun")

BASE = "https://frontend-api-v3.pump.fun"
HEADERS = {
    # Sans en-têtes de navigateur, l'API renvoie parfois 403.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://pump.fun",
    "Referer": "https://pump.fun/",
}
# Au-delà, la market cap renvoyée par pump.fun est jugée absurde (voir CLAUDE.md).
# Attention : 46 M$ a été vu en vrai (WSOS, confirmé DexScreener) -> seuil large.
MC_MAX_CREDIBLE = 1_000_000_000
# Un token encore sur la bonding curve ne peut pas valoir plus : il aurait déjà migré.
CURVE_MC_MAX = 2_000_000
# Écart toléré entre `usd_market_cap` et la MC recalculée (MC en SOL × prix du SOL)
MC_RATIO_MAX = 3.0


def clean_mc(value) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if 0 <= v <= MC_MAX_CREDIBLE else None


async def coins_by_creator(session: aiohttp.ClientSession, creator: str, limit: int = 50) -> list[dict] | None:
    """Tokens créés par une adresse. None si l'API ne répond pas (on ne bloque jamais le traçage)."""
    return await _list(session, f"{BASE}/coins?creator={creator}&limit={limit}&offset=0&includeNsfw=true")


async def list_coins(session: aiohttp.ClientSession, sort: str = "created_timestamp", complete: bool = True,
                     offset: int = 0, limit: int = 50) -> list[dict] | None:
    """Liste de tokens pump.fun (sort : market_cap, last_trade_timestamp ou created_timestamp)."""
    url = (f"{BASE}/coins?offset={offset}&limit={limit}&includeNsfw=false&sort={sort}&order=DESC"
           f"&complete={'true' if complete else 'false'}")
    return await _list(session, url)


async def _list(session: aiohttp.ClientSession, url: str) -> list[dict] | None:
    for tentative in range(3):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 429 or r.status >= 500:
                    await asyncio.sleep(2 * (tentative + 1))
                    continue
                if r.status != 200:
                    log.debug("pump.fun %s pour %s", r.status, url)
                    return None
                data = await r.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            log.debug("pump.fun erreur réseau : %s", e)
            await asyncio.sleep(2 * (tentative + 1))
            continue
        coins = data if isinstance(data, list) else data.get("coins", []) if isinstance(data, dict) else []
        prix = await sol_usd(session)
        return [_coin(c, prix) for c in coins if isinstance(c, dict)]
    return None


async def coin(session: aiohttp.ClientSession, mint: str) -> dict | None:
    """Fiche d'un token pump.fun. Cette route renvoie parfois des erreurs : None dans ce cas."""
    try:
        async with session.get(f"{BASE}/coins/{mint}", headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            data = await r.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return None
    if not isinstance(data, dict) or not data.get("mint"):
        return None
    out = _coin(data, await sol_usd(session))
    out.update({"telegram": data.get("telegram"),
                "website": data.get("website"), "pool": data.get("pump_swap_pool") or data.get("raydium_pool")})
    return out


def _mc_sol(c: dict) -> float | None:
    """Market cap en SOL : champ `market_cap`, sinon recalculée depuis les réserves de la bonding curve."""
    try:
        if c.get("market_cap"):
            return float(c["market_cap"])
        vsol, vtok, supply = (float(c.get(k) or 0) for k in
                              ("virtual_sol_reserves", "virtual_token_reserves", "total_supply"))
        if vsol and vtok and supply:
            return vsol / vtok * supply / 1e9  # lamports / unités brutes (mêmes décimales des deux côtés)
    except (TypeError, ValueError):
        pass
    return None


def market_cap_usd(c: dict, sol_price: float | None) -> float | None:
    """MC en $ fiable : `usd_market_cap` recoupée avec MC en SOL × prix du SOL."""
    mc = clean_mc(c.get("usd_market_cap"))
    mc_sol = _mc_sol(c)
    if sol_price and mc_sol:
        recalc = mc_sol * sol_price
        if mc is None or not (recalc / MC_RATIO_MAX <= mc <= recalc * MC_RATIO_MAX):
            mc = clean_mc(recalc)
    if mc is not None and c.get("complete") is False and mc > CURVE_MC_MAX:
        return None  # encore sur la bonding curve : une telle valeur est impossible
    return mc


# ATH invraisemblable : plus de 100 M$ pour un token de moins de 3 jours (vu en vrai : 482 M$, 407 M$…
# renvoyés par pump.fun pour des tokens sans liquidité). On l'ignore plutôt que de le croire.
YOUNG_ATH_MAX = 100_000_000
YOUNG_S = 3 * 86400


def credible_ath(ath: float | None, created_s: int | None) -> float | None:
    if ath and created_s and time.time() - created_s < YOUNG_S and ath > YOUNG_ATH_MAX:
        return None
    return ath


def _coin(c: dict, sol_price: float | None = None) -> dict:
    mc = market_cap_usd(c, sol_price)
    created = (c.get("created_timestamp") or 0) // 1000
    ath = credible_ath(clean_mc(c.get("ath_market_cap")), created)
    # Vu en pratique : VSOF (Reserve) ATH 12,2 M$ puis MC 2 k$ = pic puis rug.
    # Chute > 99 % depuis l'ATH -> « rug probable ».
    drop = (1 - mc / ath) if (ath and mc is not None and ath > 0) else None
    return {
        "mint": c.get("mint"),
        "creator": c.get("creator"),
        "symbol": c.get("symbol"),
        "name": c.get("name"),
        "created": created,
        "ath": ath,
        "drop": drop,
        "rug": drop is not None and drop > 0.99,
        "mc": mc,
        "complete": c.get("complete"),
        "twitter": c.get("twitter"),
    }
