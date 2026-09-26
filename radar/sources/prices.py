"""Prix du SOL en dollars (gratuit, sans clé), mis en cache 5 min.

Sert à recalculer la market cap pump.fun (le champ `usd_market_cap` est parfois absurde).
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

log = logging.getLogger("prices")

CACHE_S = 300
SOURCES = [
    ("https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT", lambda d: d.get("price")),
    ("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
     lambda d: (d.get("solana") or {}).get("usd")),
]

_cache: tuple[float, float] | None = None
_lock = asyncio.Lock()


async def sol_usd(http: aiohttp.ClientSession) -> float | None:
    """Prix du SOL en $ (None si aucune source ne répond ; on garde alors l'ancien prix s'il existe)."""
    global _cache
    if _cache and time.time() - _cache[0] < CACHE_S:
        return _cache[1]
    async with _lock:
        if _cache and time.time() - _cache[0] < CACHE_S:
            return _cache[1]
        for url, pick in SOURCES:
            try:
                async with http.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status != 200:
                        continue
                    prix = float(pick(await r.json(content_type=None)) or 0)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as e:
                log.debug("Prix SOL indisponible sur %s : %s", url, e)
                continue
            if prix > 0:
                _cache = (time.time(), prix)
                return prix
    return _cache[1] if _cache else None
