"""DexScreener (lecture seule, sans clé) : pool, market cap, liquidité, réseaux sociaux."""
from __future__ import annotations

import asyncio
import logging

import aiohttp

log = logging.getLogger("dexscreener")


async def markets(session: aiohttp.ClientSession, mints: list[str]) -> dict[str, dict]:
    """MC et liquidité de plusieurs tokens (30 par appel, meilleure paire de chacun)."""
    out: dict[str, dict] = {}
    for i in range(0, len(mints), 30):
        lot = mints[i:i + 30]
        try:
            async with session.get(f"https://api.dexscreener.com/tokens/v1/solana/{','.join(lot)}",
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                pairs = await r.json(content_type=None) if r.status == 200 else []
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            log.debug("DexScreener : %s", e)
            continue
        for p in pairs if isinstance(pairs, list) else []:
            mint = (p.get("baseToken") or {}).get("address")
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if mint and liq >= (out.get(mint) or {}).get("liquidity", -1):
                out[mint] = {"mc": p.get("marketCap") or p.get("fdv"), "liquidity": liq,
                             "volume24h": (p.get("volume") or {}).get("h24"),
                             "txns24h": sum(((p.get("txns") or {}).get("h24") or {}).get(k) or 0 for k in ("buys", "sells"))}
        await asyncio.sleep(0.3)
    return out


async def token_pairs(session: aiohttp.ClientSession, mint: str) -> dict | None:
    """Meilleure paire (plus grosse liquidité) d'un token, ou None s'il n'a pas encore de pool."""
    url = f"https://api.dexscreener.com/tokens/v1/solana/{mint}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            pairs = await r.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        log.debug("DexScreener : %s", e)
        return None
    if not isinstance(pairs, list) or not pairs:
        return None
    best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
    info = best.get("info") or {}
    return {
        "dex": best.get("dexId"),
        "url": best.get("url"),
        "mc": best.get("marketCap") or best.get("fdv"),
        "liquidity": (best.get("liquidity") or {}).get("usd"),
        "pair_created": (best.get("pairCreatedAt") or 0) // 1000,
        # Activité des 5 dernières minutes (contrôle d'une alerte « à ne pas rater »)
        "buys5": ((best.get("txns") or {}).get("m5") or {}).get("buys"),
        "sells5": ((best.get("txns") or {}).get("m5") or {}).get("sells"),
        "change5": (best.get("priceChange") or {}).get("m5"),
        "socials": {s.get("type"): s.get("url") for s in info.get("socials") or []},
        "website": next((w.get("url") for w in info.get("websites") or []), None),
    }
