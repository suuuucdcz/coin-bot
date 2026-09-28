"""GeckoTerminal (gratuit, sans clé) : historique de prix d'un token, pour savoir COMMENT il est tombé.

Un rug et un coin qui s'éteint finissent tous les deux à −99 %, mais pas à la même vitesse. Mesuré le 28/09 sur
51 rugs : de plus de 50 % du plus haut à moins de 10 % en 15 min (médiane), 30 min au plus. Un coin qui a vécu
descend en heures ou en jours : ce n'est pas un rug (il peut même avoir été un vrai succès).

Bougies de 15 min, courbe pump.fun + pool après migration, converties en market cap. Limite gratuite : 30 requêtes
par minute (une toutes les 2,2 s ici, pour tout le radar).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp

log = logging.getLogger("geckoterminal")

BASE = "https://api.geckoterminal.com/api/v2/networks/solana"
PAUSE_S = 2.2
BOUGIE_S = 900
CHUTE_BRUTALE_S = 3600       # de ≥ 50 % à ≤ 10 % du plus haut en 1 h au plus = rug (mesuré : 15 à 30 min)
PUMP_SUPPLY = 1_000_000_000

_verrou = asyncio.Lock()
_dernier = 0.0


async def _get(http: aiohttp.ClientSession, url: str) -> dict | None:
    global _dernier
    for essai in range(3):
        async with _verrou:
            attente = _dernier + PAUSE_S - time.monotonic()
            if attente > 0:
                await asyncio.sleep(attente)
            _dernier = time.monotonic()
        try:
            async with http.get(url, headers={"Accept": "application/json"},
                                timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 429:
                    await asyncio.sleep(20 * (essai + 1))
                    continue
                return await r.json(content_type=None) if r.status == 200 else None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            log.debug("GeckoTerminal : %s", e)
            await asyncio.sleep(3)
    return None


async def bougies(http: aiohttp.ClientSession, mint: str, fin: int | None = None) -> list[tuple]:
    """[(ts, ouverture, haut, bas, clôture, volume $)] en market cap $, bougies de 15 min, les ~10 jours qui finissent
    à `fin` (maintenant par défaut). Courbe pump.fun et pool principal fusionnés (le plus actif par quart d'heure)."""
    d = await _get(http, f"{BASE}/tokens/{mint}/pools?page=1")
    pools = (d or {}).get("data") or []
    pools = sorted(pools, key=lambda p: (p["relationships"]["dex"]["data"]["id"] != "pump-fun",
                                         -float(p["attributes"].get("reserve_in_usd") or 0)))[:2]
    out: dict[int, tuple] = {}
    for p in pools:
        a = p["attributes"]
        prix, fdv = float(a.get("base_token_price_usd") or 0), float(a.get("fdv_usd") or 0)
        supply = fdv / prix if prix and fdv else PUMP_SUPPLY
        url = f"{BASE}/pools/{a['address']}/ohlcv/minute?aggregate=15&limit=1000&currency=usd&token=base"
        o = await _get(http, url + (f"&before_timestamp={int(fin)}" if fin else ""))
        for t, op, hi, lo, cl, vol in ((o or {}).get("data") or {}).get("attributes", {}).get("ohlcv_list", []):
            k = int(t)
            if k not in out or vol > out[k][5]:
                out[k] = (k, op * supply, hi * supply, lo * supply, cl * supply, vol)
    return [out[k] for k in sorted(out)]


def chute(b: list[tuple]) -> dict | None:
    """Plus haut et vitesse de la chute qui a suivi. None sans données.

    brutale : de ≥ 50 % du plus haut à ≤ 10 % en CHUTE_BRUTALE_S au plus (rug) ; lente : ≤ 10 % atteint plus
    lentement (le coin a vécu puis s'est éteint) ; aucune : jamais redescendu sous 10 % du plus haut."""
    if not b:
        return None
    i = max(range(len(b)), key=lambda k: b[k][2])
    pic_ts, pic = b[i][0], b[i][2]
    apres = b[i:]
    t10 = next((x[0] for x in apres if x[3] <= 0.10 * pic), None)
    if t10 is None:
        return {"pic": pic, "pic_ts": pic_ts, "chute": "aucune", "duree": None}
    t50 = max(x[0] for x in apres if x[0] <= t10 and x[2] >= 0.5 * pic)
    duree = t10 - t50 + BOUGIE_S
    return {"pic": pic, "pic_ts": pic_ts, "chute": "brutale" if duree <= CHUTE_BRUTALE_S else "lente", "duree": duree}


async def chute_token(http: aiohttp.ClientSession, db, mint: str, fin: int | None = None) -> dict | None:
    """chute() d'un token, mémorisée en base quand elle est définitive (brutale ou lente) : un token mort ne change
    plus, et il revient dans le réseau de beaucoup de devs."""
    cle = f"chute:{mint}"
    if db is not None and (v := db.get(cle)):
        return json.loads(v)
    r = chute(await bougies(http, mint, fin))
    if r and r["chute"] != "aucune" and db is not None:
        db.put(cle, json.dumps(r))
    return r
