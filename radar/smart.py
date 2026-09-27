"""🧠 Smart money : les wallets qu'on retrouve parmi les plus gros détenteurs de PLUSIEURS vrais succès, de devs
différents. Quand plusieurs entrent ensemble dans un token jeune, c'est un signal (alerte « 🧠 SMART MONEY »).

Les succès viennent de la découverte (discovery.candidates : tokens qui tiennent après 24 h, marché vérifié sur
DexScreener, market caps gonflées écartées). Pour chacun : ses plus gros détenteurs (~4 crédits Helius).

Garde-fous :
  - seulement les succès de devs jugés propres par la découverte (groupe « découverte ») : vu en vrai, la liste
    des « succès » contenait $VSOF et $AOR (cluster Reserve), dont les gros détenteurs sont l'opérateur lui-même ;
  - jamais les wallets à parts identiques (vu en vrai sur $AMERICA : 14 détenteurs à 0,98 % chacun = UN opérateur
    qui a réparti sa supply, un bundle) ;
  - jamais le dev du token, un exchange / service, un pool, ni un wallet d'un groupe à rugs ;
  - smart money = gros détenteur d'au moins 3 succès de 3 devs différents en 14 jours ;
  - 15 wallets suivis au plus (les plus réguliers) ;
  - « À ne pas rater » seulement si 3 d'entre eux entrent ensemble : un seul wallet peut appâter les bots
    de suivi (vu dans le marché en 2025-2026 : des initiés tradent exprès pour attirer les copieurs).
"""
from __future__ import annotations

import asyncio
import logging
import time

from . import discovery
from .confiance import is_service
from .telegram import esc

log = logging.getLogger("smart")

SMART_GROUP = "smart-money"
SMART_ROLE = "smart money"
SCAN_EVERY_S = 1800
WINNERS_PER_SCAN = 6         # succès analysés par passage (les autres au suivant)
TOP_HOLDERS = 15
MIN_HOLD_PCT = 0.3           # au moins 0,3 % de la supply
MIN_WINS, MIN_CREATORS = 3, 3
WINDOW_S = 14 * 86400
SMART_MAX = 15
SMART_TOP_MIN = 3            # wallets smart money qui entrent ensemble pour « À ne pas rater »
SYSTEM = "11111111111111111111111111111111"
BUNDLE_SAME_PCT = 4          # 4 détenteurs ou plus avec la même part = supply répartie par un seul opérateur


def sans_bundles(gros: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """Retire les détenteurs dont la part est identique à celle d'au moins 3 autres (bundle d'un opérateur)."""
    compte: dict[float, int] = {}
    for _o, pct in gros:
        compte[round(pct, 2)] = compte.get(round(pct, 2), 0) + 1
    return [(o, pct) for o, pct in gros if compte[round(pct, 2)] < BUNDLE_SAME_PCT]


async def top_holders(rpc, mint: str, n: int = TOP_HOLDERS) -> list[tuple[str, float]]:
    """[(propriétaire, % de la supply)] des plus gros comptes, wallets normaux seulement (pas les pools)."""
    info = await rpc.mint_info(mint)
    supply = int((info or {}).get("supply") or 0)
    comptes = (await rpc.token_largest_accounts(mint))[:n] if supply else []
    if not comptes:
        return []
    res = await rpc.call("getMultipleAccounts", [[a["address"] for a in comptes], {"encoding": "jsonParsed"}])
    parts: dict[str, float] = {}
    for a, v in zip(comptes, (res or {}).get("value", [])):
        try:
            owner = v["data"]["parsed"]["info"]["owner"]
        except (TypeError, KeyError):
            continue
        parts[owner] = parts.get(owner, 0.0) + 100 * int(a["amount"]) / supply
    proprios = list(parts)
    res = await rpc.call("getMultipleAccounts", [proprios, {"encoding": "base64"}])
    return [(o, round(parts[o], 2)) for o, v in zip(proprios, (res or {}).get("value", []))
            if v is None or v.get("owner") == SYSTEM]


class SmartMoney:
    def __init__(self, pipeline):
        self.p = pipeline

    def _exclu(self, owner: str, creator: str) -> bool:
        if owner == creator or is_service(self.p.db.wallet(owner), self.p.db.get_label(owner)):
            return True
        grp = self.p.group(owner)
        return bool(grp and grp in self.p.bad_groups())

    async def scan_once(self) -> list[str]:
        """Analyse quelques succès pas encore vus, puis promeut les nouveaux smart money. Renvoie leurs adresses."""
        db, faits = self.p.db, 0
        for coin in await discovery.candidates(self.p.http, self.p.cfg.discovery_min_ath):
            if faits >= WINNERS_PER_SCAN:
                break
            mint = coin["mint"]
            dev = db.wallet(coin["creator"])
            if db.get(f"smart_vu:{mint}") or not dev or dev["grp"] != discovery.GROUP:
                continue   # déjà vu, ou dev pas (encore) jugé propre par la découverte
            faits += 1
            try:
                gros = await top_holders(self.p.rpc, mint)
            except Exception as e:
                log.debug("Détenteurs de %s illisibles : %s", mint[:6], e)
                continue
            for owner, pct in sans_bundles(gros):
                if pct >= MIN_HOLD_PCT and not self._exclu(owner, coin["creator"]):
                    db.add_smart_hit(owner, mint, coin["creator"], coin.get("symbol"), pct)
            db.put(f"smart_vu:{mint}", int(time.time()))
        nouveaux = await self._promote()
        if faits:
            log.info("Smart money : %d succès analysés · %d wallet(s) promus", faits, len(nouveaux))
        return nouveaux

    async def _promote(self) -> list[str]:
        db, nouveaux = self.p.db, []
        suivis = sum(1 for w in db.active_wallets() if w["grp"] == SMART_GROUP)
        for r in db.smart_candidates(int(time.time()) - WINDOW_S, MIN_WINS, MIN_CREATORS):
            if suivis >= SMART_MAX:
                break
            w = db.wallet(r["wallet"])
            if w and (w["active"] or w["grp"] == SMART_GROUP):
                continue   # déjà suivi (ou retiré exprès : sniper, service…)
            role = f"{SMART_ROLE} : gros détenteur de {r['n']} succès ({r['symbols']})"[:200]
            if await self.p.watch(r["wallet"], f"SMART_{r['wallet'][:4]}", SMART_GROUP, role, 1, None):
                nouveaux.append(r["wallet"])
                suivis += 1
        if nouveaux:
            lignes = [f"🧠 <b>{len(nouveaux)} wallet(s) smart money ajouté(s)</b>",
                      "<i>Parmi les plus gros détenteurs d'au moins 3 vrais succès de devs différents. "
                      "Quand plusieurs entrent ensemble dans un token jeune : alerte 🧠.</i>"]
            for a in nouveaux:
                lignes.append(f"• <code>{a}</code> — {esc((db.wallet(a)['role'] or '')[len(SMART_ROLE) + 3:])}")
            if self.p.tg:
                self.p.tg.enqueue("\n".join(lignes), kind="devs", topic="devs")
            log.info("Smart money : %d wallet(s) ajouté(s)", len(nouveaux))
        return nouveaux

    async def loop(self) -> None:
        await asyncio.sleep(300)   # laisse le radar démarrer
        while True:
            try:
                await self.scan_once()
            except Exception:
                log.exception("Smart money : passage en échec")
            await asyncio.sleep(SCAN_EVERY_S)
