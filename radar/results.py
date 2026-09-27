"""📈 Suivi des résultats : chaque alerte sur un token est suivie pendant 24 h.

Pour chaque alerte : market cap au moment de l'alerte, plus haut atteint ensuite, valeur à +1 h et à +24 h.
Le bilan dit quelles alertes valent le coup (par type, par niveau de confiance du wallet) : sans lui, les seuils
du radar se règlent au jugé.

Prix : DexScreener (lots de 30 tokens) ; pour les tokens pump.fun, la fiche pump.fun donne en plus le plus haut
(ATH) atteint entre deux mesures. Aucun crédit Helius.
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from . import alerts as A
from .db import DB
from .sources import dexscreener, pumpfun
from .telegram import esc

log = logging.getLogger("resultats")

# Alertes suivies (clé = type d'alerte) et leur nom dans le bilan
TRACKED = {"top": "‼️ À ne pas rater", "create": "🔴 Créations", "buy": "🟠 Achats d'un dev",
           "cluster": "🎯 Cluster / dev qui entre", "smart": "🧠 Smart money",
           "relance": "🔗 Wallet neuf d'un dev connu",
           "lp_add": "🟢 Trading ouvert", "supply_in": "🟣 Supply reçue",
           "annonce": "📆 Coins annoncés lancés"}
CHECK_EVERY_S = 600          # une mesure toutes les 10 min pendant 24 h
FOLLOW_S = 24 * 3600
PUMP_PER_ROUND = 25          # fiches pump.fun lues par tour (plus haut entre deux mesures)
HIT, BIG_HIT = 2.0, 5.0      # « a monté » = ×2 au plus haut ; « gros coup » = ×5
RUG_LEFT = 0.3               # « rug » = il reste moins de 30 % de la market cap de départ au dernier relevé


class Results:
    def __init__(self, db: DB, http: aiohttp.ClientSession | None):
        self.db, self.http = db, http

    def record(self, key: str, mint: str | None, kind: str, mc0: float | None = None, symbol: str | None = None,
               grp: str | None = None, trust: str | None = None) -> None:
        """Commence le suivi d'une alerte envoyée (une ligne par alerte, pas par token)."""
        if kind in TRACKED and mint:
            self.db.add_result(key, mint, kind, symbol, grp, trust, mc0)

    async def check_once(self, now: int | None = None) -> int:
        """Une mesure pour chaque alerte encore suivie. Renvoie le nombre d'alertes mesurées."""
        now = int(now or time.time())
        rows = self.db.results_open(now - FOLLOW_S - 3 * 3600)
        if not rows or self.http is None:
            return 0
        mints = sorted({r["mint"] for r in rows})
        prix = {m: v["mc"] for m, v in (await dexscreener.markets(self.http, mints)).items() if v.get("mc")}
        pics: dict[str, tuple[float, int]] = {}
        for m in [m for m in mints if m.endswith("pump")][:PUMP_PER_ROUND]:
            c = await pumpfun.coin(self.http, m)
            if c:
                if c.get("mc") and m not in prix:
                    prix[m] = c["mc"]
                if c.get("ath") and c.get("ath_ts"):
                    pics[m] = (c["ath"], c["ath_ts"])
            await asyncio.sleep(0.2)
        for r in rows:
            ath, ath_ts = pics.get(r["mint"], (None, 0))
            pic = ath if ath and ath_ts >= (r["sent_at"] or 0) - 60 else None   # ATH APRÈS l'alerte seulement
            self.db.update_result(r["key"], prix.get(r["mint"]), pic, now)
        return len(rows)

    async def loop(self) -> None:
        while True:
            try:
                n = await self.check_once()
                if n:
                    log.debug("Résultats : %d alerte(s) mesurée(s)", n)
            except Exception:
                log.exception("Suivi des résultats en échec")
            await asyncio.sleep(CHECK_EVERY_S)

    # --- bilan ------------------------------------------------------------------------------------------
    def report(self, days: int = 7) -> str:
        return report_text(self.db.results_since(int(time.time()) - days * 86400), days)

    def short_line(self, days: int = 7) -> str:
        """Une ligne pour /statut."""
        rows = [r for r in self.db.results_since(int(time.time()) - days * 86400) if _mult(r) is not None]
        if not rows:
            return "📈 Résultats : pas encore de mesure (suivi de chaque alerte pendant 24 h)"
        hauts = sum(1 for r in rows if _mult(r) >= HIT)
        rugs = sum(1 for r in rows if _rug(r))
        return (f"📈 Résultats {days} j : {len(rows)} alertes mesurées · {100 * hauts // len(rows)} % ont fait ×2 · "
                f"{100 * rugs // len(rows)} % rug · /resultats")


def _mult(r) -> float | None:
    """Plus haut atteint / market cap de départ."""
    return r["mc_max"] / r["mc0"] if r["mc0"] and r["mc_max"] else None


def _rug(r) -> bool:
    fin = r["mc_24h"] if r["done"] else r["last_mc"]
    return bool(r["mc0"] and fin is not None and fin < RUG_LEFT * r["mc0"])


def report_text(rows, days: int = 7) -> str:
    """Bilan par type d'alerte : combien ont fait ×2 / ×5 au plus haut, combien ont rug, les meilleures."""
    mesures = [r for r in rows if _mult(r) is not None]
    lignes = [f"📈 <b>RÉSULTATS DES ALERTES — {days} derniers jours</b>",
              f"<i>{len(rows)} alertes suivies, {len(mesures)} avec un prix. ×2 / ×5 = au plus haut après l'alerte ; "
              f"rug = il reste moins de {int(100 * RUG_LEFT)} % de la market cap de départ.</i>", A.SEP]
    if not mesures:
        lignes.append("Pas encore de mesure : chaque alerte est suivie pendant 24 h.")
        return "\n".join(lignes)
    for kind, nom in TRACKED.items():
        lot = [r for r in mesures if r["kind"] == kind]
        if not lot:
            continue
        hauts = sum(1 for r in lot if _mult(r) >= HIT)
        gros = sum(1 for r in lot if _mult(r) >= BIG_HIT)
        rugs = sum(1 for r in lot if _rug(r))
        lignes.append(f"<b>{nom}</b> : {len(lot)} · ×2 : {hauts} ({100 * hauts // len(lot)} %) · ×5 : {gros} · "
                      f"rug : {rugs} ({100 * rugs // len(lot)} %)")
        if kind == "top":
            # Le contrôle à +5 min prédit-il la suite ?
            for verdict, libelle in (("ok", "✅ tenait à +5 min"), ("suspect", "⚠️ suspect à +5 min")):
                sous = [r for r in lot if r["check5"] == verdict]
                if sous:
                    h = sum(1 for r in sous if _mult(r) >= HIT)
                    g = sum(1 for r in sous if _rug(r))
                    lignes.append(f"    {libelle} : {len(sous)} · ×2 : {100 * h // len(sous)} % · "
                                  f"rug : {100 * g // len(sous)} %")
    par_confiance: dict[str, list] = {}
    for r in mesures:
        if r["kind"] != "top":
            par_confiance.setdefault(r["trust"] or "?", []).append(r)
    if par_confiance:
        lignes += [A.SEP, "<b>Selon la confiance du wallet</b>"]
        for niveau in ("référence", "prouvé", "lié", "faible", "?"):
            lot = par_confiance.get(niveau)
            if lot:
                hauts = sum(1 for r in lot if _mult(r) >= HIT)
                lignes.append(f"• {niveau} : {len(lot)} alertes · ×2 : {100 * hauts // len(lot)} %")
    meilleurs = sorted(mesures, key=_mult, reverse=True)[:3]
    if meilleurs and _mult(meilleurs[0]) >= HIT:
        lignes += [A.SEP, "<b>Meilleures</b>"]
        for r in meilleurs:
            if _mult(r) >= HIT:
                lignes.append(f"• ${esc(r['symbol'] or r['mint'][:6])} ×{_mult(r):.1f} ({A.usd(r['mc0'])} → "
                              f"{A.usd(r['mc_max'])}) · {TRACKED.get(r['kind'], r['kind'])}"
                              + (f" · {esc(r['grp'])}" if r["grp"] else ""))
    return "\n".join(lignes)
