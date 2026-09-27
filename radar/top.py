"""‼️ À ne pas rater : sélection des alertes vérifiées et propres, et leur contrôle à +5 min."""
from __future__ import annotations

import asyncio
import json
import logging

from . import alerts as A
from .alerte import Alert
from .analysis.enrich import missing_data
from .alerts import RUG_MARK
from .analysis.enrich import TokenInfo
from .reglages import CHECK5_S, TOP_KINDS, TOP_RETRY_S
from .sources import dexscreener
from .telegram import esc

log = logging.getLogger("pipeline")


class TopMixin:
    """« ‼️ À ne pas rater » : un seul contrôle pour toutes les alertes candidates (wallet de confiance, drapeaux
    graves, drapeaux de l'annonce X, données complètes, âge), puis contrôle du token 5 min après l'alerte.
    Mélangé à Pipeline : utilise ses méthodes (_info, _skip, _spawn, _track…)."""

    # --- envoi ------------------------------------------------------------------------
    def _emit_top(self, alert: Alert) -> None:
        """Copie courte dans « 🎯 À ne pas rater » si le signal est vérifié et sans drapeau grave."""
        if not (alert.info and self.tg and not self.dry_run) or alert.kind not in TOP_KINDS:
            return
        mint = alert.info.mint
        if not alert.top_title:
            log.info("Pas « à ne pas rater » (wallet pas de confiance : satellite, lointain ou à éviter) : %s", mint)
            self._skip("pas « à ne pas rater » : wallet pas de confiance")
            return
        # Drapeaux de l'annonce X reliée à ce contrat (arnaque, abonnés achetés, compte racheté, imitation…) :
        # ils n'étaient pas pris en compte (vu à la relecture : un coin annoncé par un compte racheté pouvait
        # arriver dans ‼️ dès l'ouverture du trading).
        annonce = self._announcement_flags(mint)
        flags = list(alert.info.flags) + list(alert.flags or []) + annonce
        if not A.is_safe(flags):
            grave = next((f for f in flags if A.RUG_MARK in f or any(s in f.lower() for s in A.SEVERE)), "?")
            log.info("Pas « à ne pas rater » (signal grave : %s) : %s", grave[:90], mint)
            self._skip("pas « à ne pas rater » : signal grave")
            return  # ⛔ / 🟠 : reste dans le groupe, jamais dans les alertes « à ne pas rater »
        manque = missing_data(alert.info)
        if manque:
            # Vu en vrai : pump.fun ne répondait plus, tout sortait 🟢 faute de données. Pas de données = pas sûr.
            # Mais une market cap pas encore indexée (pool tout neuf) se complète en 1 à 3 min : on réessaie.
            log.info("Pas « à ne pas rater » pour l'instant (données incomplètes : %s) : %s", ", ".join(manque),
                     alert.info.mint)
            if not getattr(alert, "retried", False):
                self._spawn(self._retry_top(alert))
            return self._skip("pas « à ne pas rater » pour l'instant : données incomplètes")
        if alert.info.age_s is not None and alert.info.age_s > 6 * 3600:
            log.info("Pas « à ne pas rater » (token de plus de 6 h, plus un lancement) : %s", mint)
            return self._skip("pas « à ne pas rater » : token trop vieux")
        text = A.top_card(alert.info, alert.top_title, alert.top_why, list(alert.flags or []) + annonce)
        cle = f"top:{alert.kind}:{alert.info.mint}"
        if self.tg.enqueue_top(text, A.top_buttons(alert.info, getattr(self.cfg, "trade_url", "")), key=cle,
                               event_ts=alert.event_ts):
            log.info("🎯 À ne pas rater : %s %s", alert.kind, alert.info.mint)
            self._track(alert, key=cle, kind="top")
            self._spawn(self._controle_5min(alert, cle))
    def _announcement_flags(self, mint: str) -> list[str]:
        ann = self.db.find_announcement(None, mint, 0)
        if not ann or ann["ca"] != mint:
            return []
        try:
            return [str(f) for f in json.loads(ann["flags"] or "[]")]
        except ValueError:
            return []
    async def _retry_top(self, alert: Alert) -> None:
        """Relit le token après 1 puis 3 min : s'il est maintenant complet et propre, il part dans ‼️."""
        from .analysis import enrich
        for attente in TOP_RETRY_S:
            await asyncio.sleep(attente)
            enrich._cache.pop(alert.info.mint, None)
            info = await self._info(alert.info.mint, alert.info.creator)
            if not missing_data(info):
                alert.info, alert.retried = info, True
                alert.top_why += f" <i>(données complètes après {attente // 60 or 1} min)</i>"
                self._emit_top(alert)
                return
        log.info("Pas « à ne pas rater » : données toujours incomplètes après 3 min pour %s", alert.info.mint)

    async def _controle_5min(self, alert: Alert, cle: str) -> None:
        """5 min après une alerte « à ne pas rater » : le token tient-il ? (la recherche prédit la plupart des rugs
        dès les 5 premières minutes de trading : ventes qui dominent, dev qui vend, concentration qui monte)."""
        from .analysis import enrich
        await asyncio.sleep(CHECK5_S)
        mint = alert.info.mint
        enrich._cache.pop(mint, None)
        try:
            apres = await self._info(mint, alert.info.creator)
            paire = await dexscreener.token_pairs(self.http, mint) if self.http else None
        except Exception as e:
            log.info("Contrôle à +5 min impossible pour %s : %s", mint, e)
            return
        tient, raisons, chiffres = verdict_5min(alert.info, apres, paire)
        titre = "✅ <b>ÇA TIENT" if tient else "⚠️ <b>DEVENU SUSPECT"
        lignes = [f"{titre} — {A.token_title(apres)}</b>", "<i>Contrôle 5 min après l'alerte ‼️</i>"]
        lignes += [f"🚩 {esc(r)}" for r in raisons]
        lignes += [" · ".join(chiffres)] if chiffres else []
        lignes += [f"📜 <code>{mint}</code>"]
        if tient:
            lignes.append("<i>Rien d'anormal au contrôle. Ça peut changer vite : reste prudent.</i>")
        self.tg.enqueue_top("\n".join(lignes), A.top_buttons(apres, getattr(self.cfg, "trade_url", "")),
                            key=f"{cle}:5min")
        self.db.set_result_check(cle, "ok" if tient else "suspect")
        log.info("Contrôle à +5 min de %s : %s %s", mint, "tient" if tient else "suspect", "; ".join(raisons))


def verdict_5min(avant: TokenInfo, apres: TokenInfo, paire: dict | None) -> tuple[bool, list[str], list[str]]:
    """(tient, raisons d'inquiétude, chiffres) pour un token 5 min après son alerte « à ne pas rater »."""
    raisons: list[str] = []
    chiffres: list[str] = []
    nouveaux = [f for f in apres.flags if f not in avant.flags
                and (RUG_MARK in f or any(s in f.lower() for s in A.SEVERE))]
    raisons += nouveaux[:3]
    if avant.mc_usd and apres.mc_usd:
        variation = apres.mc_usd / avant.mc_usd - 1
        chiffres.append(f"MC {A.usd(avant.mc_usd)} → {A.usd(apres.mc_usd)} ({variation:+.0%})")
        if variation <= -0.4:
            raisons.append(f"market cap {variation:.0%} depuis l'alerte")
    if paire and paire.get("buys5") is not None and paire.get("sells5") is not None:
        achats, ventes = paire["buys5"], paire["sells5"]
        chiffres.append(f"5 dernières min : {achats} achats / {ventes} ventes")
        if achats + ventes < 15:
            raisons.append("presque aucun échange depuis l'alerte")
        elif ventes > 1.5 * achats:
            raisons.append(f"les ventes dominent ({ventes} ventes pour {achats} achats)")
    if avant.dev_pct and apres.dev_pct is not None and apres.dev_pct < 0.5 * avant.dev_pct:
        raisons.append(f"le dev a vendu : {avant.dev_pct:.0f} % → {apres.dev_pct:.0f} % de la supply")
    if apres.top10_pct is not None:
        chiffres.append(f"top 10 : {apres.top10_pct:.0f} %")
    return not raisons, raisons, chiffres
