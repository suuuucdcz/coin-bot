"""🚀 Décolle proprement : les tokens de devs inconnus qui décollent sainement, et la mesure de ce qui marche.

Ce n'est pas « avant les bots » (ils achètent dans le premier bloc) : c'est un décollage déjà en cours, 3 à 10 min
après la création, filtré. Mesuré le 28-29/09 : les coins qui marchent font leur plus haut des heures ou des jours
plus tard (WEPE 3 j, Nasduck 2 j 22 h, réseau Reserve 17 h en médiane) ; le problème n'est pas le moment, c'est le
TRI (plus de 1 000 tokens décollent chaque jour, moins de 1 % tiennent au-dessus de 100 k$).

Deux choses :
  1. mesure silencieuse : chaque token qui décolle (veille des lancements) est photographié (market cap, échanges,
     achats / ventes, volume, mise du dev, wallet neuf ou pas, financeur, lien avec un réseau à rugs) ; 24 h après,
     son historique de prix dit ce qu'il est devenu (gros succès, succès, rug brutal, mort). Le bilan dit quels indices
     annonçaient les succès : les seuils de la section se règlent sur ces chiffres, pas au jugé ;
  2. la section 🚀 : ceux qui passent tous les filtres (réglages de départ prudents, voir SEUILS) ; chaque alerte est
     suivie dans 📈 Résultats.
Coût : DexScreener et la toile (gratuits) ; la structure des détenteurs (3 crédits Helius) seulement pour les
candidats de la section, quelques dizaines par jour au plus.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, defaultdict, deque

from . import alerts as A
from .alerte import Alert
from .analysis.enrich import FERME_MIN, TokenInfo, holders
from .sources import dexscreener, geckoterminal
from .telegram import esc

log = logging.getLogger("decollage")

# Réglages de départ (prudents) de la section 🚀, à ajuster avec le bilan de la mesure
SEUILS = {
    "age_min_s": 150, "age_max_s": 600,        # entre 2 min 30 et 10 min après la création
    "mc_min": 20_000, "mc_max": 250_000,       # a vraiment décollé, mais pas déjà parti
    "txns_min": 100,                           # beaucoup d'échanges, pas trois wallets
    "achats_ratio": 1.2,                       # plus d'achats que de ventes sur 5 min
    "vol5_min": 8_000,                         # vrai volume sur 5 min ($)
    "achat_dev_max": 10.0,                     # mise du dev à la création (SOL) : 100 SOL = schéma Reserve
    "top10_max": 30.0, "dev_max": 10.0,        # détenteurs (hors pools)
}
ALERTES_PAR_H = 6
FABRIQUE_24H = 3                               # 3 tokens ou plus en 24 h par le même créateur : usine
JUGE_APRES_S = 24 * 3600
ABANDON_S = 4 * 86400
JUGES_PAR_TOUR = 40
TOUR_S = 30 * 60
GROS_SUCCES, SUCCES = 1_000_000, 100_000
SUPPLY_PUMP_RAW = 10 ** 15                     # 1 milliard de tokens à 6 décimales


def issue(pic: float | None, chute: str | None) -> str:
    """Ce qu'est devenu un token, d'après son plus haut et la vitesse de sa chute."""
    if not pic or pic < SUCCES:
        return "mort"
    if chute == "brutale":
        return "rug"
    return "gros succès" if pic >= GROS_SUCCES else "succès"


def refus_section(ph: dict, s: dict = SEUILS) -> str | None:
    """Raison de ne pas publier ce décollage dans 🚀 (None = il passe les filtres de départ)."""
    if ph["lien"] == "rug":
        return "relié à un réseau à rugs"
    if ph["statut"] == "actif":
        return "créateur très actif (bot / usine)"
    if (ph.get("tokens_24h") or 0) >= FABRIQUE_24H:
        return "usine à tokens"
    if not s["age_min_s"] <= ph["age_s"] <= s["age_max_s"]:
        return "trop tôt ou trop tard"
    if not s["mc_min"] <= (ph["mc"] or 0) <= s["mc_max"]:
        return "market cap hors fourchette"
    if (ph["txns"] or 0) < s["txns_min"]:
        return "trop peu d'échanges"
    if (ph["achats5"] or 0) < s["achats_ratio"] * max(1, ph["ventes5"] or 0):
        return "plus de ventes que d'achats"
    if (ph["vol5"] or 0) < s["vol5_min"]:
        return "volume trop faible"
    if ph["achat_dev"] is not None and ph["achat_dev"] > s["achat_dev_max"]:
        return "mise du dev trop grosse"
    return None


def refus_structure(ph: dict, s: dict = SEUILS) -> str | None:
    if ph["ferme"] is not None and ph["ferme"] >= FERME_MIN:
        return "ferme de wallets"
    if ph["top10"] is not None and ph["top10"] > s["top10_max"]:
        return "top 10 trop concentré"
    if ph["dev_pct"] is not None and ph["dev_pct"] > s["dev_max"]:
        return "le dev garde trop de supply"
    return None


class Decollage:
    def __init__(self, pipeline):
        self.p = pipeline
        self.db = pipeline.db
        self._alertes: deque[float] = deque()
        self.stats: Counter[str] = Counter()

    # --- 1. photo au décollage --------------------------------------------------------------------------
    def _lien(self, creator: str) -> tuple[str | None, list[dict]]:
        """(« rug » | « bon » | None) si le créateur ou un maillon de sa chaîne (toile) est connu du radar."""
        lw, toile = self.p.lancements, self.p.toile
        if lw is None:
            return None, []
        c = lw.connu(creator)
        if c:
            return c[0], []
        chaine = (toile.chaine(creator) if toile is not None else None) or []
        for h in chaine:
            if self.p.is_service_address(h["src"]):
                break
            c = lw.connu(h["src"])
            if c:
                return c[0], chaine
        return None, chaine

    async def observer(self, t: dict, m: dict, now: float | None = None) -> dict:
        """Photo d'un token qui décolle (toujours enregistrée), puis section 🚀 s'il passe les filtres."""
        now = now or time.time()
        creator = t["creator"]
        w = self.db.toile_wallet(creator)
        racine = w["racine"] if w is not None else None
        st = self.p.toile.stats_racine(racine) if racine and self.p.toile is not None else {}
        lien, _chaine = self._lien(creator)
        ph = {"mint": t["mint"], "creator": creator, "symbol": t.get("symbol"), "cree": int(t["ts"]), "photo": int(now),
              "age_s": int(now - t["ts"]), "mc": m.get("mc"), "liq": m.get("liquidity"), "txns": m.get("txns24h"),
              "achats5": m.get("buys5"), "ventes5": m.get("sells5"), "vol5": m.get("vol5"), "var5": m.get("change5"),
              "achat_dev": t.get("achat_dev"), "statut": w["statut"] if w is not None else None, "racine": racine,
              "racine_succes": st.get("succes"), "racine_rugs": st.get("rugs"), "lien": lien,
              "tokens_24h": self.db.toile_tokens_24h(creator, int(now - 86400)),
              "top10": None, "dev_pct": None, "ferme": None}
        refus = refus_section(ph)
        if refus is None:
            refus = await self._structure(ph)
        if refus is None and not self._quota(now):
            refus = "plafond d'alertes atteint"
        ph["refus"] = refus
        ph["alerte"] = int(refus is None)
        self.db.decollage_add(ph)
        self.stats["photos"] += 1
        if refus is None:
            self.stats["alertes"] += 1
            self._alerter(ph, t)
        return ph

    async def _structure(self, ph: dict) -> str | None:
        """Détenteurs (3 crédits Helius), seulement pour un candidat de la section."""
        if self.p.rpc is None:
            return "structure inconnue"
        try:
            res = await asyncio.wait_for(holders(self.p.rpc, ph["mint"], SUPPLY_PUMP_RAW, ph["creator"]), 10)
        except Exception as e:
            log.debug("Détenteurs de %s illisibles : %s", ph["mint"][:6], e)
            res = None
        if not res:
            return "structure inconnue"
        ph["top10"], ph["dev_pct"], ph["ferme"], _part = res
        return refus_structure(ph)

    def _quota(self, now: float) -> bool:
        while self._alertes and now - self._alertes[0] > 3600:
            self._alertes.popleft()
        if len(self._alertes) >= ALERTES_PAR_H:
            return False
        self._alertes.append(now)
        return True

    def _alerter(self, ph: dict, t: dict) -> None:
        info = TokenInfo(ph["mint"], name=t.get("name"), symbol=ph["symbol"], creator=ph["creator"], created_ts=ph["cree"],
                         mc_usd=ph["mc"], liquidity_usd=ph["liq"])
        dev = [f"mise de départ {ph['achat_dev']:g} SOL" if ph["achat_dev"] is not None else "mise de départ inconnue",
               {"neuf": "wallet neuf", "relais": "wallet neuf", "reutilise": "wallet déjà utilisé"}.get(ph["statut"] or "",
                                                                                              "wallet pas encore analysé")]
        if ph["racine"]:
            dev.append(f"financé par {A.short(ph['racine'])}"
                       + (f" ({ph['racine_succes']} succès connus)" if ph["racine_succes"] else ""))
        lignes = [f"🚀 <b>DÉCOLLE PROPREMENT — ${esc(ph['symbol'] or '?')}</b>" + (f" ({esc(t['name'])})" if t.get("name") else ""),
                  f"⏱ {A.age(ph['age_s'])} après la création · MC <b>{A.usd(ph['mc'])}</b> · liquidité {A.usd(ph['liq'])}",
                  f"📊 {ph['txns']} échanges · 5 min : {ph['achats5']} achats / {ph['ventes5']} ventes · volume "
                  f"{A.usd(ph['vol5'])}" + (f" · {ph['var5']:+.0f} %" if ph["var5"] is not None else ""),
                  "👤 Dev : " + " · ".join(dev),
                  f"🧱 Détenteurs : top 10 {ph['top10']:.0f} % · dev {ph['dev_pct']:.0f} % · pas de ferme de wallets",
                  f"<code>{ph['mint']}</code>",
                  "<i>Pas avant les bots : un décollage déjà en cours, filtré (pas de réseau à rugs, pas de ferme, dev "
                  "raisonnable). Section en test : chaque alerte est suivie dans 📈 Résultats.</i>"]
        self.p.emit(Alert(f"decolle:{ph['mint']}", "decolle", "\n".join(lignes), A.token_buttons(info, ph["creator"]),
                          info=info, wallet=None, event_ts=ph["cree"]))
        log.info("🚀 Décolle proprement : $%s %s (MC %s, %d échanges)", ph["symbol"], ph["mint"][:6], A.usd(ph["mc"]),
                 ph["txns"] or 0)

    # --- 2. ce qu'ils sont devenus ---------------------------------------------------------------------
    async def juger(self, now: float | None = None) -> int:
        """24 h après la photo : plus haut, vitesse de la chute (bougies), market cap actuelle."""
        now = now or time.time()
        if self.p.http is None:
            return 0
        rows = self.db.decollage_a_juger(int(now - JUGE_APRES_S), JUGES_PAR_TOUR)
        if not rows:
            return 0
        marches = await dexscreener.markets(self.p.http, [r["mint"] for r in rows])
        n = 0
        for r in rows:
            c = await geckoterminal.chute_token(self.p.http, self.db, r["mint"])
            if c is None and now - r["photo"] < ABANDON_S:
                continue   # pas encore de données : réessayé au prochain tour
            pic, chute = (c or {}).get("pic"), (c or {}).get("chute")
            self.db.decollage_juger(r["mint"], pic, chute, (marches.get(r["mint"]) or {}).get("mc"), issue(pic, chute))
            n += 1
        self.stats["juges"] += n
        return n

    # --- 3. bilan : quels indices annonçaient les succès ? ---------------------------------------------
    def bilan(self, jours: int = 7) -> str:
        rows = [r for r in self.db.decollages_depuis(int(time.time() - jours * 86400)) if r["issue"]]
        if not rows:
            return "🚀 Décollages : pas encore de token jugé (24 h après la photo)."
        def taux(lot) -> str:
            if not lot:
                return "—"
            ok = sum(1 for r in lot if r["issue"] in ("succès", "gros succès"))
            gros = sum(1 for r in lot if r["issue"] == "gros succès")
            rugs = sum(1 for r in lot if r["issue"] == "rug")
            return f"{len(lot)} · succès {100 * ok // len(lot)} % (dont ≥ 1 M$ : {gros}) · rugs {100 * rugs // len(lot)} %"
        lignes = [f"🚀 BILAN DES DÉCOLLAGES ({jours} j) — {taux(rows)}"]
        groupes = [
            ("section 🚀", lambda r: "publiés" if r["alerte"] else "non publiés"),
            ("market cap à la photo", lambda r: "< 30 k$" if (r["mc"] or 0) < 30_000 else "30-100 k$"
             if (r["mc"] or 0) < 100_000 else "≥ 100 k$"),
            ("achats / ventes (5 min)", lambda r: "?" if not r["ventes5"] else "≥ 1,5" if r["achats5"] >= 1.5 * r["ventes5"]
             else "1-1,5" if r["achats5"] >= r["ventes5"] else "< 1"),
            ("échanges", lambda r: "< 100" if (r["txns"] or 0) < 100 else "100-300" if (r["txns"] or 0) < 300 else "≥ 300"),
            ("mise du dev", lambda r: "?" if r["achat_dev"] is None else "≤ 1 SOL" if r["achat_dev"] <= 1
             else "1-10 SOL" if r["achat_dev"] <= 10 else "> 10 SOL"),
            ("créateur", lambda r: r["statut"] or "pas analysé"),
            ("financeur", lambda r: "a déjà des succès" if (r["racine_succes"] or 0) > 0 else "a déjà des rugs"
             if (r["racine_rugs"] or 0) > 0 else "inconnu"),
            ("lien avec le radar", lambda r: r["lien"] or "aucun"),
        ]
        for nom, cle in groupes:
            par = defaultdict(list)
            for r in rows:
                par[cle(r)].append(r)
            lignes.append(f"  {nom} : " + " | ".join(f"{k} : {taux(v)}" for k, v in sorted(par.items())))
        refus = Counter(r["refus"] for r in rows if r["refus"])
        lignes.append("  refus de la section : " + ", ".join(f"{k} {v}" for k, v in refus.most_common(6)))
        return "\n".join(lignes)

    def status_line(self) -> str:
        n = self.db.decollages_compte()
        return (f"🚀 Décollages : {n['photos']} photographiés · {n['juges']} jugés à 24 h · {n['succes']} succès · "
                f"{n['alertes']} publiés dans la section")

    async def boucle(self) -> None:
        dernier_bilan = 0.0
        while True:
            await asyncio.sleep(TOUR_S)
            try:
                await self.juger()
            except Exception:
                log.exception("Décollages : jugement en échec")
            if time.time() - dernier_bilan > 86400:
                dernier_bilan = time.time()
                for ligne in self.bilan().splitlines():
                    log.info("%s", ligne)


def main() -> int:
    """python -m radar.decollage : bilan de la mesure (lecture seule)."""
    import sys
    from . import config as cfgmod
    from .db import DB

    class _P:
        def __init__(self, db):
            self.db = db

    cfg = cfgmod.load()
    db = DB(cfg.db_path)
    jours = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    print(Decollage(_P(db)).bilan(jours))
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
