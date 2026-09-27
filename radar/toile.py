"""🕸️ La toile : notre propre base « qui finance qui », tissée à chaque nouveau token pump.fun, sans Helius.

Tout est public, mais tout lire coûte cher (un nœud Solana complet : des milliers d'euros par mois). La toile ne lit
que ce qui compte, avec des RPC publics gratuits, et la base grandit d'elle-même :
  1. chaque nouveau token pump.fun (flux PumpPortal, gratuit) : son créateur est-il un wallet NEUF ? publicnode
     (rapide, ~1,7 jour d'historique) donne ses premières tx ; le RPC officiel de Solana (historique complet, débit
     limité) confirme qu'il n'y a rien avant. Vu en vrai : des wallets vidés puis refinancés des mois plus tard
     ressemblaient à des neufs sur publicnode (2 cas sur 15). Seul un wallet confirmé neuf donne un lien ;
  2. son premier financeur (même règle anti-leurre que le traceur) ; un relais (neuf, 5 tx au plus) est remonté ;
     le premier financeur qui n'est ni un relais ni un wallet neuf est la « racine » (bank de l'opérateur, ou
     exchange) ;
  3. 24 h après la création, DexScreener dit si le token a tenu (market cap ≥ 50 k$) : un succès pour sa racine.
     Un token qui monte puis se fait rug avant 24 h ne compte pas ;
  4. une racine qui a financé au moins 2 créateurs à succès, avec au moins 25 % de réussite, qui n'est pas un
     service (exchange, bridge : trop de tx ou trop de clients) entre dans la watchlist comme « bank à succès » :
     ses prochains wallets neufs sont signalés dès le financement (Helius, en direct) ;
  5. à chaque création, si un maillon de la chaîne est déjà connu du radar (bank à succès, dev propre, réseau à
     rugs) : alerte immédiate « NOUVEAU WALLET D'UN DEV CONNU » (ou ⛔).
Limites : un wallet financé par un exchange reste anonyme ; la toile ne vaut qu'après quelques jours de collecte.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque

from . import alerts as A
from .alerte import Alert
from .analysis.tracer import HOT_WINDOW_S, scan_funding
from .confiance import TRUSTED
from .sources import dexscreener
from .sources.helius import LAMPORTS, RpcError, SolanaRPC, account_keys
from .telegram import esc

log = logging.getLogger("toile")

GROUP = "toile"
PUBLIC_RPS = 3               # publicnode : 4/s tenus sans erreur pendant les essais, on reste en dessous
ARCHIVE_RPS = 0.5            # RPC officiel : 1 vérification toutes les 1,5 s tenue sans erreur
WORKERS = 2
FILE_MAX = 3000              # tokens en attente ; au-delà, les plus anciens sautent (comptés « non traités »)
SIG_MAX = 1000               # 1 000 tx en moins de 2 jours : bot, usine à tokens ou service, pas un wallet neuf
RELAY_MAX_TX = 5
MAX_HOPS = 3
LIENS = ("neuf", "relais")   # statuts qui portent un lien sûr
SUCCES_MC = 50_000           # market cap 24 h après la création
MIN_SUCCES = 2               # créateurs différents
MIN_TAUX = 0.25
MAX_CREATEURS_7J = 50        # au-delà : un service qui finance ses clients, pas une équipe
PROMOTIONS_PAR_JOUR = 20
MESURE_APRES_S = 86400
MESURE_ABANDON_S = 3 * 86400
MESURES_PAR_TOUR = 600
LOT_DEXSCREENER = 20
TOUR_S = 20 * 60
CACHE_JOURS = 7              # wallets sans lien (réutilisés, très actifs) : simple cache
GARDE_JOURS = 60             # la toile garde deux mois (les succès, toujours)


def solde_avant(tx: dict, adresse: str) -> float | None:
    """Solde SOL de `adresse` juste avant la transaction."""
    meta = tx.get("meta") or {}
    for k, a in zip(account_keys(tx), meta.get("preBalances", [])):
        if k == adresse:
            return a / LAMPORTS
    return None


class Toile:
    def __init__(self, pipeline, public: SolanaRPC | None = None, archive: SolanaRPC | None = None):
        cfg = pipeline.cfg
        self.p = pipeline
        self.db = pipeline.db
        self.pub = public or SolanaRPC(cfg.toile_rpc_url, PUBLIC_RPS)
        self.arc = archive or SolanaRPC(cfg.toile_archive_url, ARCHIVE_RPS)
        self.file: deque[dict] = deque(maxlen=FILE_MAX)
        self._reveil = asyncio.Event()
        self._en_cours: set[str] = set()
        self._promus: deque[float] = deque()
        self.stats: Counter[str] = Counter()

    async def close(self) -> None:
        await self.pub.close()
        await self.arc.close()

    # --- 1. flux des nouveaux tokens ------------------------------------------------------------------
    def on_new_token(self, msg: dict) -> None:
        creator, mint = msg.get("traderPublicKey"), msg.get("mint")
        if not creator or not mint:
            return
        if len(self.file) == FILE_MAX:
            self.stats["non_traites"] += 1
        self.file.append({"mint": mint, "creator": creator, "symbol": msg.get("symbol"), "ts": time.time()})
        self._reveil.set()

    async def worker(self) -> None:
        while True:
            if not self.file:
                self._reveil.clear()
                await self._reveil.wait()
                continue
            t = self.file.pop()   # le plus récent d'abord : une alerte n'a de valeur que tout de suite
            try:
                await self.traiter(t)
            except Exception as e:
                self.stats["erreurs"] += 1
                log.debug("Toile : token %s non traité (%s)", t["mint"][:6], e)

    async def traiter(self, t: dict) -> dict | None:
        creator = t["creator"]
        for _ in range(30):
            if creator not in self._en_cours:
                break
            await asyncio.sleep(1)   # l'autre worker résout déjà ce créateur (deux tokens coup sur coup)
        w = self.db.toile_wallet(creator)
        if w is None:
            self._en_cours.add(creator)
            try:
                w = await self.resoudre(creator)
            finally:
                self._en_cours.discard(creator)
        self.stats["traites"] += 1
        if w is None or w["statut"] not in LIENS:
            return None
        self.db.toile_add_token(t["mint"], creator, t.get("symbol"), int(t["ts"]))
        return await self.evaluer(t, w)

    # --- 2. qui a financé ce wallet ? ----------------------------------------------------------------
    async def financement(self, adresse: str, sigs: list[dict] | None = None) -> dict:
        """{statut: neuf, source, amount, ts[, leurre]} pour un wallet confirmé neuf ; sinon {statut: reutilise |
        actif | inconnu}. RpcError si un RPC public ne répond pas (rien n'est alors enregistré)."""
        if sigs is None:
            sigs = await self.pub.signatures(adresse, limit=SIG_MAX)
        if not sigs:
            raise RpcError("aucune transaction visible (RPC public en retard ?)")
        if len(sigs) >= SIG_MAX:
            return {"statut": "actif"}
        ok = [s for s in reversed(sigs) if s.get("err") is None]   # de la plus ancienne à la plus récente
        if not ok:
            return {"statut": "inconnu"}
        tx0 = await self.pub.transaction(ok[0]["signature"])
        if not tx0:
            raise RpcError("première transaction introuvable sur le RPC public")
        if (solde_avant(tx0, adresse) or 0) > 0:
            return {"statut": "reutilise"}   # il avait déjà du SOL : son histoire commence avant ce que voit publicnode
        if await self.arc.signatures(adresse, before=sigs[-1]["signature"], limit=1):
            return {"statut": "reutilise"}   # vidé puis refinancé : l'historique complet montre des tx plus anciennes
        cache = {ok[0]["signature"]: tx0}

        async def fetch(sig: str) -> dict | None:
            return cache.get(sig) or await self.pub.transaction(sig)
        f = await scan_funding(adresse, ok, fetch)
        return {"statut": "neuf", **f} if f else {"statut": "inconnu"}

    async def _relais(self, adresse: str) -> list[dict] | None:
        """Signatures d'un relais probable (5 tx au plus sur publicnode), sinon None."""
        try:
            sigs = await self.pub.signatures(adresse, limit=RELAY_MAX_TX + 1)
        except RpcError:
            return None
        return sigs if sigs and len(sigs) <= RELAY_MAX_TX else None

    async def resoudre(self, adresse: str, profondeur: int = 0, sigs: list[dict] | None = None):
        """Résout et enregistre le financement d'un wallet (relais remontés). None si les RPC n'ont pas répondu."""
        try:
            f = await self.financement(adresse, sigs)
        except RpcError as e:
            self.stats["erreurs"] += 1
            log.debug("Toile : %s non résolu (%s)", adresse[:6], e)
            return None
        if f["statut"] != "neuf":
            self.stats[f["statut"]] += 1
            self.db.toile_put(adresse, f["statut"])
            return self.db.toile_wallet(adresse)
        src = f["source"]
        amont = self.db.toile_wallet(src)
        if amont is None and profondeur < MAX_HOPS and not self.service_connu(src):
            relais = await self._relais(src)
            if relais is not None:
                amont = await self.resoudre(src, profondeur + 1, relais)
        racine = amont["racine"] if amont is not None and amont["statut"] in LIENS and amont["racine"] else src
        statut = "relais" if profondeur else "neuf"
        self.stats[statut] += 1
        self.db.toile_put(adresse, statut, src, f["amount"], f["ts"], racine, (f.get("leurre") or {}).get("source"))
        return self.db.toile_wallet(adresse)

    def chaine(self, adresse: str) -> list[dict] | None:
        """Maillons du créateur jusqu'à sa racine, lus en base (aucun appel). None si la toile ne le relie pas."""
        hops: list[dict] = []
        cur = adresse
        for _ in range(MAX_HOPS + 1):
            w = self.db.toile_wallet(cur)
            if w is None or w["statut"] not in LIENS or not w["source"]:
                break
            hops.append({"src": w["source"], "sol": w["sol"],
                         "leurre": {"source": w["leurre"]} if w["leurre"] else None})
            if w["source"] == w["racine"]:
                break
            cur = w["source"]
        return hops or None

    # --- 3. succès mesurés à 24 h ----------------------------------------------------------------------
    async def mesurer(self, now: float | None = None) -> int:
        now = now or time.time()
        rows = self.db.toile_a_mesurer(int(now - MESURE_APRES_S), MESURES_PAR_TOUR)
        if not rows or self.p.http is None:
            return 0
        mesures: dict[str, float] = {}
        for i in range(0, len(rows), LOT_DEXSCREENER):
            lot = rows[i:i + LOT_DEXSCREENER]
            marches = await dexscreener.markets(self.p.http, [r["mint"] for r in lot])
            if not marches:
                # DexScreener muet sur tout un lot : panne probable, on réessaie au prochain tour (abandon à 3 jours)
                mesures.update({r["mint"]: 0.0 for r in lot if now - r["ts"] > MESURE_ABANDON_S})
                continue
            mesures.update({r["mint"]: float((marches.get(r["mint"]) or {}).get("mc") or 0) for r in lot})
        self.db.toile_set_mc(mesures)
        self.stats["mesures"] += len(mesures)
        for r in rows:
            if mesures.get(r["mint"], 0) >= SUCCES_MC:
                self.stats["succes"] += 1
                w = self.db.toile_wallet(r["creator"])
                if w is not None and w["racine"]:
                    await self.examiner(w["racine"])
        return len(mesures)

    # --- 4. financeurs à succès -------------------------------------------------------------------------
    def stats_racine(self, racine: str) -> dict:
        return self.db.toile_stats(racine, SUCCES_MC, int(time.time() - 7 * 86400))

    @staticmethod
    def qualifie(st: dict) -> bool:
        return (st["succes"] >= MIN_SUCCES and st["succes"] >= MIN_TAUX * st["juges"]
                and st["recents"] <= MAX_CREATEURS_7J)

    def service_connu(self, adresse: str) -> bool:
        return self.p.is_service_address(adresse)

    async def est_service(self, adresse: str) -> bool:
        """Exchange, bridge, bot de paiement : étiqueté, trop de wallets neufs financés en 7 jours, ou trop de tx en
        quelques heures (même règle que le traceur ; 1 appel publicnode, mémorisé)."""
        if self.service_connu(adresse):
            return True
        if self.db.toile_clients(adresse, int(time.time() - 7 * 86400)) > MAX_CREATEURS_7J:
            return True
        if self.db.get(f"toile_service:{adresse}") == "0":
            return False
        try:
            sigs = await self.pub.signatures(adresse, limit=1000)
        except RpcError:
            return True   # dans le doute, pas de promotion (on réessaiera)
        times = [s["blockTime"] for s in sigs if s.get("blockTime")]
        service = len(sigs) >= 1000 and bool(times) and max(times) - min(times) <= HOT_WINDOW_S
        self.db.put(f"toile_service:{adresse}", "1" if service else "0")
        return service

    async def examiner(self, racine: str) -> bool:
        """Promeut la racine si elle vient de passer le seuil (une seule fois)."""
        if self.db.get(f"toile_promu:{racine}") or self.service_connu(racine):
            return False
        st = self.stats_racine(racine)
        if not self.qualifie(st):
            return False
        return await self.promouvoir(racine, st)

    async def promouvoir(self, racine: str, st: dict) -> bool:
        now = time.time()
        while self._promus and now - self._promus[0] > 86400:
            self._promus.popleft()
        if len(self._promus) >= PROMOTIONS_PAR_JOUR:
            return False
        row = self.db.wallet(racine)
        if row is not None and ((row["grp"] or "") in self.p.bad_groups()
                                or (row["active"] and self.p.trust(racine) in TRUSTED)):
            self.db.put(f"toile_promu:{racine}", int(now))   # déjà connu (réseau à rugs ou wallet de confiance)
            return False
        if await self.est_service(racine):
            return False
        ex = self.db.toile_succes(racine, SUCCES_MC)
        role = (f"bank à succès (toile : {st['succes']} créateurs neufs sur {st['juges']} au-dessus de "
                f"{A.usd(SUCCES_MC)} après 24 h : " + ", ".join(f"${r['symbol'] or '?'} {A.usd(r['mc_24h'])}" for r in ex)
                + ")")
        label = f"TOILE_{racine[:4]}"
        if row is not None:
            self.db.set_wallet_role(racine, label, GROUP, role, 1)
        await self.p.watch(racine, label, GROUP, role, 1, None)
        self.db.put(f"toile_promu:{racine}", int(now))
        self._promus.append(now)
        self.stats["promus"] += 1
        log.info("Toile : %s promu (%d succès sur %d créateurs mesurés)", label, st["succes"], st["juges"])
        lignes = [f"🕸️ <b>TOILE : nouveau financeur à succès suivi</b> ({esc(label)})", f"<code>{racine}</code>",
                  f"{st['succes']} des {st['juges']} wallets neufs qu'il a financés ont lancé un token encore au-dessus "
                  f"de {A.usd(SUCCES_MC)} 24 h après :"]
        lignes += [f"• ${esc(r['symbol'] or '?')} {A.usd(r['mc_24h'])} · <code>{r['mint']}</code>" for r in ex]
        lignes.append("<i>Ses prochains wallets neufs seront signalés dès le financement, leur token dès la création.</i>")
        self.p.emit(Alert(f"toile:{racine}", "discovery", "\n".join(lignes), wallet=racine))
        return True

    # --- 5. alerte à la création -------------------------------------------------------------------------
    async def relier(self, creator: str) -> tuple[tuple[str, object], list[dict]] | None:
        """Premier maillon de la chaîne déjà connu du radar ((genre, wallet), maillons jusqu'à lui). Un exchange ou
        un service coupe la chaîne : ses clients n'ont rien en commun (vu en vrai : Binance)."""
        lw = self.p.lancements
        chaine = self.chaine(creator) or []
        for i, h in enumerate(chaine):
            if self.service_connu(h["src"]):
                return None
            connu = lw.connu(h["src"]) if lw is not None else None
            if connu is None:
                continue
            for maillon in chaine[:i + 1]:   # vérifié seulement quand il y a un lien (1 appel par financeur, mémorisé)
                if await self.est_service(maillon["src"]):
                    log.info("Toile : %s relié à %s via un service (%s), ignoré", creator[:6], connu[1]["label"],
                             maillon["src"][:6])
                    return None
            return connu, chaine[:i + 1]
        return None

    async def evaluer(self, t: dict, w) -> dict | None:
        """Nouveau token d'un wallet neuf : sa racine passe-t-elle le seuil ? un maillon est-il déjà connu ?"""
        if w["racine"]:
            await self.examiner(w["racine"])
        lw = self.p.lancements
        if lw is None or t["creator"] in self.p.watched:
            return None   # créateur déjà suivi : le pipeline l'alerte en direct
        relie = await self.relier(t["creator"])
        if relie is None:
            return None
        (genre, row), chaine = relie
        lw.pending.pop(t["mint"], None)   # déjà relié : pas de seconde remontée par Helius
        self.stats["alertes"] += 1
        res = {**t, "mc": None, "txns": None, "genre": genre, "wallet": row["address"], "label": row["label"],
               "grp": row["grp"], "role": row["role"], "depth": row["depth"], "chaine": chaine}
        log.info("Toile : %s ($%s) relié dès la création à %s (%s)", t["mint"][:6], t.get("symbol"), row["label"], genre)
        await lw.alerter(res)
        return res

    # --- suivi ------------------------------------------------------------------------------------------
    def status_line(self) -> str:
        n = self.db.toile_resume(SUCCES_MC)
        s = self.stats
        promus = len([1 for _k, v in self.db.settings_like("toile_promu:") if v and v != "0"])
        sante = " · ⚠️ RPC public en difficulté" if self.pub.recent_failures() + self.arc.recent_failures() >= 5 else ""
        return (f"🕸️ Toile : {n.get('neuf', 0)} wallets neufs reliés à {n['financeurs']} financeurs · "
                f"{n['succes']} succès sur {n['mesures']} tokens mesurés à 24 h · {promus} financeurs promus · "
                f"{s['alertes']} alertes · file {len(self.file)}"
                + (f" · {s['non_traites']} non traités" if s["non_traites"] else "") + sante)

    async def boucle(self) -> None:
        menage = 0.0
        while True:
            await asyncio.sleep(TOUR_S)
            try:
                await self.mesurer()
            except Exception:
                log.exception("Toile : mesure à 24 h en échec")
            if time.time() - menage > 86400:
                menage = time.time()
                n = self.db.toile_purge(int(menage - CACHE_JOURS * 86400), int(menage - GARDE_JOURS * 86400), SUCCES_MC)
                log.info("Toile : ménage, %d lignes anciennes retirées", n)

    def taches(self) -> list:
        return [*(self.worker() for _ in range(WORKERS)), self.boucle()]
