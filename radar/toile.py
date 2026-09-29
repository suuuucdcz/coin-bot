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
  3. 6 h après la création, le token est jugé sur son historique de prix (GeckoTerminal) : succès s'il a dépassé
     100 k$ sans chute brutale, même s'il redescend doucement ensuite (un coin qui s'éteint n'est pas un rug) ; rug
     s'il est tombé de plus de 50 % à moins de 10 % de son plus haut en moins d'une heure (mesuré : 15 à 30 min).
     Chaque succès est revu à 24 h et 72 h : les faux fonds du réseau Reserve tiennent parfois 1 à 2 jours avant
     d'être vidés ; un rug tardif déclasse le financeur ;
  4. une racine qui a financé au moins 2 créateurs à succès, avec au moins 25 % de réussite, qui n'est pas un
     service (exchange, bridge : trop de tx ou trop de clients), et dont le réseau n'a pas de rugs (même analyse
     que la découverte, quelques crédits Helius par promotion) entre dans la watchlist comme « bank à succès » :
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
from .analysis import network
from .analysis.tracer import HOT_WINDOW_S, scan_funding
from .confiance import TRUSTED
from .sources import dexscreener, geckoterminal
from .sources.helius import LAMPORTS, RpcError, SolanaRPC, account_keys, in_background
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
SUCCES_PIC = 100_000         # un succès a dépassé 100 k$ (courbe pump.fun terminée et au-delà)…
CANDIDAT_MC = 30_000         # … candidats : migrés, ou encore ≥ 30 k$ au moment du jugement
MIN_SUCCES = 2               # créateurs différents
MIN_TAUX = 0.25
MAX_CREATEURS_7J = 50        # au-delà : un service qui finance ses clients, pas une équipe
PROMOTIONS_PAR_JOUR = 20
JUGE_APRES_S = 6 * 3600      # premier jugement 6 h après la création
REVUES_S = (86400, 3 * 86400)  # un succès est revu à 24 h et à 72 h (rug tardif ?)
ABANDON_S = 3 * 86400        # sans données trois jours après : raté
JUGES_PAR_TOUR = 600
BOUGIES_PAR_TOUR = 40        # historiques de prix lus par tour (GeckoTerminal : 30 requêtes / min)
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

    # --- 3. jugement des tokens : a-t-il marché, et comment est-il retombé ? -------------------------------
    async def _verdict(self, mint: str) -> tuple[str, float | None] | None:
        """(verdict, plus haut) d'après l'historique de prix ; None sans données (réessayé)."""
        r = await geckoterminal.chute_token(self.p.http, self.db, mint)
        if r is None:
            return None
        if r["pic"] < SUCCES_PIC:
            return "raté", r["pic"]
        return ("rug" if r["chute"] == "brutale" else "succès"), r["pic"]

    async def juger(self, now: float | None = None) -> int:
        """Premier jugement 6 h après la création, puis revue des succès à 24 h et 72 h. Renvoie le nombre de jugements."""
        now = now or time.time()
        if self.p.http is None:
            return 0
        n = bougies = 0
        rows = self.db.toile_a_juger(int(now - JUGE_APRES_S), JUGES_PAR_TOUR)
        candidats = []
        for i in range(0, len(rows), LOT_DEXSCREENER):
            lot = rows[i:i + LOT_DEXSCREENER]
            marches = await dexscreener.markets(self.p.http, [r["mint"] for r in lot])
            if not marches:
                # DexScreener muet sur tout un lot : panne probable, on réessaie (abandon après 3 jours)
                for r in lot:
                    if now - r["ts"] > ABANDON_S:
                        self.db.toile_juger(r["mint"], "raté", None, int(now))
                        n += 1
                continue
            for r in lot:
                m = marches.get(r["mint"]) or {}
                if m.get("dex") not in (None, "pumpfun") or (m.get("mc") or 0) >= CANDIDAT_MC:
                    candidats.append(r)           # a quitté la courbe pump.fun, ou vaut encore quelque chose
                else:
                    self.db.toile_juger(r["mint"], "raté", None, int(now))
                    n += 1
        nouveaux = []
        for r in candidats:
            if bougies >= BOUGIES_PAR_TOUR:
                break                             # la suite au prochain tour
            bougies += 1
            v = await self._verdict(r["mint"])
            if v is None:
                if now - r["ts"] > ABANDON_S:
                    self.db.toile_juger(r["mint"], "raté", None, int(now))
                    n += 1
                continue
            self.db.toile_juger(r["mint"], v[0], v[1], int(now))
            n += 1
            if v[0] == "succès":
                nouveaux.append(r)
            elif v[0] == "rug":
                await self.declasser(r)   # un financeur déjà promu qui lance un rug est déclassé
        # Revue des succès (24 h, 72 h) : un rug tardif les déclasse, et déclasse le financeur s'il avait été promu
        for r in self.db.toile_a_revoir(int(now), REVUES_S, max(0, BOUGIES_PAR_TOUR - bougies)):
            v = await self._verdict(r["mint"])
            if v is None:
                continue
            self.db.toile_juger(r["mint"], v[0], v[1], int(now))
            if v[0] == "rug":
                await self.declasser(r)
        self.stats["juges"] += n
        for r in nouveaux:
            self.stats["succes"] += 1
            w = self.db.toile_wallet(r["creator"])
            if w is not None and w["racine"]:
                await self.examiner(w["racine"])
        return n

    async def declasser(self, token) -> None:
        """Un succès s'est effondré d'un coup : son financeur, s'il avait été promu, passe en réseau à rugs."""
        w = self.db.toile_wallet(token["creator"])
        racine = w["racine"] if w is not None else None
        if not racine or self.db.get(f"toile_promu:{racine}") in (None, "0"):
            return
        label = f"TOILE_{racine[:4]}"
        sym = token["symbol"] or "?"
        self.db.set_wallet_role(racine, label, "reseau-rugs",
                                f"financeur déclassé par la toile : rug brutal de ${sym} après un succès", 1)
        self.db.put(f"toile_promu:{racine}", "0")
        log.warning("Toile : %s déclassé (rug brutal de $%s)", label, sym)
        self.p.emit(Alert(f"toile_declasse:{racine}", "discovery",
                          f"⛔ <b>TOILE : {esc(label)} déclassé</b>\n<code>{racine}</code>\n"
                          f"${esc(sym)}, lancé par un de ses wallets neufs, vient de s'effondrer d'un coup (rug). "
                          "Ses prochains wallets seront marqués ⛔.", wallet=racine))

    # --- 4. financeurs à succès -------------------------------------------------------------------------
    def stats_racine(self, racine: str) -> dict:
        return self.db.toile_stats(racine, int(time.time() - 7 * 86400))

    @staticmethod
    def qualifie(st: dict) -> bool:
        # Aucun rug brutal parmi ses wallets neufs : un opérateur qui vide ses tokens n'est jamais « à succès »
        return (st["succes"] >= MIN_SUCCES and st["succes"] >= MIN_TAUX * st["juges"] and not st["rugs"]
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
        ex = self.db.toile_succes(racine)
        if await self.reseau_a_rugs(racine, ex):
            return False
        role = (f"bank à succès (toile : {st['succes']} créateurs neufs sur {st['juges']} au-dessus de "
                f"{A.usd(SUCCES_PIC)} sans rug : " + ", ".join(f"${r['symbol'] or '?'} {A.usd(r['pic'])}" for r in ex)
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
                  f"{st['succes']} des {st['juges']} wallets neufs qu'il a financés ont lancé un token monté au-dessus "
                  f"de {A.usd(SUCCES_PIC)} sans rug brutal (réseau vérifié) :"]
        lignes += [f"• ${esc(r['symbol'] or '?')} plus haut {A.usd(r['pic'])} · <code>{r['mint']}</code>" for r in ex]
        lignes.append("<i>Ses prochains wallets neufs seront signalés dès le financement, leur token dès la création.</i>")
        self.p.emit(Alert(f"toile:{racine}", "discovery", "\n".join(lignes), wallet=racine))
        return True

    async def reseau_a_rugs(self, racine: str, succes: list) -> bool:
        """Même analyse de réseau que la découverte, sur le créateur du meilleur succès : des rugs dans le réseau
        (projets vidés, faux succès à plusieurs M$, relais au même montant…) = pas de promotion. Vu en vrai : ces
        réseaux font monter leurs tokens à plusieurs M$ pendant 1 à 5 jours avant de les vider."""
        if not succes:
            return True
        try:
            rep = await in_background(network.quick(self.p, succes[0]["creator"], network.CHUTES_PAR_ANALYSE))
        except Exception as e:
            log.info("Toile : réseau de %s non vérifiable (%s), promotion reportée", racine[:6], e)
            return True   # dans le doute, pas de promotion (réessayé au prochain succès)
        flag = network.quick_verdict(rep)
        if flag and not flag.startswith(network.LEURRE_FLAG):
            log.info("Toile : %s non promu, réseau douteux : %s", racine[:6], flag)
            self.db.put(f"toile_promu:{racine}", "0")   # jugé : plus réexaminé
            return True
        return False

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
        if t["creator"] in self.p.watched:
            return None   # créateur déjà suivi : le pipeline l'alerte en direct
        if self.p.cibles is not None:
            chaine = self.chaine(t["creator"]) or []
            if chaine and await self.p.cibles.lien_toile(t, chaine):
                return None   # 🎯 financé par un wallet d'une cible : son nouveau coin (alerte de la cible)
        lw = self.p.lancements
        if lw is None:
            return None
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
        n = self.db.toile_resume()
        s = self.stats
        promus = len([1 for _k, v in self.db.settings_like("toile_promu:") if v and v != "0"])
        sante = " · ⚠️ RPC public en difficulté" if self.pub.recent_failures() + self.arc.recent_failures() >= 5 else ""
        return (f"🕸️ Toile : {n.get('neuf', 0)} wallets neufs reliés à {n['financeurs']} financeurs · "
                f"{n['succes']} succès et {n['rugs']} rugs sur {n['juges']} tokens jugés · {promus} financeurs promus · "
                f"{s['alertes']} alertes · file {len(self.file)}"
                + (f" · {s['non_traites']} non traités" if s["non_traites"] else "") + sante)

    async def boucle(self) -> None:
        menage = 0.0
        while True:
            await asyncio.sleep(TOUR_S)
            try:
                await self.juger()
            except Exception:
                log.exception("Toile : jugement des tokens en échec")
            if time.time() - menage > 86400:
                menage = time.time()
                n = self.db.toile_purge(int(menage - CACHE_JOURS * 86400), int(menage - GARDE_JOURS * 86400))
                log.info("Toile : ménage, %d lignes anciennes retirées", n)

    def taches(self) -> list:
        return [*(self.worker() for _ in range(WORKERS)), self.boucle()]
