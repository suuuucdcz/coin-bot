"""🎯 Cibles : un dev suivi de près, dans SA section Telegram. Tout ce que font ses wallets, en direct, jusqu'à son
prochain coin.

Une cible = le wallet du dev, les wallets de son opérateur (bank, wallet de lancement…) et tous les wallets neufs
qu'ils financent (sans limite de profondeur), groupe « cible:<NOM> ». Pour eux, aucun filtre anti-bruit (bavard,
sourdine, tempête, sniper, usine, ferme, purge des inactifs, plafond de la watchlist).

Deux façons de suivre un wallet :
  - raconté : chaque transaction est racontée dans la section (le dev, le bank…) ;
  - discret (rôle contenant « discret ») : les wallets de bundle (50 wallets qui achètent tous au lancement), les
    mélangeurs, les bots. Rien n'est raconté un par un : leurs achats servent à repérer le coin, leurs ventes font un
    bilan, leurs armements déclenchent l'alerte « armement ». Ils sortent aussi des alertes ordinaires du radar.

Le prochain coin est repéré par quatre chemins indépendants (le premier arrivé alerte, les autres sont dédoublonnés) :
  1. un wallet de la cible crée un token (transaction lue par Helius) ;
  2. le flux des nouveaux tokens pump.fun (PumpPortal) : le créateur est un wallet de la cible (secours si Helius rate) ;
  3. la toile : le créateur d'un nouveau token a été financé par un wallet de la cible (wallet qu'on ne suivait pas) ;
  4. plusieurs wallets de la cible achètent le même token JEUNE en 10 min (le bundle au lancement) : c'est son coin,
     même s'il l'a créé depuis un wallet financé par un exchange (sans lien visible).
Ces alertes passent devant toutes les autres dans la file Telegram, partent aussi dans ‼️ (avec le son) et sont
épinglées dans la section.

Avant le coin, l'ARMEMENT : un wallet de la cible reçoit des virements identiques de 5 wallets différents en 3 min
(outil de bundle : vu le 19/09 sur $PUMPINU, 10 × 0,0339 SOL au dev et 10 × 6,84 SOL à un acheteur du bundle, 2 min
avant la création), ou ses wallets financent 8 wallets neufs en 2 min (mélangeur).

Ajouter une cible : python -m radar.cible ajouter <NOM> <adresse du dev> [--wallets a b] [--discrets c d] [--notes "..."]
Retirer des wallets : python -m radar.cible retirer <NOM> a b c
(pris en compte par le radar en moins d'une minute, sans redémarrage).
"""
from __future__ import annotations

import asyncio
import logging
import time
import zlib
from collections import OrderedDict, defaultdict, deque

from . import alerts as A
from .analysis.classify import analyze, programs, token_deltas
from .analysis.enrich import TokenInfo
from .sources import pumpfun
from .sources.helius import AMM_PROGRAMS, IGNORED_MINTS, PUMP_FUN, account_keys, in_background, sol_deltas
from .telegram import esc

log = logging.getLogger("cible")

PREFIXE = "cible:"
DISCRET = "discret"          # mot du rôle d'un wallet suivi sans être raconté
COULEUR = 16766590
ACHAT_GROUPE_S = 600         # plusieurs wallets de la cible sur le même token en 10 min : son coin
COIN_JEUNE_S = 1800          # … seulement sur un token de moins de 30 min (pas un vieux coin, pas un stablecoin)
LANCEMENT_S = 120            # achats dans les 2 min qui suivent la création : le bundle du lancement
NEUF_MAX_TX = 5              # un destinataire avec 5 tx au plus = wallet neuf : il rejoint la cible
POUSSIERE_SOL = 0.002        # en dessous, sans token : bruit (address poisoning, frais)
ENVOI_MIN_SOL = 0.003        # virement suivi à partir de 0,003 SOL (au-dessus du loyer d'un compte de token)
ENVOIS_MAX = 25              # destinataires lus au plus dans une même transaction
IMPORTANT_SOL = 0.05         # au-dessus : message avec le son
EXISTANT_FORT_SOL = 1.0      # envoi vers un wallet existant : avec le son à partir de 1 SOL (paiements quotidiens discrets)
GROS_INTERNE_SOL = 20        # gros mouvement entre ses wallets (19/09 : 170 SOL au wallet de lancement 8 min avant)
DISCRET_SOL = 50             # wallet discret : SOL reçu ou envoyé raconté à partir de 50 SOL (armement : 10 × 6,84)
ARMEMENT_S = 180             # armement : virements identiques (± 3 %) de 5 wallets différents en 3 min
ARMEMENT_MIN = 5
ARMEMENT_ECART = 0.03
ARMEMENT_NEUFS = 8           # … ou 8 wallets neufs financés par la cible en 2 min (le 19/09 : 3 min avant la création)
ARMEMENT_PAUSE_S = 900       # une alerte d'armement par cible et par quart d'heure
RAFALE_S = 120               # 3 envois de SOL vers des wallets inconnus en 2 min : une rafale (mélangeur, bundle,
RAFALE_MIN = 3               # paiements ; le 19/09, 100+ montants tous différents) : suivis en silence, en tâche de fond
AJOUTS_MAX_H = 400           # garde-fou : wallets neufs ajoutés à une cible par heure
RATTRAPAGE_S = (8, 22)       # relecture d'un wallet neuf ajouté à +8 s et +30 s (un mélangeur va plus vite que l'abonnement)
RELECTURES_RELAIS_S = (8, 22, 60, 150)   # relais non abonné : relu à +8 s, +30 s, +90 s, +4 min (création : 2-3 min après)
PURGE_S = 3600               # chaque heure : les relais vidés (ajoutés il y a plus de 2 h, < 0,001 SOL) sont retirés
PURGE_AGE_S = 7200
PURGE_SOLDE = 0.001
BILANS_S = (120, 600)        # bilans du bundle après son coin (2 min, 10 min)
VUS_MAX = 5000
RECHARGE_S = 60


def topic(nom: str) -> str:
    return f"cible_{nom.lower()}"


def _qte(raw: int, dec: int) -> str:
    v = raw / 10 ** dec if dec else float(raw)
    return f"{v / 1e9:.2f} Md" if v >= 1e9 else f"{v / 1e6:.2f} M" if v >= 1e6 else f"{v / 1e3:.1f} k" if v >= 1e3 \
        else f"{v:g}"


def _proches(a: float, b: float, ecart: float) -> bool:
    return abs(a - b) <= ecart * max(a, b)


class Cibles:
    def __init__(self, pipeline, tg):
        self.p = pipeline
        self.tg = tg
        self.db = pipeline.db
        self.membres: dict[str, str] = {}                         # adresse -> nom de la cible
        self.discrets: set[str] = set()                           # membres suivis sans être racontés
        self.noms: set[str] = set()
        self._achats: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
        self._fiches: dict[str, dict] = {}                        # fiche pump.fun de chaque token vu (mint -> fiche)
        self._taches: set = set()                                 # références des tâches lancées (sinon perdues)
        self._vus: OrderedDict[str, None] = OrderedDict()         # transactions déjà lues (websocket ou rattrapage)
        self._recus: dict[str, deque] = defaultdict(deque)        # wallet -> (heure, SOL, source) reçus de l'extérieur
        self._neufs: dict[str, deque] = defaultdict(deque)        # cible -> heures des wallets neufs financés
        self._envois: dict[str, deque] = defaultdict(deque)       # cible -> heures des envois vers des inconnus
        self._annonces: set[tuple[str, str]] = set()              # (cible, mint) déjà annoncés
        self._ajouts: dict[str, deque] = defaultdict(deque)       # cible -> heures des ajouts automatiques
        self._arme: dict[str, float] = {}                         # cible -> heure de la dernière alerte d'armement
        self._bundle: dict[tuple[str, str], dict] = {}            # (cible, mint) -> achats / ventes de ses wallets

    # --- membres et sections --------------------------------------------------------------------------
    def recharger(self) -> list[str]:
        """Relit la base ; renvoie les cibles nouvelles (à préparer)."""
        self.membres = self.db.cible_membres()
        self.discrets = self.db.cible_discrets()
        nouvelles = [r["nom"] for r in self.db.cibles() if r["nom"] not in self.noms]
        return nouvelles

    def cible_de(self, adresse: str | None) -> str | None:
        return self.membres.get(adresse) if adresse else None

    async def preparer(self, nom: str) -> None:
        """Section de la cible (créée si besoin) et fiche épinglée (renvoyée quand ses notes changent)."""
        self.noms.add(nom)
        if self.tg is None:
            return
        self.tg.add_topic(topic(nom), "🎯", f"Cible {nom}", COULEUR)
        try:
            await self.tg.ensure_topic(topic(nom))
        except Exception as e:
            log.warning("Section de la cible %s non créée : %s", nom, e)
        row = next((r for r in self.db.cibles() if r["nom"] == nom), None)
        wallets = [a for a, n in self.membres.items() if n == nom]
        n_discrets = sum(1 for a in wallets if a in self.discrets)
        notes = (row["notes"] if row else "") or ""
        fiche = [f"🎯 <b>CIBLE {esc(nom)}</b> : tout ce que font ses wallets, en direct",
                 f"Dev : <code>{row['racine'] if row else '?'}</code>",
                 f"{len(wallets)} wallets suivis, dont {n_discrets} en silence (bundle, mélangeur : seuls leurs "
                 "achats groupés, leurs armements et les gros montants sortent). Chaque wallet neuf financé rejoint "
                 "la cible automatiquement."]
        if notes:
            fiche += ["", esc(notes)]
        fiche += ["", "<b>Avant le coin</b> : ⚠️ ARMEMENT quand ses wallets reçoivent des virements identiques de "
                  "plusieurs wallets (outil de bundle), en général 1 à 3 min avant la création.",
                  "<b>Son prochain coin</b> : alerte 🎯 ici ET dans ‼️ (avec le son, avant tous les autres messages), "
                  "épinglée : créé par un de ses wallets, par un wallet qu'il a financé (même inconnu), ou acheté par "
                  "son bundle dans les minutes qui suivent la création."]
        version = zlib.crc32(notes.encode())
        self._envoyer(nom, "\n".join(fiche), cle=f"cible_fiche:{nom}:{version}", fort=False, epingler=True)

    async def ajouter(self, adresse: str, nom: str, role: str, parent: str | None, discret: bool = False,
                      rattraper: bool = False, abonner: bool = True) -> bool:
        """Un wallet rejoint la cible : surveillé, sans filtre, jamais purgé (sauf relais vidés).

        abonner=False (relais d'une rafale) : pas d'abonnement temps réel (des centaines de relais rempliraient la file
        des transactions juste avant la création) ; il est relu en tâche de fond et reste reconnu comme membre (sa
        création de token part tout de suite par le flux pump.fun, son financement par la toile)."""
        if adresse in self.membres:
            return False
        now = time.time()
        ajouts = self._ajouts[nom]
        while ajouts and now - ajouts[0] > 3600:
            ajouts.popleft()
        if len(ajouts) >= AJOUTS_MAX_H:
            log.warning("Cible %s : %d wallets ajoutés en 1 h, %s n'est pas ajouté (garde-fou)", nom, len(ajouts),
                        adresse[:6])
            return False
        ajouts.append(now)
        if discret and DISCRET not in role:
            role = f"{role} · suivi {DISCRET}"
        par = self.db.wallet(parent) if parent else None
        depth = min(9, ((par["depth"] if par else 1) or 1) + 1)
        label = f"{nom}_W_{adresse[:4]}"
        if self.db.wallet(adresse) is not None:
            self.db.set_wallet_role(adresse, label, PREFIXE + nom, role, depth)
        self.db.add_wallet(adresse, label, PREFIXE + nom, role, depth, parent)   # ajoute ou réactive
        self.membres[adresse] = nom
        if discret:
            self.discrets.add(adresse)
        if abonner:
            self.p.watched.add(adresse)
            if self.p.watcher is not None:
                await self.p.watcher.add(adresse)
        if rattraper and self.p.rpc is not None:
            self._lancer(self._rattraper(adresse, RATTRAPAGE_S if abonner else RELECTURES_RELAIS_S))
        log.info("Cible %s : %s ajouté (%s)", nom, adresse[:6], role)
        return True

    async def _rattraper(self, adresse: str, attentes=RATTRAPAGE_S) -> None:
        """Relit les premières transactions d'un wallet neuf : celles passées avant l'abonnement (mélangeurs), ou
        toutes celles d'un relais non abonné."""
        for attente in attentes:
            await asyncio.sleep(attente)
            if adresse not in self.membres:
                return
            try:
                sigs = await in_background(self.p.rpc.signatures(adresse, limit=20))
            except Exception as e:
                log.debug("Rattrapage %s : %s", adresse[:6], e)
                continue
            for s in reversed(sigs or []):
                sig = s.get("signature")
                if not sig or sig in self._vus or s.get("err") is not None:
                    continue
                try:
                    tx = await in_background(self.p.rpc.transaction(sig))
                except Exception:
                    continue
                if tx:
                    await self.on_tx(sig, tx)

    def _lancer(self, coro) -> None:
        tache = asyncio.get_running_loop().create_task(coro)
        self._taches.add(tache)
        tache.add_done_callback(self._taches.discard)

    # --- envoi ----------------------------------------------------------------------------------------
    def _envoyer(self, nom: str, texte: str, markup: dict | None = None, cle: str | None = None, fort: bool = False,
                 epingler: bool = False, top: bool = False, urgent: bool = False) -> None:
        if self.tg is None or self.p.dry_run:
            return
        self.tg.enqueue(texte, markup, key=cle, kind="cible" if fort else "cible_info", topic=topic(nom),
                        on_sent=self._epingler if epingler else None, urgent=urgent)
        if top:
            self.tg.enqueue_top(texte, markup, key=f"top:{cle}" if cle else None, urgent=urgent)

    def _epingler(self, message_id: int) -> None:
        self._lancer(self.tg.pin(message_id))

    def _nom(self, adresse: str) -> str:
        lab = self.p.label(adresse)
        return f"<b>{esc(lab)}</b>" if lab else f"<code>{A.short(adresse)}</code>"

    async def _symbole(self, mint: str) -> tuple[str, dict]:
        """(ticker, fiche pump.fun) ; la fiche est gardée (créateur compris) dès qu'elle a été lue."""
        c = self._fiches.get(mint)
        if c is None and self.p.http is not None:
            try:
                c = await asyncio.wait_for(pumpfun.coin(self.p.http, mint), 4) or None
            except Exception:
                c = None
            if c and c.get("symbol"):
                self._fiches[mint] = c
        return (c or {}).get("symbol") or "?", c or {}

    # --- 🎯 le prochain coin ------------------------------------------------------------------------------
    async def nouveau_coin(self, nom: str, mint: str, createur: str, comment: str, symbole: str | None = None,
                           nom_token: str | None = None) -> None:
        if (nom, mint) in self._annonces:
            return                             # déjà annoncé par un autre chemin (création, bundle, toile, flux)
        self._annonces.add((nom, mint))
        if not symbole:
            symbole, c = await self._symbole(mint)
            nom_token = nom_token or c.get("name")
        info = TokenInfo(mint, name=nom_token, symbol=symbole, creator=createur)
        texte = (f"🎯 <b>CIBLE {esc(nom)} : NOUVEAU COIN — ${esc(symbole or '?')}</b>"
                 + (f" ({esc(nom_token)})" if nom_token else "") + f"\n{comment}\n<code>{mint}</code>\n"
                 f"Créateur : <code>{createur}</code>")
        self._envoyer(nom, texte, A.token_buttons(info, createur), cle=f"cible_crea:{mint}", fort=True, epingler=True,
                      top=True, urgent=True)
        log.warning("🎯 Cible %s : nouveau coin %s ($%s) — %s", nom, mint, symbole, comment)
        self._lancer(self._bilan_bundle(nom, mint, symbole or "?"))
        if createur and len(createur) >= 32 and createur not in self.membres:
            await self.ajouter(createur, nom, f"créateur de ${symbole} (cible {nom})", None)

    def on_new_token(self, msg: dict) -> None:
        """Flux pump.fun (PumpPortal) : un créateur de la cible (secours si la transaction Helius manque)."""
        createur, mint = msg.get("traderPublicKey"), msg.get("mint")
        nom = self.cible_de(createur)
        if nom and mint:
            self._lancer(self.nouveau_coin(nom, mint, createur, f"Créé par {self._nom(createur)} (flux pump.fun)",
                                           msg.get("symbol"), msg.get("name")))

    async def lien_toile(self, t: dict, chaine: list[dict]) -> bool:
        """La toile a relié un nouveau créateur à un wallet de la cible (un wallet qu'on ne suivait pas encore)."""
        for h in chaine:
            nom = self.cible_de(h["src"])
            if nom:
                await self.nouveau_coin(nom, t["mint"], t["creator"],
                                        f"Créé par un wallet financé par {self._nom(h['src'])} ({h['sol']:g} SOL), "
                                        "repéré par la toile", t.get("symbol"), t.get("name"))
                return True
        return False

    # --- ⚠️ l'armement, juste avant le coin -----------------------------------------------------------------
    def _armement(self, nom: str, raison: str) -> None:
        now = time.time()
        if now - self._arme.get(nom, 0) < ARMEMENT_PAUSE_S:
            return
        self._arme[nom] = now
        texte = (f"⚠️ 🎯 <b>CIBLE {esc(nom)} : ARMEMENT EN COURS</b>\n{raison}\n"
                 "C'est ce que fait un outil de bundle juste avant un lancement (vu sur $PUMPINU : création 2 à 3 min "
                 "après). Le CA partira ici et dans ‼️ dès la création.")
        self._envoyer(nom, texte, cle=f"cible_arme:{nom}:{int(now // ARMEMENT_PAUSE_S)}", fort=True, top=True,
                      urgent=True)
        log.warning("⚠️ Cible %s : armement — %s", nom, raison)

    def _note_recu(self, nom: str, wallet: str, sol: float, src: str | None) -> None:
        """SOL reçu : des virements identiques de 5 wallets différents en 3 min = armement (les relais d'un mélangeur
        suivi par la cible comptent aussi)."""
        if not src or src == wallet:
            return
        now = time.time()
        recus = self._recus[wallet]
        recus.append((now, sol, src))
        while recus and now - recus[0][0] > ARMEMENT_S:
            recus.popleft()
        sources = {s for (_t, v, s) in recus if _proches(v, sol, ARMEMENT_ECART)}
        if len(sources) >= ARMEMENT_MIN:
            self._armement(nom, f"{self._nom(wallet)} reçoit <b>{len(sources)} virements identiques</b> "
                                f"({sol:.4g} SOL) de {len(sources)} wallets différents en moins de "
                                f"{ARMEMENT_S // 60} min.")

    def _note_neuf(self, nom: str) -> None:
        now = time.time()
        neufs = self._neufs[nom]
        neufs.append(now)
        while neufs and now - neufs[0] > RAFALE_S:
            neufs.popleft()
        if len(neufs) >= ARMEMENT_NEUFS:
            self._armement(nom, f"Ses wallets financent <b>{len(neufs)} wallets neufs</b> en moins de "
                                f"{RAFALE_S // 60} min (mélangeur / bundle).")

    def _note_envoi(self, nom: str) -> int:
        """Envois de SOL de la cible vers des wallets inconnus dans les 2 dernières minutes (celui-ci compris)."""
        now = time.time()
        envois = self._envois[nom]
        envois.append(now)
        while envois and now - envois[0] > RAFALE_S:
            envois.popleft()
        return len(envois)

    async def _suivre_si_neuf(self, nom: str, src: str, dst: str, sol: float) -> None:
        """En tâche de fond (rafale, wallet discret) : le destinataire rejoint la cible, en silence, s'il est neuf."""
        try:
            n = len(await in_background(self.p.rpc.signatures(dst, limit=NEUF_MAX_TX + 1)))
        except Exception:
            return
        if n <= NEUF_MAX_TX and await self.ajouter(
                dst, nom, f"wallet neuf financé par {self.p.label(src) or src[:6]} ({sol:g} SOL)", src, discret=True,
                rattraper=True, abonner=False):
            self._note_neuf(nom)

    # --- 📊 le bundle ---------------------------------------------------------------------------------------
    def _note_bundle(self, nom: str, mint: str, wallet: str, achat: bool, sol: float) -> None:
        b = self._bundle.setdefault((nom, mint), {"wallets": set(), "achats": 0, "sol_a": 0.0, "ventes": 0,
                                                  "sol_v": 0.0})
        b["wallets"].add(wallet)
        if achat:
            b["achats"] += 1
            b["sol_a"] += sol
        else:
            b["ventes"] += 1
            b["sol_v"] += sol

    async def _bilan_bundle(self, nom: str, mint: str, sym: str) -> None:
        debut = time.time()
        for quand in BILANS_S:
            await asyncio.sleep(max(0.0, debut + quand - time.time()))
            b = self._bundle.get((nom, mint))
            if not b:
                continue
            etat = ("il a déjà revendu plus qu'il n'a acheté : il sort" if b["sol_v"] >= b["sol_a"] > 0
                    else "il vend" if b["ventes"] else "il n'a pas encore vendu")
            self._envoyer(nom, f"📊 <b>{esc(nom)}</b> · bundle sur ${esc(sym)} à +{quand // 60} min : "
                               f"{len(b['wallets'])} wallets de la cible · {b['achats']} achats ({b['sol_a']:.2f} SOL) · "
                               f"{b['ventes']} ventes ({b['sol_v']:.2f} SOL) : {etat}\n<code>{mint}</code>",
                          cle=f"cible_bilan:{mint}:{quand}", fort=True)
        await asyncio.sleep(3600)
        self._bundle.pop((nom, mint), None)

    # --- tout ce que font ses wallets ----------------------------------------------------------------
    async def on_tx(self, sig: str, tx: dict) -> None:
        if not self.membres or (tx.get("meta") or {}).get("err") is not None or sig in self._vus:
            return
        self._vus[sig] = None
        while len(self._vus) > VUS_MAX:
            self._vus.popitem(last=False)
        keys = account_keys(tx)
        tdeltas = token_deltas(tx)
        touches = {k for k in keys if k in self.membres} | {o for (o, _m) in tdeltas if o in self.membres}
        par_cible: dict[str, list[str]] = defaultdict(list)
        for w in touches:
            par_cible[self.membres[w]].append(w)
        for nom, wallets in par_cible.items():
            try:
                lignes, fort = await self._raconter(nom, wallets, sig, tx, tdeltas)
            except Exception:
                log.exception("Cible %s : transaction %s non racontée", nom, sig[:8])
                continue
            if lignes:
                heure = time.strftime("%H:%M:%S", time.localtime(tx.get("blockTime") or time.time()))
                texte = f"🎯 <b>{esc(nom)}</b> · {heure}\n" + "\n".join(lignes) + \
                    f"\n<a href=\"https://solscan.io/tx/{sig}\">transaction</a>"
                self._envoyer(nom, texte, cle=f"cible:{nom}:{sig}", fort=fort)

    async def _raconter(self, nom: str, wallets: list[str], sig: str, tx: dict, tdeltas) -> tuple[list[str], bool]:
        events = analyze(tx, set(wallets))
        deltas = sol_deltas(tx)
        lignes: list[str] = []
        fort = False
        decrits = set()
        for ev in events:
            if ev.kind == "transfer":
                continue    # les virements de SOL sont lus plus bas, dès 0,003 SOL (mélangeur du 19/09 : 0,0085 SOL)
            qui = self._nom(ev.wallet)
            discret = ev.wallet in self.discrets
            decrits.add(ev.wallet)
            if ev.kind == "create":
                await self.nouveau_coin(nom, ev.mint, ev.wallet, f"Créé par {qui} sur {esc(ev.dex or '?')}")
                lignes.append(f"🎯 {qui} <b>crée un token</b> : <code>{ev.mint}</code> (alerte épinglée)")
                fort = True
            elif ev.kind in ("buy", "sell"):
                self._note_bundle(nom, ev.mint, ev.wallet, ev.kind == "buy", ev.sol)
                sym, c = await self._symbole(ev.mint)
                if ev.kind == "buy":
                    await self._achat_groupe(nom, ev.wallet, ev.mint, sym)
                if discret:
                    continue
                q = _qte(ev.tokens_raw, ev.decimals)
                if ev.kind == "buy":
                    lignes.append(f"🟠 {qui} <b>achète</b> {q} ${esc(sym)} pour {ev.sol:.3f} SOL"
                                  + self._age(c) + f"\n   <code>{ev.mint}</code>")
                    fort = True
                else:
                    lignes.append(f"🔻 {qui} <b>vend</b> {q} ${esc(sym)} contre {ev.sol:.3f} SOL\n   <code>{ev.mint}</code>")
                    fort = fort or ev.sol >= IMPORTANT_SOL
            elif ev.kind in ("supply_in", "supply_out", "lp_add"):
                if discret:
                    continue
                sym, c = await self._symbole(ev.mint)
                q = _qte(ev.tokens_raw, ev.decimals)
                if ev.kind == "supply_in":
                    lignes.append(f"📥 {qui} reçoit {q} ${esc(sym)} sans payer\n   <code>{ev.mint}</code>")
                elif ev.kind == "supply_out":
                    lignes.append(f"📤 {qui} envoie {q} ${esc(sym)} à {self._nom(ev.other or '')}\n   <code>{ev.mint}</code>")
                else:
                    lignes.append(f"🟢 {qui} ajoute de la liquidité : ${esc(sym)} + {ev.sol:.3f} SOL\n   <code>{ev.mint}</code>")
                fort = True
        # SOL envoyé (sans token acheté ni vendu) : à chaque destinataire, qui rejoint la cible s'il est neuf
        for w in wallets:
            if deltas.get(w, 0.0) >= -POUSSIERE_SOL or w in decrits or any(
                    o == w and a != b and m not in IGNORED_MINTS for (o, m), (a, b, _d) in tdeltas.items()):
                continue
            dests = sorted(((v, k) for k, v in deltas.items() if k != w and v >= ENVOI_MIN_SOL), reverse=True)
            if dests:
                decrits.add(w)
            for v, k in dests[:ENVOIS_MAX]:
                ligne, important = await self._envoi(nom, w, k, v)
                if ligne:
                    lignes.append(ligne)
                fort = fort or important
        # SOL reçu, et tout ce que l'analyse n'a pas décrit (rien n'est passé sous silence, sauf pour les discrets)
        for w in wallets:
            d = deltas.get(w, 0.0)
            discret = w in self.discrets
            if d > POUSSIERE_SOL and not any(e.wallet == w and e.kind == "sell" for e in events):
                src = min(((v, k) for k, v in deltas.items() if k != w and v < 0), default=(0, None))[1]
                self._note_recu(nom, w, d, src)
                decrits.add(w)
                if discret and d < DISCRET_SOL:
                    continue
                lignes.append(f"💰 {self._nom(w)} <b>reçoit</b> {d:.4f} SOL de {self._source(src)}")
                fort = fort or d >= IMPORTANT_SOL
            elif w not in decrits and not discret:
                bouge = [(m, a, b, dec) for (o, m), (a, b, dec) in tdeltas.items() if o == w and a != b]
                if abs(d) <= POUSSIERE_SOL and not bouge:
                    continue   # frais seuls, poussière : bruit
                if bouge and all(m in IGNORED_MINTS for (m, _a, _b, _d) in bouge):
                    lignes.append(f"💱 {self._nom(w)} : {d:+.4f} SOL contre des stablecoins (USDC / USDT / wSOL)")
                    continue
                progs = self._programmes(tx)
                lignes.append(f"⚙️ {self._nom(w)} : {d:+.4f} SOL" + (f" · {len(bouge)} token(s) modifié(s)" if bouge else "")
                              + (f" · {progs}" if progs else ""))
        return lignes, fort

    def _age(self, c: dict) -> str:
        cree = c.get("created")
        if not cree:
            return ""
        age = int(time.time() - cree)
        createur = c.get("creator")
        lien = " (créé par un wallet de la cible)" if self.cible_de(createur) else ""
        return f" · token de {A.age(age)}{lien}"

    def _source(self, adresse: str | None) -> str:
        if not adresse:
            return "?"
        if self.cible_de(adresse):
            return f"{self._nom(adresse)} (cible)"
        lab = self.db.get_label(adresse)
        if lab:
            return f"<b>{esc(lab)}</b>"
        if self.p.is_service_address(adresse):
            return f"un exchange / service (<code>{A.short(adresse)}</code>)"
        return f"<code>{adresse}</code>"

    @staticmethod
    def _programmes(tx: dict) -> str:
        noms = sorted({AMM_PROGRAMS[p] for p in programs(tx) if p in AMM_PROGRAMS}
                      | ({"pump.fun"} if PUMP_FUN in programs(tx) else set()))
        return ", ".join(noms)

    async def _envoi(self, nom: str, src: str, dst: str | None, sol: float) -> tuple[str | None, bool]:
        """SOL envoyé par un wallet de la cible : interne, vers un exchange, ou vers un wallet NEUF (qui la rejoint)."""
        qui = self._nom(src)
        discret = src in self.discrets
        if not dst:
            return (None if discret else f"💸 {qui} envoie {sol:.4f} SOL"), False
        if self.cible_de(dst):
            if sol >= GROS_INTERNE_SOL:
                return (f"⚠️ {qui} → {self._nom(dst)} : <b>{sol:.2f} SOL</b> entre ses wallets (gros mouvement : "
                        "souvent juste avant ou juste après un lancement)"), True
            if (discret or dst in self.discrets) and sol < DISCRET_SOL:
                return None, False            # vers ou depuis un relais / le bundle : silencieux (30 lignes le 19/09 au soir)
            return f"🔁 {qui} → {self._nom(dst)} : {sol:.4f} SOL (entre ses wallets)", sol >= IMPORTANT_SOL
        lab = self.db.get_label(dst)
        if lab or self.p.is_service_address(dst):
            if discret and sol < DISCRET_SOL:
                return None, False
            return (f"🏦 {qui} <b>envoie {sol:.4f} SOL</b> à <b>{esc(lab or 'un exchange / service')}</b> : l'argent "
                    "peut revenir par un wallet neuf sans lien visible"), sol >= IMPORTANT_SOL
        rafale = self._note_envoi(nom)
        if self.p.rpc is not None and (discret or rafale >= RAFALE_MIN):
            # Rafale (mélangeur : 100+ wallets en 1 min) ou wallet discret : rien ne bloque la lecture des
            # transactions, les wallets neufs sont vérifiés et suivis en tâche de fond, sans un message chacun
            self._lancer(self._suivre_si_neuf(nom, src, dst, sol))
            if discret or rafale > RAFALE_MIN:
                return None, False
            return (f"🌀 {qui} <b>envoie du SOL en rafale</b> à des wallets inconnus (mélangeur, bundle ou paiements) : "
                    "les wallets neufs sont suivis en silence ; armement et CA signalés ici"), True
        try:
            n = len(await self.p.rpc.signatures(dst, limit=NEUF_MAX_TX + 1)) if self.p.rpc is not None else NEUF_MAX_TX + 1
        except Exception:
            n = NEUF_MAX_TX + 1
        if n <= NEUF_MAX_TX:
            if await self.ajouter(dst, nom, f"wallet neuf financé par {self.p.label(src) or src[:6]} ({sol:g} SOL)",
                                  src, rattraper=True):
                self._note_neuf(nom)
            return (f"🆕 {qui} <b>finance un wallet NEUF</b> ({sol:.4f} SOL) : <code>{dst}</code>\n"
                    "   → ajouté à la cible (souvent le prochain wallet de lancement)"), True
        return (f"💸 {qui} envoie {sol:.4f} SOL à <code>{dst}</code> (wallet existant, {NEUF_MAX_TX}+ tx)",
                sol >= EXISTANT_FORT_SOL)

    async def _achat_groupe(self, nom: str, wallet: str, mint: str, sym: str) -> None:
        """Plusieurs wallets de la cible sur le même token JEUNE en 10 min : c'est son coin (son bundle)."""
        if mint in IGNORED_MINTS or (nom, mint) in self._annonces:
            return
        now = time.time()
        achats = self._achats[(nom, mint)]
        achats.setdefault(wallet, now)
        for w, t in list(achats.items()):
            if now - t > ACHAT_GROUPE_S:
                del achats[w]
        if len(achats) < 2:
            return
        _s, c = await self._symbole(mint)
        cree = c.get("created")
        if cree:
            if now - cree > COIN_JEUNE_S:
                return                         # vieux coin : ses wallets tradent, ce n'est pas un lancement
        elif not mint.endswith("pump"):
            return                             # âge inconnu et pas un token pump.fun : on ne conclut pas
        premier = min(achats.values())
        if cree and premier - cree <= LANCEMENT_S:
            comment = (f"{len(achats)} wallets de la cible l'achètent dans les {max(1, int(premier - cree))} s qui "
                       "suivent sa création : c'est son bundle au lancement")
        else:
            comment = (f"{len(achats)} wallets de la cible l'achètent en moins de {ACHAT_GROUPE_S // 60} min"
                       + (f" (token de {A.age(int(now - cree))})" if cree else "") + " : achat groupé au lancement")
        await self.nouveau_coin(nom, mint, c.get("creator") or "?", comment, sym, c.get("name"))

    # --- suivi -----------------------------------------------------------------------------------------
    def status_line(self) -> str | None:
        if not self.noms:
            return None
        par, disc = defaultdict(int), defaultdict(int)
        for a, n in self.membres.items():
            par[n] += 1
            disc[n] += a in self.discrets
        return "🎯 Cibles : " + " · ".join(f"{n} ({par[n]} wallets, dont {disc[n]} discrets)" for n in sorted(self.noms))

    async def purger(self) -> int:
        """Relais vidés (ajoutés automatiquement il y a plus de 2 h, suivis en discret, < 0,001 SOL) : retirés.
        Un mélangeur en ajoute des centaines par lancement ; s'ils resservent, ils reviennent par leur financeur."""
        if self.p.rpc is None:
            return 0
        limite = int(time.time()) - PURGE_AGE_S
        candidats = [r["address"] for r in self.db.conn.execute(
            "SELECT address FROM wallets WHERE active = 1 AND grp LIKE 'cible:%' AND role LIKE 'wallet neuf financé%' "
            "AND role LIKE ? AND added_at < ?", (f"%{DISCRET}%", limite))]
        vides = []
        for i in range(0, len(candidats), 100):
            lot = candidats[i:i + 100]
            try:
                res = await in_background(self.p.rpc.call(
                    "getMultipleAccounts", [lot, {"encoding": "base64", "dataSlice": {"offset": 0, "length": 0}}]))
            except Exception as e:
                log.debug("Purge des relais : %s", e)
                continue
            for a, compte in zip(lot, (res or {}).get("value") or []):
                if compte is None or (compte.get("lamports") or 0) / 1e9 < PURGE_SOLDE:
                    vides.append(a)
        for a in vides:
            nom = self.membres.pop(a, None)
            self.discrets.discard(a)
            self.db.conn.execute("UPDATE wallets SET grp = ? WHERE address = ?", (f"ex-cible:{nom}", a))
        self.db.conn.commit()
        if vides:
            await self.p.unwatch(vides)
            log.info("Cibles : %d relais vidés retirés de la surveillance", len(vides))
        return len(vides)

    async def boucle(self) -> None:
        """Nouvelles cibles et nouveaux wallets ajoutés en base (commande, autre module) pris en compte."""
        derniere_purge = time.time()
        while True:
            if time.time() - derniere_purge >= PURGE_S:
                derniere_purge = time.time()
                try:
                    await self.purger()
                except Exception:
                    log.exception("Cibles : purge des relais en échec")
            try:
                avant = set(self.membres)
                for nom in self.recharger():
                    await self.preparer(nom)
                for a in set(self.membres) - avant:
                    if a not in self.p.watched:
                        self.p.watched.add(a)
                        if self.p.watcher is not None:
                            await self.p.watcher.add(a)
            except Exception:
                log.exception("Cibles : rechargement en échec")
            await asyncio.sleep(RECHARGE_S)


def main() -> int:
    """python -m radar.cible ajouter <NOM> <dev> [--wallets a b] [--discrets c d] [--notes "..."] | retirer | liste"""
    import argparse

    from . import config as cfgmod
    from .db import DB

    ap = argparse.ArgumentParser(prog="python -m radar.cible")
    sous = ap.add_subparsers(dest="cmd", required=True)
    aj = sous.add_parser("ajouter", help="suivre un dev de près dans sa propre section")
    aj.add_argument("nom")
    aj.add_argument("dev")
    aj.add_argument("--wallets", nargs="*", default=[], help="wallets de ce dev ou de son opérateur, racontés")
    aj.add_argument("--discrets", nargs="*", default=[], help="wallets suivis en silence (bundle, bots)")
    aj.add_argument("--notes", default="")
    ret = sous.add_parser("retirer", help="ne plus suivre ces wallets")
    ret.add_argument("nom")
    ret.add_argument("adresses", nargs="+")
    sous.add_parser("liste")
    args = ap.parse_args()
    db = DB(cfgmod.load().db_path)
    if args.cmd == "liste":
        membres, discrets = db.cible_membres(), db.cible_discrets()
        for r in db.cibles():
            ws = [a for a, n in membres.items() if n == r["nom"]]
            print(f"{r['nom']} : dev {r['racine']} · {len(ws)} wallets dont {sum(a in discrets for a in ws)} discrets")
        db.close()
        return 0
    nom = args.nom.upper()
    if args.cmd == "retirer":
        for a in args.adresses:
            db.conn.execute("UPDATE wallets SET active = 0, grp = ? WHERE address = ? AND grp = ?",
                            (f"ex-cible:{nom}", a, PREFIXE + nom))
        db.conn.commit()
        print(f"Cible {nom} : {len(args.adresses)} wallets retirés (effectif au prochain redémarrage du radar).")
        db.close()
        return 0
    anciennes = next((r for r in db.cibles() if r["nom"] == nom), None)
    db.cible_add(nom, args.dev, args.notes or (anciennes["notes"] if anciennes else ""))
    lignes = [(args.dev, f"dev (cible {nom})", 1, None)] + \
             [(a, f"wallet de l'opérateur (cible {nom})", 2, args.dev) for a in args.wallets] + \
             [(a, f"bundle de l'opérateur (cible {nom}) · suivi {DISCRET}", 2, args.dev) for a in args.discrets]
    for adresse, role, depth, parent in lignes:
        label = f"DEV_{nom}_{adresse[:4]}" if depth == 1 else f"{nom}_W_{adresse[:4]}"
        if db.wallet(adresse) is not None:
            db.set_wallet_role(adresse, label, PREFIXE + nom, role, depth)
        db.add_wallet(adresse, label, PREFIXE + nom, role, depth, parent)
        db.conn.execute("UPDATE wallets SET active = 1 WHERE address = ?", (adresse,))
    db.conn.commit()
    print(f"Cible {nom} : {len(lignes)} wallets ({len(args.discrets)} discrets). Pris en compte en moins d'une minute.")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
