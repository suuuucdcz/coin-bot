"""Traitement d'une transaction : événements -> vérifications -> liaison au lancement -> alerte.

Utilisé par main.py (temps réel) et par `python -m radar.analysis.classify <signature>` (test).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import Counter, OrderedDict, defaultdict
from contextvars import ContextVar
from dataclasses import dataclass

import aiohttp

from . import alerts as A
from .analysis.classify import Event, analyze, programs
from .analysis.enrich import TokenInfo, missing_data, purge_cache, token_info
from .config import Config
from .db import DB
from .sources.helius import AMM_PROGRAMS, LogsWatcher, SolanaRPC, account_keys, in_background, sol_deltas
from .telegram import Telegram, esc

log = logging.getLogger("pipeline")

TOP_KINDS = ("create", "cluster", "lp_add", "match")   # alertes qui peuvent aller dans « ‼️ À ne pas rater »
YOUNG_TOKEN_S = 24 * 3600      # « mint jeune » = moins de 24 h
NEW_WALLET_MAX_TX = 5          # un wallet avec moins de 5 tx = nouveau wallet
SUPPLY_IN_MIN_PCT = 1.0        # réception de supply significative
RESERVE_MIN_PCT = 10.0         # wallet de réserve = détient au moins 10 %
DEV_BIG_BUY_PCT = 20.0         # achat initial du dev au-delà duquel on met un drapeau rouge
BURST_WINDOW_S = 120
BURST_MAX_ALERTS = 5           # au-delà, les fundings en rafale sont ajoutés sans alerte
TRADE_KINDS = {"buy", "sell", "supply_in"}
CLUSTER_WINDOW_S = 15 * 60     # achats d'un même groupe dans cette fenêtre = entrée du cluster
TRADE_MAX_PER_HOUR = 3         # alertes achat/vente max par wallet et par heure
TRADE_MUTE_S = 6 * 3600        # durée de la sourdine d'un wallet qui trade en boucle


RUG_MARK = "opérateur de rugs en série"
# Compartiment Telegram de chaque type d'alerte
KIND_TOPIC = {"create": "onchain", "buy": "onchain", "supply_in": "onchain", "lp_add": "onchain", "supply_out": "onchain",
              "sell": "onchain", "transfer": "clusters", "funding": "clusters", "cex": "clusters",
              "trace": "devs", "mute": "clusters", "cluster": "onchain", "discovery": "devs",
              "system": "system"}


@dataclass
class Alert:
    key: str
    kind: str
    text: str
    markup: dict | None = None
    replace: bool = False   # complète l'alerte rapide déjà envoyée sous la même clé
    # « 🎯 À ne pas rater » : titre et raison, seulement pour les signaux vérifiés et sans drapeau grave
    top_title: str = ""
    top_why: str = ""
    info: TokenInfo | None = None
    flags: list[str] | None = None

    @property
    def topic(self) -> str:
        # Tout ce qui touche un cluster de rugs connu part dans 🚩 Arnaques repérées
        return "scams" if RUG_MARK in self.text else KIND_TOPIC.get(self.kind, "onchain")


def is_dev_role(role: str | None) -> bool:
    """Wallet de dev (et pas « financé par DEV_X » ni « a financé le dev » : le mot seul ne suffit pas)."""
    r = (role or "").strip().lower()
    return r.startswith(("dev", "wallet principal du dev"))


def is_upstream_role(role: str | None) -> bool:
    """Wallet « en amont » d'un cluster (bank, seed, source, distributeur) : là où reviennent les profits."""
    r = (role or "").lower()
    return any(k in r for k in ("bank", "seed", "source", "distributeur", "financeur"))


FARM_WINDOW_S = 6 * 3600
FARM_MIN_TOKENS = 4          # un groupe qui « entre » dans 4 tokens différents en 6 h = ferme de bots
SNIPER_MIN_TOKENS = 5        # un wallet qui achète 5 tokens différents en 6 h = sniper
FUNDED_MAX_24H = 12          # au-delà, un financeur est un distributeur : ses nouveaux wallets ne sont plus ajoutés
FACTORY_FLAG_24H = 3         # 3 tokens créés en 24 h : signal grave (lanceur en série)
FACTORY_UNWATCH_24H = 5      # 5 tokens créés en 24 h : usine / plateforme de lancement, plus un dev à suivre
SUPPLY_OUT_MIN_PCT = 1.0     # déplacement de supply signalé au-delà de 1 % de la supply
TOP_RETRY_S = (60, 180)      # alerte « à ne pas rater » retentée quand seules des données manquaient
INDEPENDENT_GROUPS = {"découverte", "manuel"}   # wallets rassemblés par le radar, sans lien entre eux
TRUSTED = ("référence", "prouvé", "lié")


def wallet_trust(row, parent_trust: str | None = None, bad_groups: set[str] | frozenset = frozenset(),
                 parent_role: str | None = None) -> str:
    """Confiance dans un wallet suivi, selon COMMENT il a été trouvé.

    référence : watchlist de départ (hors cluster de rugs) · prouvé : dev d'un vrai succès (vérifié
    DexScreener) ou dev relié à un compte X par un lien dans les deux sens · lié : financé directement par
    un wallet de confiance, ou adresse publiée par le compte officiel · faible : tout le reste (satellites,
    acheteurs, détenteurs, créateurs ou financeurs de faux coins, chaînes de financement lointaines).
    Vu en vrai : 30 alertes « le cluster entre » déclenchées par les acheteurs d'un faux coin = une ferme de bots.
    """
    if row is None:
        return "faible"
    r = (row["role"] or "").lower()
    if row["grp"] in bad_groups or r.startswith("dev reclassé"):
        # Vu en vrai : un dev reclassé ⛔ gardait « référence » (niveau 0), et les wallets qu'il finance
        # devenaient « de confiance ». Un wallet d'un groupe à éviter n'est jamais de confiance.
        return "faible"
    if row["depth"] == 0 and row["grp"] != "découverte":
        return "référence"
    if r.startswith("dev (découverte"):
        return "prouvé"
    if r.startswith("dev probable") and "renvoie vers" in r:
        return "prouvé"   # CA publié par le compte officiel ET le token renvoie vers lui
    if r.startswith("dev probable") and "adresse publiée par" in r:
        return "lié"
    if r.startswith(("bank probable", "financé par")) and parent_trust in ("référence", "prouvé"):
        return "lié"
    if r.startswith("financé par") and parent_trust == "lié" and (parent_role or "").lower().startswith("bank probable"):
        # Nouveau wallet financé par le bank d'un dev à succès : c'est LE scénario suivi (le dev relance avec un
        # wallet neuf). Vu à la relecture : il retombait « faible », donc sa création n'allait jamais dans ‼️.
        return "lié"
    return "faible"


def watch_priority(role: str | None, depth: int) -> int:
    """0 = watchlist de départ, 1 = dev / wallet financé / bank / contrat, 2 = satellite, acheteur, détenteur."""
    r = (role or "").lower()
    if depth == 0:
        return 0
    if is_dev_role(role) or is_upstream_role(role) or r.startswith("financé par") or "contrat" in r:
        return 1
    return 2


def is_service(row, label: str | None) -> bool:
    txt = f"{row['role'] if row else ''} {label or ''}".lower()
    return any(k in txt for k in ("hot wallet", "cex", "exchange", "usine à tokens"))


# Raison notée pour l'événement en cours (chaque événement est traité dans sa propre tâche asyncio)
_SKIPPED: ContextVar[list | None] = ContextVar("skipped", default=None)


class Pipeline:
    def __init__(self, cfg: Config, db: DB, rpc: SolanaRPC, http: aiohttp.ClientSession,
                 tg: Telegram | None = None, watcher: LogsWatcher | None = None, dry_run: bool = False):
        self.cfg, self.db, self.rpc, self.http = cfg, db, rpc, http
        self.tg, self.watcher, self.dry_run = tg, watcher, dry_run
        self.watched: set[str] = set()
        self.mints: set[str] = set()   # adresses de la watchlist qui sont des contrats de token
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._burst: dict[str, list[float]] = defaultdict(list)
        self._tasks: set[asyncio.Task] = set()
        self.on_launch = None          # rappel de l'agenda quand un pool apparaît
        self.on_ann_update = None      # rappel de l'agenda pour mettre à jour une fiche coin
        self.on_create = None          # rappel de l'agenda : un wallet suivi a créé un token
        self.on_cluster_entry = None   # rappel de l'agenda : un cluster entre dans un token
        self._entries: dict[tuple[str, str], dict[str, float]] = {}
        self._inflight: set[str] = set()
        self._trades: dict[str, list[float]] = defaultdict(list)
        self._trade_muted: dict[str, float] = {}
        self._full_warned = 0.0
        self.decisions: Counter = Counter()      # pourquoi les événements n'ont pas donné d'alerte
        self.decisions_since = time.time()
        self.reload_watchlist()

    @property
    def labels(self) -> dict[str, str]:
        """Étiquettes (exchanges…), relues en base : le traçage en apprend de nouvelles en continu."""
        return self.db.labels()

    # --- watchlist -------------------------------------------------------------
    async def detect_mints(self) -> None:
        """Repère les contrats de token de la watchlist (ex. ASH_MINT, ou MINT_$TICKER ajouté par l'agenda).

        Sans ça, après un redémarrage, l'ouverture du trading d'un contrat annoncé ne serait plus détectée.
        """
        for a in list(self.watched):
            w = self.db.wallet(a)
            if not (w and (w["depth"] == 0 or (w["label"] or "").startswith("MINT_"))):
                continue
            if self.db.alert_already_sent(f"lp:{a}"):
                # Déjà lancé (vu en vrai : $ASH redevenait « en attente » à chaque redémarrage et chaque
                # échange sur le token arrivait ici)
                self.watched.discard(a)
                continue
            if await self.rpc.mint_info(a):
                self.mints.add(a)
        if self.mints:
            log.info("Contrats suivis en attente de lancement : %s", ", ".join(self.label(m) or m for m in self.mints))

    def reload_watchlist(self) -> None:
        self.watched = set()
        labels = self.labels
        for w in self.db.active_wallets():
            if is_service(w, labels.get(w["address"])):
                continue  # un exchange enverrait des centaines de « fundings » sans rapport
            self.watched.add(w["address"])

    def label(self, address: str) -> str | None:
        w = self.db.wallet(address)
        return (w["label"] if w else None) or self.db.get_label(address)

    def group(self, address: str) -> str | None:
        w = self.db.wallet(address)
        return w["grp"] if w else None

    def rug_flags(self, *addresses: str | None) -> list[str]:
        """Drapeau ⛔ si le wallet OU un de ses financeurs connus (3 niveaux) appartient à un groupe à éviter.
        Vu en vrai : un wallet financé par un dev reclassé ⛔ restait dans le groupe « découverte »."""
        for a in addresses:
            cur, vus = a, set()
            for _ in range(4):
                if not cur or cur in vus:
                    break
                vus.add(cur)
                w = self.db.wallet(cur)
                if not w:
                    break
                g = w["grp"]
                via = "" if cur == a else f" (via son financeur {w['label']})"
                if g and g in self.cfg.rug_groups:
                    return [f"cluster « {g} »{via} : {RUG_MARK} — à signaler, pas à acheter"]
                if g and self.is_farm(g):
                    return [f"groupe « {g} »{via} : ferme de bots qui achète tous les lancements ({RUG_MARK}) — à éviter"]
                cur = w["parent"]
        return []

    def bad_groups(self) -> set[str]:
        return set(self.cfg.rug_groups)

    def is_sniper(self, address: str) -> bool:
        return bool(self.db.get(f"sniper:{address}"))

    def creations_24h(self, wallet: str, mint: str | None = None) -> int:
        """Tokens distincts créés par ce wallet en 24 h (mémoire du radar + table des tokens)."""
        now = int(time.time())
        cle = f"creates:{wallet}"
        vus = {m: t for m, t in json.loads(self.db.get(cle) or "{}").items() if now - t < 24 * 3600}
        for r in self.db.conn.execute("SELECT mint, COALESCE(created_at, first_seen) AS t FROM tokens "
                                      "WHERE creator=? AND COALESCE(created_at, first_seen) > ?",
                                      (wallet, now - 24 * 3600)):
            vus.setdefault(r["mint"], r["t"])
        if mint:
            vus.setdefault(mint, now)
            self.db.put(cle, json.dumps(vus))
        return len(vus)

    async def _factory_check(self, ev: Event) -> tuple[bool, list[str]]:
        """Usine à tokens (vu en vrai : le wallet de la plateforme startup.fun créait un token de chat toutes les
        10 à 20 min : 3 alertes inutiles à chaque fois). Renvoie (à retirer, drapeaux)."""
        n = self.creations_24h(ev.wallet, ev.mint)
        flags = ([f"lanceur en série : {n} tokens créés en 24 h (usine à tokens)"]
                 if n >= FACTORY_FLAG_24H else [])
        w = self.db.wallet(ev.wallet)
        if n < FACTORY_UNWATCH_24H or self.dry_run or not w or w["depth"] == 0:
            return False, flags  # la watchlist de départ n'est jamais retirée automatiquement
        nom = w["label"] or A.short(ev.wallet)
        self.db.put(f"factory:{ev.wallet}", int(time.time()))
        self.db.set_label(ev.wallet, f"usine à tokens ({n} créations en 24 h)")
        await self.unwatch([ev.wallet])
        log.warning("%s reclassé usine à tokens (%d créations en 24 h) : retiré de la surveillance", nom, n)
        self.emit(Alert(f"factory:{ev.wallet}", "system",
                        f"🏭 <b>{esc(nom)} reclassé : usine à tokens</b>\n"
                        f"{n} tokens créés en 24 h : c'est une plateforme de lancement ou un lanceur en série, "
                        f"pas un dev qui prépare UN projet. Retiré de la surveillance.\n<code>{ev.wallet}</code>"))
        return True, flags

    async def _sniper_check(self, ev: Event) -> bool:
        """Un wallet suivi qui achète 5 tokens différents en 6 h n'est pas un satellite : c'est un sniper
        (vu en vrai : 2 « acheteurs précoces » achetaient un nouveau token toutes les 10 minutes)."""
        if self.is_sniper(ev.wallet):
            return True
        w = self.db.wallet(ev.wallet)
        if not w or w["depth"] == 0:
            return False  # la watchlist de départ n'est jamais reclassée automatiquement
        now = int(time.time())
        cle = f"buys:{ev.wallet}"
        vus = {m: t for m, t in json.loads(self.db.get(cle) or "{}").items() if now - t < FARM_WINDOW_S}
        vus.setdefault(ev.mint or "", now)
        self.db.put(cle, json.dumps(vus))
        if len(vus) >= SNIPER_MIN_TOKENS:
            self.db.put(f"sniper:{ev.wallet}", now)
            await self.unwatch([ev.wallet])
            log.warning("%s reclassé sniper (%d tokens achetés en 6 h) : retiré de la surveillance", w["label"], len(vus))
            return True
        return False

    def is_farm(self, grp: str) -> bool:
        return bool(grp and self.db.get(f"farm:{grp}"))

    def trust(self, address: str | None) -> str:
        """Niveau de confiance d'un wallet (voir wallet_trust), en remontant son financeur si besoin."""
        row = self.db.wallet(address) if address else None
        if row is None:
            return "faible"
        bad = self.bad_groups()
        parent = self.db.wallet(row["parent"]) if row["parent"] else None
        grand = self.db.wallet(parent["parent"]) if parent is not None and parent["parent"] else None
        grand_trust = wallet_trust(grand, None, bad) if grand is not None else None
        parent_trust = wallet_trust(parent, grand_trust, bad) if parent is not None else None
        return wallet_trust(row, parent_trust, bad, parent["role"] if parent is not None else None)

    def _farm_check(self, grp: str, mint: str) -> bool:
        """Mémorise les tokens où le groupe « entre ». Trop de tokens différents = ferme de bots."""
        now = int(time.time())
        cle = f"grp_tokens:{grp}"
        vus = {m: t for m, t in json.loads(self.db.get(cle) or "{}").items() if now - t < FARM_WINDOW_S}
        vus.setdefault(mint, now)
        self.db.put(cle, json.dumps(vus))
        if len(vus) >= FARM_MIN_TOKENS and not self.is_farm(grp):
            self.db.put(f"farm:{grp}", now)
            log.warning("Groupe %s reclassé « ferme de bots » (%d tokens en 6 h)", grp, len(vus))
            self.emit(Alert(f"farm:{grp}", "system",
                            f"🤖 <b>Groupe « {esc(grp)} » reclassé : ferme de bots</b>\n"
                            f"Ses wallets sont entrés dans {len(vus)} tokens différents en 6 h : ils achètent tout "
                            "pour simuler de l'engouement, puis revendent. Ses entrées ne sont plus des signaux ; "
                            "ses prochains tokens seront marqués ⛔."))
        return self.is_farm(grp)

    async def watch(self, address: str, label: str, grp: str | None, role: str, depth: int, parent: str | None) -> bool:
        if self.dry_run or depth > self.cfg.trace_max_hops:
            return False
        if is_service(None, self.db.get_label(address)):
            return False  # un exchange enverrait des centaines de « fundings » sans rapport
        if address not in self.watched and len(self.watched) >= self.cfg.watch_max:
            # Plafond atteint : un wallet important (dev, wallet financé par un bank) prend la place du
            # satellite le moins actif. Sans ça, les satellites des annonces X rempliraient la watchlist
            # et le radar refuserait justement les nouveaux wallets de dev.
            if watch_priority(role, depth) <= 1:
                for w in self.db.least_active():
                    if watch_priority(w["role"], w["depth"]) == 2 and w["grp"] not in self.cfg.rug_groups:
                        await self.unwatch([w["address"]])
                        log.info("Watchlist pleine : %s mis en veille pour faire place à %s", w["label"], label)
                        break
            if len(self.watched) < self.cfg.watch_max:
                added = self.db.add_wallet(address, label, grp or "", role, depth, parent)
                if added:
                    self.watched.add(address)
                    if self.watcher:
                        await self.watcher.add(address)
                return added
            if time.time() - self._full_warned > 86400:
                self._full_warned = time.time()
                self.emit(Alert(f"full:{int(time.time()) // 86400}", "system",
                                f"⚠️ Watchlist pleine ({self.cfg.watch_max} adresses) : les nouveaux wallets ne sont "
                                "plus ajoutés. Augmente WATCH_MAX dans .env ou baisse WATCH_STALE_DAYS."))
            log.warning("Watchlist pleine : %s non ajouté", address)
            return False
        added = self.db.add_wallet(address, label, grp or "", role, depth, parent)
        if added:
            self.watched.add(address)
            if self.watcher:
                await self.watcher.add(address)
        return added

    async def purge_services(self) -> int:
        """Retire les wallets reconnus comme services (échangeur, launchpad…) et les clients qu'ils ont financés."""
        cibles = []
        for a in list(self.watched):
            w = self.db.wallet(a)
            if is_service(w, self.db.get_label(a)):
                cibles.append(a)
            elif w and w["parent"] and is_service(self.db.wallet(w["parent"]), self.db.get_label(w["parent"])) \
                    and w["depth"] > 0:
                cibles.append(a)
        if cibles:
            await self.unwatch(cibles)
            log.info("%d wallet(s) de services (ou financés par un service) retirés de la surveillance", len(cibles))
        return len(cibles)

    def decisions_line(self) -> str:
        """« 120 événements · 3 alertes · écartés : 80 achats de satellites, 20 tokens anciens… »"""
        evts = sum(n for k, n in self.decisions.items() if k.startswith("événement "))
        alertes = sum(n for k, n in self.decisions.items() if k.startswith("alerte "))
        ecartes = [(k, n) for k, n in self.decisions.items() if not k.startswith(("événement ", "alerte "))]
        top = ", ".join(f"{n} {k}" for k, n in sorted(ecartes, key=lambda x: -x[1])[:4])
        return f"{evts} événements · {alertes} alertes" + (f" · écartés : {top}" if top else "")

    def cleanup(self) -> None:
        """Libère la mémoire des fenêtres glissantes (appelé chaque heure par main.py)."""
        log.info("Décisions de la dernière heure : %s", self.decisions_line())
        self.decisions, self.decisions_since = Counter(), time.time()
        now = time.time()
        self._entries = {k: v for k, v in self._entries.items() if any(now - t < CLUSTER_WINDOW_S for t in v.values())}
        self._burst = defaultdict(list, {k: v for k, v in self._burst.items() if v and now - v[-1] < BURST_WINDOW_S})
        self._trades = defaultdict(list, {k: v for k, v in self._trades.items() if v and now - v[-1] < 3600})
        self._trade_muted = {k: t for k, t in self._trade_muted.items() if t > now}
        purge_cache()

    async def unwatch(self, addresses: list[str]) -> None:
        """Met des wallets en veille (purge des wallets inactifs)."""
        self.db.deactivate(addresses)
        for a in addresses:
            self.watched.discard(a)
            if self.watcher:
                await self.watcher.remove(a)

    # --- dédoublonnage -----------------------------------------------------------
    def seen_signature(self, sig: str) -> bool:
        if sig in self._seen:
            return True
        self._seen[sig] = None
        if len(self._seen) > 5000:
            self._seen.popitem(last=False)
        return False

    def already(self, key: str) -> bool:
        return not self.dry_run and self.db.alert_already_sent(key)

    # --- traitement -------------------------------------------------------------
    async def handle_signature(self, sig: str) -> list[Alert]:
        tx = await self.rpc.transaction_retry(sig)
        if not tx:
            log.warning("Transaction introuvable : %s", sig)
            return []
        events = analyze(tx, self.watched - self.mints)
        # Contrat suivi (CA publié avant le lancement, cas $ASH) : la 1re tx sur un AMM qui le
        # touche = pool créé / trading ouvert, même si le wallet qui le fait n'est pas suivi.
        keys = account_keys(tx)
        dex = next((AMM_PROGRAMS[p] for p in programs(tx) if p in AMM_PROGRAMS), None)
        for m in self.mints & set(keys):
            if dex and not any(e.kind == "lp_add" and e.mint == m for e in events):
                payer = keys[0]
                events.append(Event("lp_add", payer, sig, tx.get("blockTime") or 0, m,
                                    sol=max(0.0, -sol_deltas(tx).get(payer, 0.0)), dex=dex))
        out: list[Alert] = []
        for ev in events:
            try:
                a = await self.process(ev)
            except Exception:
                log.exception("Erreur sur l'événement %s (%s)", ev.kind, sig)
                continue
            if a:
                out.append(a)
        return out

    async def _cluster_track(self, ev: Event) -> None:
        """🎯 Le dev (probable) ou ≥ 2 wallets d'un même groupe entrent dans le même token jeune.

        C'est LE signal recherché : le cluster achète quelques minutes avant l'annonce publique.
        Passe avant l'anti-rafale pour ne jamais être masqué.
        """
        w = self.db.wallet(ev.wallet)
        grp = w["grp"] if w else None
        if not grp or not ev.mint:
            return
        if self.is_farm(grp):
            return  # ferme de bots : elle achète tout, ce n'est pas un signal
        is_dev = is_dev_role(w["role"])
        now = time.time()
        key = (grp, ev.mint)
        entries = {a: t for a, t in self._entries.get(key, {}).items() if now - t < CLUSTER_WINDOW_S}
        entries[ev.wallet] = now
        self._entries[key] = entries
        if len(entries) < 2 and not is_dev:
            return
        if grp in INDEPENDENT_GROUPS and len(entries) >= 2 and not is_dev:
            return  # « découverte », « manuel » : des devs sans lien entre eux, pas un cluster
        # Ferme de bots : seulement de VRAIS achats groupés (≥ 2 wallets du groupe), jamais un groupe de devs
        # indépendants (vu en vrai : tout le groupe « découverte » avait été classé ferme, donc ⛔)
        if len(entries) >= 2 and grp not in INDEPENDENT_GROUPS and self._farm_check(grp, ev.mint):
            return
        akey = f"cluster:{grp}:{ev.mint}:{len(entries) >= 3}"
        if self.already(akey) or akey in self._inflight:
            return
        self._inflight.add(akey)
        try:
            await self._cluster_alert(ev, grp, is_dev, entries, akey)
        finally:
            self._inflight.discard(akey)

    def _known_old(self, mint: str | None) -> bool:
        """Token déjà connu comme vieux (plus de 24 h) : un token vieux le reste, inutile de le réanalyser
        (vu en vrai : un wallet rachetait le même vieux token toutes les 9 min, analyse complète à chaque fois)."""
        tok = self.db.token(mint) if mint else None
        return bool(tok and tok["created_at"] and time.time() - tok["created_at"] > YOUNG_TOKEN_S)

    async def _cluster_alert(self, ev: Event, grp: str, is_dev: bool, entries: dict[str, float], akey: str) -> None:
        if self._known_old(ev.mint):
            return
        info = await self._info(ev.mint, dev=True)
        if info.age_s is not None and info.age_s > YOUNG_TOKEN_S:
            if info.created_ts:
                self.db.upsert_token(ev.mint, info.symbol, info.name, info.creator, info.created_ts)
            return  # achat d'un vieux token : pas un lancement
        if info.crowded:
            return  # déjà lancé et callé (beaucoup de monde dessus) : trop tard, inutile
        qui = "LE DEV ENTRE DANS UN TOKEN" if is_dev and len(entries) == 1 else f"LE CLUSTER ENTRE ({len(entries)} wallets)"
        membres = [f"🎯 Groupe <b>{esc(grp)}</b> · {len(entries)} achat{'s' if len(entries) > 1 else ''} "
                   f"en {CLUSTER_WINDOW_S // 60} min"]
        for a in entries:
            r = self.db.wallet(a)
            membres.append(f"• <b>{esc(self.label(a) or A.short(a))}</b>"
                           + (f" <i>{esc(r['role'])}</i>" if r and r["role"] else "") + f"\n  <code>{a}</code>")
        text = A.card(f"🎯 <b>{qui}</b>", info, self.rug_flags(ev.wallet, info.creator), membres,
                      A.token_block(info, []) + self._announcement_line(info))
        flags = self.rug_flags(ev.wallet, info.creator)
        # « À ne pas rater » seulement si le dev lui-même (ou un wallet prouvé) entre : des satellites
        # ou des acheteurs qui achètent ensemble, c'est exactement ce que fait une ferme de bots.
        confiance = any(self.trust(a) in ("référence", "prouvé") for a in entries)
        alert = Alert(akey, "cluster", text, A.token_buttons(info, ev.wallet, mute=ev.wallet),
                      top_title=qui if confiance else "", top_why=(self._why(ev.wallet, "achète ce token tout jeune") if len(entries) == 1
                                              else f"{len(entries)} wallets du groupe <b>{esc(grp)}</b> achètent "
                                                   f"ce token tout jeune en moins de {CLUSTER_WINDOW_S // 60} min"),
                      info=info, flags=flags)
        self.emit(alert)
        if self.on_cluster_entry:
            await self.on_cluster_entry(grp, ev.mint, list(entries), info, alert)

    def muted(self, wallet: str) -> bool:
        """Coupé à la main (bouton 🔇 Couper 24 h) ?"""
        until = self.db.get(f"mute:{wallet}")
        return bool(until and float(until) > time.time())

    async def process(self, ev: Event) -> Alert | None:
        now = time.time()
        self.decisions[f"événement {ev.kind}"] += 1
        if ev.kind != "create" and self.muted(ev.wallet):
            return self._skip("wallet coupé à la main")  # les créations de token restent toujours signalées
        if ev.kind == "buy" and not self.dry_run and await self._sniper_check(ev):
            return self._skip("achat d'un sniper")  # sniper : il achète tout, ce n'est ni un signal ni un membre du cluster
        if ev.kind in ("buy", "supply_in") and not self.dry_run:
            self._spawn(self._cluster_track(ev), urgent=True)
        if ev.kind in ("buy", "supply_in"):
            # La sourdine ne touche que les achats : une vente du dev ou d'une réserve passe toujours.
            if self._trade_muted.get(ev.wallet, 0) > now:
                return None
            recent = [t for t in self._trades[ev.wallet] if now - t < 3600]
            self._trades[ev.wallet] = recent
            if len(recent) >= TRADE_MAX_PER_HOUR:
                # Vu en pratique : un gros détenteur (bot) qui achète/vend en boucle noie tout le reste
                self._trade_muted[ev.wallet] = now + TRADE_MUTE_S
                return Alert(f"trademute:{ev.wallet}:{int(now) // TRADE_MUTE_S}", "mute",
                             f"🔇 <b>{esc(self.label(ev.wallet) or A.short(ev.wallet))}</b> trade en boucle\n"
                             f"Plus de {TRADE_MAX_PER_HOUR} achats/ventes en 1 h : ses achats/ventes sont coupés "
                             f"{TRADE_MUTE_S // 3600} h.\n<i>Créations de token et fundings restent signalés.</i>",
                             A.wallet_buttons(ev.wallet, trace=ev.wallet))
        handler = getattr(self, f"_on_{ev.kind}")
        note = _SKIPPED.set([])
        try:
            alert = await handler(ev)
            if alert is None and not _SKIPPED.get():
                # Aucune raison notée par le handler : on le dit quand même (journal complet, rien de silencieux)
                self._skip(f"{ev.kind} sans signal")
        finally:
            _SKIPPED.reset(note)
        if alert and ev.kind in TRADE_KINDS:
            self._trades[ev.wallet].append(now)
        return alert

    async def _info(self, mint: str, creator_hint: str | None = None, dev: bool = True) -> TokenInfo:
        info = await token_info(self.rpc, self.http, mint, creator_hint, dev)
        if dev and info.creator and info.network is None:
            # Réseau du dev : ses wallets et leurs projets passés. Un nouveau wallet financé par un opérateur
            # qui vide tous ses tokens en moins d'une minute est signalé (et bloqué pour « à ne pas rater »).
            from .analysis import network
            try:
                network.annotate(info, await asyncio.wait_for(network.quick(self, info.creator), 12))
            except Exception as e:
                log.debug("Réseau du dev %s : %s", info.creator[:6], e)
        return info

    def _why(self, wallet: str, action: str) -> str:
        """« DEV_XBC (découverte : $XBC a fait 11 M$) l'a créé » : pourquoi ce wallet compte."""
        w = self.db.wallet(wallet)
        nom = esc(self.label(wallet) or A.short(wallet))
        role = f" <i>({esc(w['role'])})</i>" if w and w["role"] else ""
        grp = f" · groupe {esc(w['grp'])}" if w and w["grp"] else ""
        return f"<b>{nom}</b>{role}{grp} {action}"

    def _head(self, ev: Event) -> str:
        line = A.wallet_line(ev.wallet, self.label(ev.wallet), self.group(ev.wallet))
        retard = int(time.time()) - ev.ts if ev.ts else 0
        if retard > 120:
            # Rattrapage après une coupure : l'événement n'est pas « en direct », il faut le savoir
            line += f"\n⏳ <b>Vu avec {A.age(retard)} de retard</b> (rattrapage après coupure)"
        return line

    def _amount(self, info: TokenInfo, ev: Event) -> str:
        pct = info.pct_supply(ev.tokens_raw)
        return f"{pct:.2f} % supply" if pct is not None else "part de supply inconnue"

    def _quick(self, key: str, kind: str, title: str, ev: Event, flags: list[str]) -> bool:
        """Alerte immédiate, avant l'analyse (qui prend quelques secondes) ; complétée ensuite.

        Renvoie False si l'alerte a déjà été envoyée.
        """
        tok = self.db.token(ev.mint)
        info = TokenInfo(ev.mint, name=ev.extra.get("name") or (tok["name"] if tok else None),
                         symbol=ev.extra.get("symbol") or (tok["symbol"] if tok else None))
        lines = [f"{title}</b>", A.token_title(info)]
        if flags:
            lines.append(A.verdict(flags))
        lines += [A.SEP, self._head(ev)]
        if self.label(ev.mint):
            lines.append(f"📌 Contrat suivi : <b>{esc(self.label(ev.mint))}</b>")
        lines += [A.SEP, f"📜 <code>{ev.mint}</code>", "<i>⏳ Analyse en cours : dev, X, market cap…</i>"]
        lines += A.flags_block(flags)
        return self.emit(Alert(key, kind, "\n".join(lines), A.token_buttons(info, ev.wallet)))

    # 🔴 création
    async def _on_create(self, ev: Event) -> Alert | None:
        key = f"create:{ev.mint}"
        if self.already(key) or key in self._inflight:
            return None  # déjà signalé (Helius et PumpPortal peuvent voir la même création)
        self._inflight.add(key)
        try:
            retire, usine = await self._factory_check(ev)
            if retire:
                return self._skip("création d'une usine à tokens")
            if not self._quick(key, "create", "🔴 <b>DEV CRÉE UN TOKEN", ev, self.rug_flags(ev.wallet) + usine):
                return None
            return await self._create_alert(ev, key, usine)
        finally:
            self._inflight.discard(key)

    async def _create_alert(self, ev: Event, key: str, usine: list[str] | None = None) -> Alert:
        info = await self._info(ev.mint, ev.wallet)
        info.creator = info.creator or ev.wallet
        info.created_ts = info.created_ts or ev.ts
        self.db.upsert_token(ev.mint, info.symbol, info.name, ev.wallet, ev.ts)
        head = [self._head(ev)]
        flags = self.rug_flags(ev.wallet)
        if usine and not any("créés en 24 h" in f for f in info.flags):
            flags += usine
        if ev.tokens_raw:
            head.append(f"💰 Achat initial : <b>{ev.sol:.2f} SOL</b> → {self._amount(info, ev)}")
            pct = info.pct_supply(ev.tokens_raw)
            if pct and pct >= DEV_BIG_BUY_PCT:
                # Vu en pratique : VSOF, le dev s'achète 79 % de la supply à la création, puis rug.
                flags.append(f"le dev s'achète {pct:.0f} % de la supply dès la création (risque de dump)")
        text = A.card("🔴 <b>DEV CRÉE UN TOKEN</b>", info, flags, head,
                      A.token_block(info, []) + self._announcement_line(info))
        if not self.dry_run:
            self._spawn(self._auto_trace(ev.wallet, info))
            if self.on_create:
                self._spawn(self.on_create(ev.wallet, ev.mint))
        confiance = self.trust(ev.wallet) in TRUSTED
        return Alert(key, "create", text, A.token_buttons(info, ev.wallet), replace=True,
                     top_title="UN DEV SUIVI CRÉE UN TOKEN" if confiance else "",
                     top_why=self._why(ev.wallet, "l'a créé"), info=info, flags=flags)

    def _announcement_line(self, info: TokenInfo) -> list[str]:
        """Liaison avec l'agenda : ce token a-t-il été annoncé sur X ?"""
        ann = self.db.find_announcement((info.symbol or "").upper() or None, info.mint, int(time.time()) - 36 * 3600)
        if not ann:
            return []
        lien = f"<a href=\"{esc(ann['tweet_url'])}\">@{esc(ann['handle'])}</a>"
        if ann["ca"] != info.mint:
            # Même ticker qu'une annonce, mais ce n'est pas le contrat relié à l'annonce : souvent une copie
            etat = "CA de l'annonce pas encore connu" if not ann["ca"] else "l'annonce est reliée à un AUTRE contrat"
            return [f"⚠️ Même ticker qu'une annonce de {lien}, mais {etat} : possible copie"]
        quand = ""
        if ann["launch_ts"]:
            from .agenda import paris
            quand = f" · lancement prévu {paris(ann['launch_ts'])} (Paris)"
        return [f"📣 Annoncé sur X par {lien}{quand}"]

    # 🟠 achat
    async def _on_buy(self, ev: Event) -> Alert | None:
        w = self.db.wallet(ev.wallet)
        if not self.dry_run and self.trust(ev.wallet) not in TRUSTED and not (
                w and (is_dev_role(w["role"]) or is_upstream_role(w["role"]))):
            # Satellite, acheteur, détenteur : son achat compte pour « le cluster entre », mais une alerte
            # à chaque achat, c'est du bruit (vu en vrai : un sniper « satellite » toutes les 10 minutes).
            return self._skip("achat d'un satellite (sert au suivi du cluster)")
        key = f"buy:{ev.wallet}:{ev.mint}"
        if self.already(key):
            return None
        if self._known_old(ev.mint):
            return self._skip("achat d'un token ancien")  # déjà vu vieux : aucun appel réseau
        info = await self._info(ev.mint)
        if info.age_s is None or info.age_s > YOUNG_TOKEN_S:
            log.info("Achat ignoré (token ancien ou âge inconnu) : %s", ev.mint)
            if info.created_ts:
                self.db.upsert_token(ev.mint, info.symbol, info.name, info.creator, info.created_ts)
            return self._skip("achat d'un token ancien")
        if info.crowded:
            log.info("Achat ignoré (déjà lancé et callé, %s tx) : %s", info.tx_count, ev.mint)
            return self._skip("achat d'un token déjà callé")
        self.db.upsert_token(ev.mint, info.symbol, info.name, info.creator, info.created_ts)
        is_creator = info.creator == ev.wallet
        titre = "LE DEV ACHÈTE SON TOKEN" if is_creator else "UN WALLET SUIVI ACHÈTE"
        head = [self._head(ev), f"💰 <b>{ev.sol:.2f} SOL</b> → {self._amount(info, ev)}"]
        text = A.card(f"🟠 <b>{titre}</b>", info, self.rug_flags(ev.wallet, info.creator), head,
                      A.token_block(info, []) + self._announcement_line(info))
        return Alert(key, "buy", text, A.token_buttons(info, ev.wallet, mute=ev.wallet))

    # 🟣 supply reçue gratuitement (préparation de lancement)
    async def _on_supply_in(self, ev: Event) -> Alert | None:
        key = f"supply:{ev.wallet}:{ev.mint}"
        if self.already(key):
            return None
        info = await self._info(ev.mint)
        pct = info.pct_supply(ev.tokens_raw)
        if pct is None or pct < SUPPLY_IN_MIN_PCT or info.age_s is None or info.age_s > YOUNG_TOKEN_S or info.crowded:
            return None
        self.db.upsert_token(ev.mint, info.symbol, info.name, info.creator, info.created_ts)
        head = [self._head(ev), f"📦 Reçoit <b>{pct:.1f} %</b> de la supply sans payer : <b>préparation de lancement</b>"]
        text = A.card("🟣 <b>SUPPLY REÇUE</b>", info, self.rug_flags(ev.wallet, info.creator), head,
                      A.token_block(info, []) + self._announcement_line(info))
        return Alert(key, "supply_in", text, A.token_buttons(info, ev.wallet, mute=ev.wallet))

    # 📤 déplacement de supply (le dev ou une réserve envoie ses tokens ailleurs)
    async def _on_supply_out(self, ev: Event) -> Alert | None:
        key = f"supout:{ev.wallet}:{ev.mint}:{ev.other}"
        if self.already(key):
            return None
        w = self.db.wallet(ev.wallet)
        info = await self._info(ev.mint, dev=False)
        pct = info.pct_supply(ev.tokens_raw)
        pre_pct = info.pct_supply(ev.pre_tokens_raw) or 0.0
        tok = self.db.token(ev.mint)
        createur = ev.wallet in (info.creator, tok["creator"] if tok else None)
        important = createur or pre_pct >= RESERVE_MIN_PCT or (w is not None and (
            is_dev_role(w["role"]) or self.trust(ev.wallet) in TRUSTED))
        if pct is None or pct < SUPPLY_OUT_MIN_PCT or not important:
            return self._skip("déplacement de supply trop petit ou wallet secondaire")
        dest = ev.other or ""
        lab_dest = self.db.get_label(dest)
        dw = self.db.wallet(dest)
        if lab_dest:
            vers = f"vers <b>{esc(lab_dest)}</b> : <b>vente probable</b>"
        elif dw:
            vers = f"vers <b>{esc(dw['label'])}</b> (déjà suivi)"
        else:
            nb = len(await self.rpc.signatures(dest, limit=NEW_WALLET_MAX_TX + 1))
            neuf = nb < NEW_WALLET_MAX_TX
            vers = "vers un <b>wallet neuf</b> (maintenant surveillé)" if neuf else "vers un autre wallet"
            if neuf:
                await self.watch(dest, f"RECOIT_{dest[:4]}", w["grp"] if w else None,
                                 f"reçoit {pct:.1f} % de la supply de {self.label(ev.wallet) or A.short(ev.wallet)}",
                                 (w["depth"] if w else 0) + 1, ev.wallet)
        titre = "LE DEV DÉPLACE SA SUPPLY" if createur else "UN WALLET SUIVI DÉPLACE DE LA SUPPLY"
        head = [self._head(ev), f"📤 <b>{pct:.1f} %</b> de la supply (détenait {pre_pct:.1f} %) {vers}",
                f"<code>{dest}</code>",
                "💡 <i>Souvent juste avant une vente : directement sur un exchange, ou via d'autres wallets.</i>"]
        text = A.card(f"📤 <b>{titre}</b>", info, self.rug_flags(ev.wallet, info.creator), head, A.token_block(info, []))
        return Alert(key, "supply_out", text, A.token_buttons(info, ev.wallet, mute=ev.wallet))

    # 🟢 ajout de liquidité = ouverture du trading
    async def _on_lp_add(self, ev: Event) -> Alert | None:
        key = f"lp:{ev.mint}"
        if self.already(key) or key in self._inflight:
            return None
        self._inflight.add(key)
        try:
            if not self._quick(key, "lp_add", "🟢 <b>LIQUIDITÉ AJOUTÉE : TRADING OUVERT", ev,
                               self.rug_flags(ev.wallet, ev.mint)):
                return None
            return await self._lp_alert(ev, key)
        finally:
            self._inflight.discard(key)

    async def _lp_alert(self, ev: Event, key: str) -> Alert:
        info = await self._info(ev.mint)
        pct = info.pct_supply(ev.tokens_raw) if ev.tokens_raw else None
        detail = (f"💧 Dépôt : <b>{ev.sol:.2f} SOL</b> + {pct:.1f} % de la supply sur {esc(ev.dex or '?')}"
                  if pct is not None else f"💧 Première transaction sur {esc(ev.dex or '?')} pour ce contrat suivi")
        text = A.card("🟢 <b>LIQUIDITÉ AJOUTÉE : TRADING OUVERT</b>", info,
                      self.rug_flags(ev.wallet, ev.mint, info.creator), [self._head(ev), detail],
                      A.token_block(info, []) + self._announcement_line(info))
        if self.on_launch and not self.dry_run:
            self._spawn(self.on_launch(ev.mint))
        if ev.mint in self.mints and not self.dry_run:
            # Mission accomplie pour ce contrat : on arrête de l'écouter (sinon chaque trade arrive ici)
            self.mints.discard(ev.mint)
            self.watched.discard(ev.mint)
            if self.watcher:
                self._spawn(self.watcher.remove(ev.mint))
        suivi = self.label(ev.mint)
        return Alert(key, "lp_add", text, A.token_buttons(info, ev.wallet), replace=True,
                     top_title="TRADING OUVERT" if suivi else "",
                     top_why=f"Contrat annoncé à l'avance ({esc(suivi)}) : la liquidité vient d'être ajoutée" if suivi else "",
                     info=info, flags=self.rug_flags(ev.wallet, ev.mint, info.creator))

    # ⚠️ vente d'un wallet de réserve / du dev
    async def _on_sell(self, ev: Event) -> Alert | None:
        key = f"sell:{ev.wallet}:{ev.mint}"
        if self.already(key):
            return None
        tok = self.db.token(ev.mint)
        if tok is None and not self.label(ev.mint) and not self.db.find_announcement(None, ev.mint, 0):
            # Seules les ventes d'un token SUIVI comptent (créé / acheté jeune / annoncé) : sinon chaque revente
            # d'un bot ou d'un satellite coûtait une analyse complète du token (crédits Helius) pour rien.
            return self._skip("vente d'un token non suivi")
        info = await self._info(ev.mint, dev=False)
        pre_pct = info.pct_supply(ev.pre_tokens_raw) or 0.0
        is_creator = ev.wallet in (info.creator, tok["creator"] if tok else None)
        if pre_pct < RESERVE_MIN_PCT and not is_creator:
            return self._skip("petite vente (moins de 10 % de la supply)")
        titre = "LE DEV VEND" if is_creator else "UN WALLET DE RÉSERVE VEND"
        head = [self._head(ev), f"📉 Vend {self._amount(info, ev)} (détenait {pre_pct:.1f} %) "
                                f"contre <b>{ev.sol:.2f} SOL</b>"]
        text = A.card(f"⚠️ <b>{titre}</b>", info, [], head, A.token_block(info, []))
        return Alert(key, "sell", text, A.token_buttons(info, ev.wallet, mute=ev.wallet))

    # 🟡 funding / ⚫ profits / 🔁 interne
    async def _on_transfer(self, ev: Event) -> Alert | None:
        dst, src = ev.other, ev.wallet
        src_row = self.db.wallet(src)
        dst_row = self.db.wallet(dst)
        if dst_row and not dst_row["active"] and ev.sol >= self.cfg.funding_min_sol:
            # Wallet mis en veille (inactif) qui reçoit de nouveau des fonds : les devs réutilisent
            # souvent un ancien wallet. On le réveille et on prévient.
            if self.is_sniper(dst) or self.db.get(f"factory:{dst}") or is_service(dst_row, self.db.get_label(dst)):
                return self._skip("wallet écarté (sniper, usine, service) refinancé")  # retiré exprès : il reste retiré
            key = f"wake:{ev.signature}:{dst}"
            if self.already(key):
                return None
            if not await self.watch(dst, dst_row["label"], dst_row["grp"], dst_row["role"], dst_row["depth"],
                                    dst_row["parent"]):
                return self._skip("wallet en veille refinancé, non réactivé (watchlist pleine ou profondeur max)")
            txt = (f"🟡 <b>WALLET EN VEILLE REFINANCÉ</b> · <b>{ev.sol:g} SOL</b>\n"
                   f"{esc(self.label(src) or A.short(src))} ➜ <b>{esc(dst_row['label'] or A.short(dst))}</b>\n"
                   f"<code>{dst}</code>\n<i>Il était inactif : il est de nouveau surveillé (prochain lancement ?).</i>")
            return Alert(key, "funding", txt, A.wallet_buttons(src, dst, sig=ev.signature, trace=dst))
        if dst in self.watched or dst_row:
            if ev.sol < self.cfg.profit_min_sol:
                return None
            is_dev = (src_row is not None and is_dev_role(src_row["role"])) or bool(self.db.tokens_created_by(src))
            # Profits = retour vers l'amont (bank, seed, le wallet qui l'a financé), pas vers un autre dev
            is_dev = is_dev and (is_upstream_role(dst_row["role"] if dst_row else None)
                                 or (src_row is not None and src_row["parent"] == dst))
            key = f"xfer:{ev.signature}:{dst}"
            if self.already(key):
                return None
            montant = f"{ev.sol:,.1f}".replace(",", " ")
            sens = f"{esc(self.label(src) or A.short(src))} ➜ {esc(self.label(dst) or A.short(dst))}"
            if is_dev:
                txt = (f"⚫ <b>PROFITS RAPATRIÉS</b> · <b>{montant} SOL</b>\n{sens}\n"
                       "<i>Le dernier token de ce dev vient probablement d'être rug.</i>")
            else:
                txt = (f"🔁 <b>TRANSFERT INTERNE DU CLUSTER</b> · <b>{montant} SOL</b>\n{sens}\n"
                       "<i>Le cluster se prépare peut-être à financer de nouveaux devs.</i>")
            return Alert(key, "transfer", txt, A.wallet_buttons(src, dst, sig=ev.signature, mute=src))

        dst_label = self.db.get_label(dst)
        if dst_label:  # envoi vers un exchange connu
            if ev.sol < self.cfg.profit_min_sol:
                return None
            key = f"cex:{ev.signature}:{dst}"
            if self.already(key):
                return None
            montant = f"{ev.sol:,.1f}".replace(",", " ")
            return Alert(key, "cex", f"🏦 <b>ENVOI VERS UN EXCHANGE</b> · <b>{montant} SOL</b>\n"
                                     f"{esc(self.label(src) or A.short(src))} ➜ {esc(dst_label)}\n"
                                     "<i>Encaissement probable.</i>",
                         A.wallet_buttons(src, sig=ev.signature, mute=src))

        if ev.sol < self.cfg.funding_min_sol:
            return None
        if is_service(src_row, self.db.get_label(src)):
            return None  # vu en vrai : un service surveillé par erreur finançait ses clients, tous ajoutés
        key = f"fund:{src}:{dst}"
        if self.already(key):
            return None
        finances = self.db.conn.execute("SELECT COUNT(*) FROM wallets WHERE parent=? AND added_at > ?",
                                        (src, int(time.time()) - 86400)).fetchone()[0]
        if finances >= FUNDED_MAX_24H:
            # Vu en vrai : un distributeur (plateforme de lancement) finançait des dizaines de wallets par jour,
            # tous ajoutés à la watchlist. Au-delà de 12 en 24 h, ce n'est plus un dev qui prépare ses wallets.
            if not self.dry_run:
                self.db.mark_alert_sent(key, "funding")
            return self._skip("financeur en série : nouveaux wallets non ajoutés")
        nb = len(await self.rpc.signatures(dst, limit=NEW_WALLET_MAX_TX + 1))
        if nb >= NEW_WALLET_MAX_TX:
            return self._skip("paiement vers un wallet existant")  # pas un nouveau dev
        depth = (src_row["depth"] if src_row else 0) + 1
        grp = src_row["grp"] if src_row else None
        added = await self.watch(dst, f"NEW_{dst[:4]}", grp, f"financé par {self.label(src) or src[:6]}", depth, src)

        # Rafale (ex. 0,10 SOL vers 5 relais en 1 min) : on résume au lieu de spammer
        now = time.time()
        rafale = [t for t in self._burst[src] if now - t < BURST_WINDOW_S] + [now]
        self._burst[src] = rafale
        if len(rafale) > BURST_MAX_ALERTS + 1:
            if not self.dry_run:
                self.db.mark_alert_sent(key, "funding")
            return None
        if len(rafale) == BURST_MAX_ALERTS + 1:
            txt = (f"🟡 <b>RAFALE DE FUNDINGS</b>\n{esc(self.label(src) or A.short(src))} finance de nombreux "
                   "nouveaux wallets (même schéma que les relais « Reserve »).\n"
                   "<i>Les suivants entrent dans la watchlist sans alerte individuelle.</i>")
            return Alert(key, "funding", txt, A.wallet_buttons(src, trace=src))
        suivi = ("(mode test : pas d'ajout à la watchlist)" if self.dry_run
                 else f"➕ ajouté à la watchlist (profondeur {depth})" if added
                 else "déjà suivi" if self.db.wallet(dst) else f"non ajouté (profondeur max {self.cfg.trace_max_hops})")
        flags = self.rug_flags(src)
        lignes = [f"🟡 <b>NOUVEAU WALLET FINANCÉ</b> · <b>{ev.sol:g} SOL</b>"]
        if flags:
            lignes.append(A.verdict(flags))
        lignes += [A.SEP, A.wallet_line(src, self.label(src), grp),
                   f"➜ nouveau wallet ({nb} tx) :\n<code>{dst}</code>", f"<i>{suivi}</i>",
                   "💡 <i>Un wallet neuf financé par un cluster surveillé est souvent le prochain wallet de "
                   "lancement : sa création de token arrivera dans 🔥 Alertes dev.</i>"]
        lignes += A.flags_block(flags)
        return Alert(key, "funding", "\n".join(lignes), A.wallet_buttons(src, dst, sig=ev.signature, trace=dst))

    # --- traçage automatique du dev après une création ------------------------------
    async def _auto_trace(self, creator: str, info: TokenInfo) -> None:
        """Après une création : fiche du dev + satellites (sujet 🧬), tous ajoutés à la surveillance."""
        from . import devs
        try:
            report = await devs.resolve(self, info, creator, f"${info.symbol or info.mint[:4]}")
            text, markup = devs.card(self, report, info, A.ticker(info))
            self.emit(Alert(f"trace:{info.mint}", "trace", text, markup))
            ann = self.db.find_announcement(None, info.mint, 0)
            if ann:
                flags = set(json.loads(ann["flags"] or "[]")) | set(devs.rug_flags(self, report))
                details = json.loads(ann["details"] or "{}")   # fusion : ne pas effacer faux coins / devs probables
                details.update({"dev_how": report.how, "funding": devs.chain_text(self, report),
                                "dev_history": A.dev_line(info) or ""})
                self.db.update_announcement(ann["id"], dev=creator, flags=json.dumps(sorted(flags)),
                                            satellites=json.dumps(devs.satellites_json(report)),
                                            details=json.dumps(details))
                if self.on_ann_update:
                    await self.on_ann_update(ann["id"])
        except Exception:
            log.exception("Traçage automatique impossible pour %s", creator)

    async def on_pumpportal_create(self, msg: dict) -> None:
        """PumpPortal voit la création avant Helius : alerte immédiate si le créateur est suivi."""
        creator = msg.get("traderPublicKey")
        if creator not in self.watched:
            return
        ev = Event("create", creator, msg.get("signature", ""), int(time.time()), msg["mint"],
                   sol=float(msg.get("solAmount") or 0), tokens_raw=int(float(msg.get("initialBuy") or 0) * 1e6),
                   decimals=6, dex="pump.fun", extra={"symbol": msg.get("symbol"), "name": msg.get("name")})
        alert = await self.process(ev)
        if alert:
            self.emit(alert)

    def _spawn(self, coro, urgent: bool = False) -> None:
        """Tâche de fond. Par défaut en priorité RPC basse : elle ne retarde jamais une alerte."""
        t = asyncio.create_task(coro if urgent else in_background(coro))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

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
        if self.tg.enqueue_top(text, A.top_buttons(alert.info, getattr(self.cfg, "trade_url", "")),
                               key=f"top:{alert.kind}:{alert.info.mint}"):
            log.info("🎯 À ne pas rater : %s %s", alert.kind, alert.info.mint)

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

    def _skip(self, raison: str) -> None:
        """Note pourquoi un événement n'a pas donné d'alerte (visible dans /statut et le journal)."""
        self.decisions[raison] += 1
        vu = _SKIPPED.get()
        if vu is not None:
            vu.append(raison)
        return None

    def emit(self, alert: Alert) -> bool:
        self._emit_top(alert)
        if alert.topic == "scams" and not alert.text.startswith("🏴‍☠️"):
            # Dans la section arnaques, le premier mot doit dire quoi faire
            alert.text = "🏴‍☠️ <b>À SIGNALER — NE PAS ACHETER</b>\n" + alert.text
        if self.tg and not self.dry_run:
            if alert.replace:
                self._spawn(self.tg.replace(alert.key, alert.text, alert.markup, alert.topic), urgent=True)
                log.info("Alerte %s complétée : %s", alert.kind, alert.key)
                return True
            if self.tg.enqueue(alert.text, alert.markup, alert.key, alert.kind, topic=alert.topic):
                log.info("Alerte %s -> %s : %s", alert.kind, alert.topic, alert.key)
                self.decisions[f"alerte {alert.kind}"] += 1
                vu = _SKIPPED.get()
                if vu is not None:
                    vu.append("alerte")
                return True
            return False
        print("\n" + re.sub(r"</?(b|i|code|a)\b[^>]*>", "", alert.text) + "\n")
        return True


async def run_signature_cli(signature: str, send: bool) -> int:
    """Rejoue une transaction passée comme si elle arrivait maintenant (pour tester)."""
    from . import config as cfgmod
    cfg = cfgmod.load()
    db = DB(cfg.db_path)
    db.import_watchlist(cfg.watchlist_path)
    db.import_labels(cfg.labels_path)
    tg = Telegram(cfg.telegram_bot_token, cfg.telegram_chat_id) if send else None
    async with SolanaRPC(cfg.rpc_url) as rpc, aiohttp.ClientSession() as http:
        p = Pipeline(cfg, db, rpc, http, None, None, dry_run=True)
        await p.detect_mints()
        tx = await rpc.transaction_retry(signature)
        if not tx:
            print("Transaction introuvable.")
            return 1
        events = analyze(tx, p.watched)
        print(f"{len(events)} événement(s) brut(s) : " + ", ".join(f"{e.kind}({A.short(e.wallet)})" for e in events))
        alerts = await p.handle_signature(signature)
        if not alerts:
            print("Aucune alerte (événements filtrés : token ancien, wallet existant, montant trop faible…).")
        for a in alerts:
            p.emit(a)
            if tg:
                await tg.send_now(a.text, a.markup)
        if tg:
            await tg.close()
            print(f"{len(alerts)} alerte(s) envoyée(s) sur Telegram.")
    db.close()
    return 0
