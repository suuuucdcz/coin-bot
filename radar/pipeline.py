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

import aiohttp

from . import alerts as A
from .alerte import Alert, KIND_TOPIC, RUG_MARK
from .analysis.classify import Event, analyze, programs
from .analysis.enrich import TokenInfo, purge_cache, token_info
from .confiance import is_dev_role, is_service, is_smart_role, wallet_trust, watch_priority
from .config import Config
from .db import DB
from .evenements import EvenementsMixin
from .reglages import (
    BURST_WINDOW_S, CLUSTER_WINDOW_S, FACTORY_FLAG_24H, FACTORY_UNWATCH_24H, FARM_MIN_TOKENS, FARM_WINDOW_S,
    LAUNCH_OLD_S, SNIPER_MIN_TOKENS, TRADE_KINDS, TRADE_MAX_PER_HOUR, TRADE_MUTE_S)
from .sources import dexscreener
from .sources.helius import AMM_PROGRAMS, LogsWatcher, SolanaRPC, account_keys, in_background, sol_deltas
from .telegram import Telegram, esc
from .smart import SMART_GROUP
from .top import TopMixin

# Réexportés : les autres modules et les tests les importent depuis radar.pipeline
__all__ = ["Pipeline", "run_signature_cli", "Alert", "wallet_trust", "is_dev_role", "RUG_MARK", "KIND_TOPIC"]

log = logging.getLogger("pipeline")


# Raison notée pour l'événement en cours (chaque événement est traité dans sa propre tâche asyncio)
_SKIPPED: ContextVar[list | None] = ContextVar("skipped", default=None)


class Pipeline(TopMixin, EvenementsMixin):
    def __init__(self, cfg: Config, db: DB, rpc: SolanaRPC, http: aiohttp.ClientSession,
                 tg: Telegram | None = None, watcher: LogsWatcher | None = None, dry_run: bool = False):
        self.cfg, self.db, self.rpc, self.http = cfg, db, rpc, http
        self.tg, self.watcher, self.dry_run = tg, watcher, dry_run
        self.watched: set[str] = set()
        self.results = None            # suivi des résultats (radar/results.py), branché par main.py
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
            if self.db.alert_already_sent(f"lp:{a}") or self.db.get(f"lance:{a}") or await self.already_trading(a):
                # Déjà lancé (vu en vrai : $ASH redevenait « en attente » à chaque redémarrage et chaque
                # échange sur le token arrivait ici)
                self.watched.discard(a)
                continue
            if await self.rpc.mint_info(a):
                self.mints.add(a)
        if self.mints:
            log.info("Contrats suivis en attente de lancement : %s", ", ".join(self.label(m) or m for m in self.mints))

    async def already_trading(self, mint: str) -> bool:
        """Le token s'échange-t-il déjà depuis plus de 30 min (DexScreener, gratuit) ? Alors ce n'est plus un
        lancement : un ajout de liquidité ou un contrat « en attente » ne doit pas donner « TRADING OUVERT »."""
        if self.db.get(f"lance:{mint}"):
            return True
        if self.http is None or self.dry_run:
            return False
        paire = await dexscreener.token_pairs(self.http, mint)
        if paire and paire["pair_created"] and time.time() - paire["pair_created"] > LAUNCH_OLD_S:
            self.db.put(f"lance:{mint}", int(time.time()))
            return True
        return False

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

    def is_smart(self, address: str) -> bool:
        w = self.db.wallet(address)
        return bool(w and is_smart_role(w["role"]))

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
        if not w or w["depth"] == 0 or is_smart_role(w["role"]):
            # watchlist de départ jamais reclassée ; un smart money achète beaucoup de tokens : c'est son rôle
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
        if grp == SMART_GROUP:
            return False   # le smart money entre dans beaucoup de tokens : ce n'est pas une ferme
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
        if self.db.get(f"tempete:{address}"):
            return False  # adresse coupée pour flot de transactions (bot / programme) : elle reste coupée
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
            if alert is not None:
                alert.wallet = alert.wallet or ev.wallet
                alert.event_ts = alert.event_ts or ev.ts
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

    def _spawn(self, coro, urgent: bool = False) -> None:
        """Tâche de fond. Par défaut en priorité RPC basse : elle ne retarde jamais une alerte."""
        t = asyncio.create_task(coro if urgent else in_background(coro))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def _skip(self, raison: str) -> None:
        """Note pourquoi un événement n'a pas donné d'alerte (visible dans /statut et le journal)."""
        self.decisions[raison] += 1
        vu = _SKIPPED.get()
        if vu is not None:
            vu.append(raison)
        return None

    def _track(self, alert: Alert, key: str | None = None, kind: str | None = None) -> None:
        """Suivi des résultats (radar/results.py) : l'alerte est mesurée pendant 24 h."""
        if self.results is None or alert.info is None:
            return
        w = alert.wallet
        self.results.record(key or alert.key, alert.info.mint, kind or alert.kind, alert.info.mc_usd,
                            alert.info.symbol, self.group(w) if w else None, self.trust(w) if w else None)

    def emit(self, alert: Alert) -> bool:
        self._emit_top(alert)
        if alert.topic == "scams" and not alert.text.startswith("🏴‍☠️"):
            # Dans la section arnaques, le premier mot doit dire quoi faire
            alert.text = "🏴‍☠️ <b>À SIGNALER — NE PAS ACHETER</b>\n" + alert.text
        if self.tg and not self.dry_run:
            if alert.replace:
                self._spawn(self.tg.replace(alert.key, alert.text, alert.markup, alert.topic), urgent=True)
                log.info("Alerte %s complétée : %s", alert.kind, alert.key)
                self._track(alert)
                return True
            if self.tg.enqueue(alert.text, alert.markup, alert.key, alert.kind, topic=alert.topic,
                               event_ts=alert.event_ts):
                log.info("Alerte %s -> %s : %s", alert.kind, alert.topic, alert.key)
                self._track(alert)
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
