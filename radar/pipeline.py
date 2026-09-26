"""Traitement d'une transaction : événements -> vérifications -> liaison au lancement -> alerte.

Utilisé par main.py (temps réel) et par `python -m radar.analysis.classify <signature>` (test).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass

import aiohttp

from . import alerts as A
from .analysis.classify import Event, analyze, programs
from .analysis.enrich import TokenInfo, purge_cache, token_info
from .config import Config
from .db import DB
from .sources.helius import AMM_PROGRAMS, LogsWatcher, SolanaRPC, account_keys, in_background, sol_deltas
from .telegram import Telegram, esc

log = logging.getLogger("pipeline")

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
KIND_TOPIC = {"create": "onchain", "buy": "onchain", "supply_in": "onchain", "lp_add": "onchain",
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
    return any(k in txt for k in ("hot wallet", "cex", "exchange"))


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
            if w and (w["depth"] == 0 or (w["label"] or "").startswith("MINT_")) and await self.rpc.mint_info(a):
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
        for a in addresses:
            g = self.group(a) if a else None
            if g and g in self.cfg.rug_groups:
                return [f"cluster « {g} » : {RUG_MARK} — à signaler, pas à acheter"]
        return []

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

    def cleanup(self) -> None:
        """Libère la mémoire des fenêtres glissantes (appelé chaque heure par main.py)."""
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
        is_dev = is_dev_role(w["role"])
        now = time.time()
        key = (grp, ev.mint)
        entries = {a: t for a, t in self._entries.get(key, {}).items() if now - t < CLUSTER_WINDOW_S}
        entries[ev.wallet] = now
        self._entries[key] = entries
        if len(entries) < 2 and not is_dev:
            return
        akey = f"cluster:{grp}:{ev.mint}:{len(entries) >= 3}"
        if self.already(akey) or akey in self._inflight:
            return
        self._inflight.add(akey)
        try:
            await self._cluster_alert(ev, grp, is_dev, entries, akey)
        finally:
            self._inflight.discard(akey)

    async def _cluster_alert(self, ev: Event, grp: str, is_dev: bool, entries: dict[str, float], akey: str) -> None:
        info = await self._info(ev.mint, dev=True)
        if info.age_s is not None and info.age_s > YOUNG_TOKEN_S:
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
        alert = Alert(akey, "cluster", text, A.token_buttons(info, ev.wallet, mute=ev.wallet))
        self.emit(alert)
        if self.on_cluster_entry:
            await self.on_cluster_entry(grp, ev.mint, list(entries), info, alert)

    def muted(self, wallet: str) -> bool:
        """Coupé à la main (bouton 🔇 Couper 24 h) ?"""
        until = self.db.get(f"mute:{wallet}")
        return bool(until and float(until) > time.time())

    async def process(self, ev: Event) -> Alert | None:
        now = time.time()
        if ev.kind != "create" and self.muted(ev.wallet):
            return None  # les créations de token restent toujours signalées
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
        alert = await handler(ev)
        if alert and ev.kind in TRADE_KINDS:
            self._trades[ev.wallet].append(now)
        return alert

    async def _info(self, mint: str, creator_hint: str | None = None, dev: bool = True) -> TokenInfo:
        return await token_info(self.rpc, self.http, mint, creator_hint, dev)

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
            if not self._quick(key, "create", "🔴 <b>DEV CRÉE UN TOKEN", ev, self.rug_flags(ev.wallet)):
                return None
            return await self._create_alert(ev, key)
        finally:
            self._inflight.discard(key)

    async def _create_alert(self, ev: Event, key: str) -> Alert:
        info = await self._info(ev.mint, ev.wallet)
        info.creator = info.creator or ev.wallet
        info.created_ts = info.created_ts or ev.ts
        self.db.upsert_token(ev.mint, info.symbol, info.name, ev.wallet, ev.ts)
        head = [self._head(ev)]
        flags = self.rug_flags(ev.wallet)
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
        return Alert(key, "create", text, A.token_buttons(info, ev.wallet), replace=True)

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
        key = f"buy:{ev.wallet}:{ev.mint}"
        if self.already(key):
            return None
        info = await self._info(ev.mint)
        if info.age_s is None or info.age_s > YOUNG_TOKEN_S:
            log.info("Achat ignoré (token ancien ou âge inconnu) : %s", ev.mint)
            return None
        if info.crowded:
            log.info("Achat ignoré (déjà lancé et callé, %s tx) : %s", info.tx_count, ev.mint)
            return None
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
        return Alert(key, "lp_add", text, A.token_buttons(info, ev.wallet), replace=True)

    # ⚠️ vente d'un wallet de réserve / du dev
    async def _on_sell(self, ev: Event) -> Alert | None:
        key = f"sell:{ev.wallet}:{ev.mint}"
        if self.already(key):
            return None
        info = await self._info(ev.mint, dev=False)
        pre_pct = info.pct_supply(ev.pre_tokens_raw) or 0.0
        tok = self.db.token(ev.mint)
        is_creator = ev.wallet in (info.creator, tok["creator"] if tok else None)
        if pre_pct < RESERVE_MIN_PCT and not is_creator:
            return None
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
            key = f"wake:{ev.signature}:{dst}"
            if self.already(key):
                return None
            await self.watch(dst, dst_row["label"], dst_row["grp"], dst_row["role"], dst_row["depth"], dst_row["parent"])
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
        key = f"fund:{src}:{dst}"
        if self.already(key):
            return None
        nb = len(await self.rpc.signatures(dst, limit=NEW_WALLET_MAX_TX + 1))
        if nb >= NEW_WALLET_MAX_TX:
            return None  # paiement vers un wallet existant : pas un nouveau dev
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
                   f"➜ nouveau wallet ({nb} tx) :\n<code>{dst}</code>", f"<i>{suivi}</i>"]
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
    def emit(self, alert: Alert) -> bool:
        if self.tg and not self.dry_run:
            if alert.replace:
                self._spawn(self.tg.replace(alert.key, alert.text, alert.markup, alert.topic), urgent=True)
                log.info("Alerte %s complétée : %s", alert.kind, alert.key)
                return True
            if self.tg.enqueue(alert.text, alert.markup, alert.key, alert.kind, topic=alert.topic):
                log.info("Alerte %s -> %s : %s", alert.kind, alert.topic, alert.key)
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
