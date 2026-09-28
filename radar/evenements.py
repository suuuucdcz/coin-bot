"""Événements on-chain -> alertes : un handler par type d'événement (voir radar/analysis/classify.py)."""
from __future__ import annotations

import json
import logging
import time

from . import alerts as A
from .alerte import Alert
from .analysis.classify import Event
from .analysis.enrich import TokenInfo
from .confiance import TRUSTED, is_dev_role, is_service, is_upstream_role
from .reglages import (
    BURST_MAX_ALERTS, BURST_WINDOW_S, CLUSTER_WINDOW_S, DEV_BIG_BUY_PCT, DUMP_BUCKET_S, DUMP_MIN_SOL, FUNDED_MAX_24H,
    INDEPENDENT_GROUPS, NEW_WALLET_MAX_TX, RESERVE_MIN_PCT, SUIVI_MAX_S, SUPPLY_IN_MIN_PCT, SUPPLY_OUT_MIN_PCT, VIVANT_MC,
    YOUNG_TOKEN_S)
from .smart import SMART_GROUP, SMART_TOP_MIN
from .sources import dexscreener
from .telegram import esc

log = logging.getLogger("pipeline")


class EvenementsMixin:
    """Traitement de chaque type d'événement on-chain (création, achat, supply, liquidité, vente,
    funding, cluster). Mélangé à Pipeline : utilise ses méthodes (_info, emit, trust, watch…)."""

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

    async def _suivi(self, mint: str | None) -> str | None:
        """Raison de NE PAS suivre la vente / le déplacement de supply de ce token (None = token suivi).
        Vu en vrai : un wallet d'un réseau à rugs déplaçait la supply d'un token mort de 44 jours ($SEAL) : alerte
        et analyse complète (crédits Helius) pour rien. Un token de plus de 7 jours reste suivi s'il vaut encore
        quelque chose : des coins vivent et descendent doucement pendant des semaines (DexScreener, gratuit)."""
        tok = self.db.token(mint) if mint else None
        if tok is None and not self.label(mint) and not self.db.find_announcement(None, mint, 0):
            return "token non suivi"
        if tok is not None and tok["created_at"] and time.time() - tok["created_at"] > SUIVI_MAX_S:
            m = (await dexscreener.markets(self.http, [mint])).get(mint) if self.http is not None else None
            if not m or (m.get("mc") or 0) < VIVANT_MC:
                return "token de plus de 7 jours, mort"
        return None

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
        smart = grp == SMART_GROUP
        qui = (f"SMART MONEY ENTRE ({len(entries)} wallets)" if smart
               else "LE DEV ENTRE DANS UN TOKEN" if is_dev and len(entries) == 1
               else f"LE CLUSTER ENTRE ({len(entries)} wallets)")
        membres = [f"🎯 Groupe <b>{esc(grp)}</b> · {len(entries)} achat{'s' if len(entries) > 1 else ''} "
                   f"en {CLUSTER_WINDOW_S // 60} min"]
        for a in entries:
            r = self.db.wallet(a)
            membres.append(f"• <b>{esc(self.label(a) or A.short(a))}</b>"
                           + (f" <i>{esc(r['role'])}</i>" if r and r["role"] else "") + f"\n  <code>{a}</code>")
        text = A.card(f"{'🧠' if smart else '🎯'} <b>{qui}</b>", info, self.rug_flags(ev.wallet, info.creator), membres,
                      A.token_block(info, []) + self._announcement_line(info))
        flags = self.rug_flags(ev.wallet, info.creator)
        # « À ne pas rater » seulement si le dev lui-même (ou un wallet prouvé) entre : des satellites
        # ou des acheteurs qui achètent ensemble, c'est exactement ce que fait une ferme de bots.
        confiance = any(self.trust(a) in ("référence", "prouvé") for a in entries)
        if smart:
            # Smart money : un seul wallet peut appâter les copieurs ; il en faut SMART_TOP_MIN ensemble
            confiance = len(entries) >= SMART_TOP_MIN
        alert = Alert(akey, "smart" if smart else "cluster", text, A.token_buttons(info, ev.wallet, mute=ev.wallet),
                      top_title=qui if confiance else "",
                      top_why=(f"{len(entries)} wallets smart money (gros détenteurs de vrais succès) achètent ce "
                               f"token tout jeune en moins de {CLUSTER_WINDOW_S // 60} min" if smart
                               else self._why(ev.wallet, "achète ce token tout jeune") if len(entries) == 1
                                              else f"{len(entries)} wallets du groupe <b>{esc(grp)}</b> achètent "
                                                   f"ce token tout jeune en moins de {CLUSTER_WINDOW_S // 60} min"),
                      info=info, flags=flags, wallet=ev.wallet, event_ts=ev.ts)
        self.emit(alert)
        if self.on_cluster_entry:
            await self.on_cluster_entry(grp, ev.mint, list(entries), info, alert)

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
        return self.emit(Alert(key, kind, "\n".join(lines), A.token_buttons(info, ev.wallet),
                               wallet=ev.wallet, event_ts=ev.ts))
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
        raison = await self._suivi(ev.mint)
        if raison:
            return self._skip(f"déplacement de supply : {raison}")
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
            if await self.already_trading(ev.mint):
                # Vu en vrai après une purge : un ajout de liquidité sur le pool de $ASH (lancé la veille) repartait
                # en « TRADING OUVERT ». Un token qui s'échange déjà n'ouvre pas son trading.
                return self._skip("liquidité ajoutée à un token qui s'échange déjà")
            if not self._quick(key, "lp_add", "🟢 <b>LIQUIDITÉ AJOUTÉE : TRADING OUVERT", ev,
                               self.rug_flags(ev.wallet, ev.mint)):
                return None
            return await self._lp_alert(ev, key)
        finally:
            self._inflight.discard(key)

    async def _lp_alert(self, ev: Event, key: str) -> Alert:
        info = await self._info(ev.mint)
        self.db.put(f"lance:{ev.mint}", int(time.time()))   # souvenir « déjà lancé », gardé par la remise à zéro
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
        encore = self.already(key)
        if encore:
            # Déjà une vente alertée : on ne se tait plus si c'est un gros dump. Vu en vrai ($MrBeast, $INSTA,
            # $Claude, 28/09) : le dev vend un peu 1 à 7 min après la création (alertée), puis vide tout au sommet
            # ~2 h plus tard (166 à 226 k$, −99 % en 15 min) : ce dump n'était pas alerté.
            if ev.sol < DUMP_MIN_SOL:
                return None
            key = f"sell:{ev.wallet}:{ev.mint}:{int(time.time()) // DUMP_BUCKET_S}"
            if self.already(key):
                return None
        tok = self.db.token(ev.mint)
        raison = await self._suivi(ev.mint)
        if raison:
            # Seules les ventes d'un token SUIVI et récent comptent (créé / acheté jeune / annoncé) : sinon chaque
            # revente d'un bot ou d'un satellite coûtait une analyse complète du token (crédits Helius) pour rien.
            return self._skip(f"vente : {raison}")
        info = await self._info(ev.mint, dev=False)
        pre_pct = info.pct_supply(ev.pre_tokens_raw) or 0.0
        is_creator = ev.wallet in (info.creator, tok["creator"] if tok else None)
        if pre_pct < RESERVE_MIN_PCT and not is_creator:
            return self._skip("petite vente (moins de 10 % de la supply)")
        titre = "LE DEV VEND" if is_creator else "UN WALLET DE RÉSERVE VEND"
        if encore:
            titre = "LE DEV VIDE SA POSITION" if is_creator else "UN WALLET DE RÉSERVE VEND ENCORE"
        head = [self._head(ev), f"📉 Vend {self._amount(info, ev)} (détenait {pre_pct:.1f} %) "
                                f"contre <b>{ev.sol:.2f} SOL</b>"]
        text = A.card(f"{'🚨' if encore else '⚠️'} <b>{titre}</b>", info, [], head, A.token_block(info, []))
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
        if src_row is not None and self.trust(src) == "faible" and not is_dev_role(src_row["role"]) \
                and not is_upstream_role(src_row["role"]) and (src_row["grp"] or "") not in self.bad_groups():
            # Vu en vrai ($AXEL) : deux « satellites » (ils avaient financé un détenteur) ont financé 24 wallets neufs
            # en 5 min : une ferme de bots, pas un dev qui prépare son lancement. Alertes et ajouts inutiles, et
            # un flot de notifications qui mange le quota Helius.
            return self._skip("financement par un satellite (bot probable) : non suivi")
        finances = self.db.conn.execute("SELECT COUNT(*) FROM wallets WHERE parent=? AND added_at > ?",
                                        (src, int(time.time()) - 86400)).fetchone()[0]
        if finances >= FUNDED_MAX_24H:
            # Vu en vrai : un distributeur (plateforme de lancement) finançait des dizaines de wallets par jour,
            # tous ajoutés à la watchlist. Au-delà de 12 en 24 h, ce n'est plus un dev qui prépare ses wallets.
            if not self.dry_run:
                self.db.mark_alert_sent(key, "funding")
            return self._skip("financeur en série : nouveaux wallets non ajoutés")
        # Transactions du wallet AVANT ce financement (et pas au moment où on vérifie : un dev qui crée son token
        # dans la foulée aurait déjà 5 tx et passerait pour un vieux wallet)
        nb = len(await self.rpc.signatures(dst, before=ev.signature, limit=NEW_WALLET_MAX_TX + 1))
        if nb >= NEW_WALLET_MAX_TX:
            return self._skip("paiement vers un wallet existant")  # pas un nouveau dev
        depth = (src_row["depth"] if src_row else 0) + 1
        grp = src_row["grp"] if src_row else None
        added = await self.watch(dst, f"NEW_{dst[:4]}", grp, f"financé par {self.label(src) or src[:6]}", depth, src)
        if self.on_funding and not self.dry_run:
            self._spawn(self.on_funding(src, dst, ev.sol))   # le dev d'un coin annoncé prépare-t-il son wallet ?

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
