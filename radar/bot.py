"""🤖 Commandes et boutons du bot Telegram.

Le bot ne répond que dans le groupe des alertes et, en privé, aux admins (TELEGRAM_ADMINS).
Commandes : /statut /agenda /token /wallet /tracer /suivre /retirer /x /watchlist /silence /aide.
Coller un CA ou une adresse seule dans le chat affiche sa fiche.
Boutons (callback_data) : t:<adr> tracer · s:<adr> suivre · u:<adr> retirer · m:<adr> couper 24 h ·
r:<adr> réactiver · k:<mint> fiche token · w:<adr> fiche wallet · c:<commande>.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

from . import alerts as A
from .agenda import PARIS
from .analysis import xlinks
from .analysis.enrich import TokenInfo, token_info, x_handle
from .analysis.jev import ROLE_LABELS
from .analysis.tracer import Tracer, save_result
from .analysis.xparse import is_solana_address
from .config import _handles
from .sources import pumpfun, pumpportal
from .sources.helius import in_background
from .sources.x_watch import quiet_until
from .telegram import esc, keyboard

log = logging.getLogger("bot")

MUTE_S = 24 * 3600
DASHBOARD_EVERY_S = 300
ADDRESS_RE = re.compile(r"^\s*([1-9A-HJ-NP-Za-km-z]{32,44})\s*$")
NUM = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
KIND_LABELS = {"create": "créations", "buy": "achats", "sell": "ventes", "supply_in": "supply",
               "lp_add": "ouvertures", "cluster": "entrées cluster", "funding": "fundings",
               "transfer": "transferts", "cex": "exchanges", "agenda": "agenda", "fakes": "faux coins",
               "devs": "fiches dev", "trace": "traçages", "system": "système", "mute": "sourdines"}
MENU = keyboard([("📊 Statut", "c:statut"), ("📅 Agenda", "c:agenda")],
                [("📋 Watchlist", "c:watchlist"), ("❓ Aide", "c:aide")])


@dataclass
class Ctx:
    """Où répondre : même conversation, même sujet, en réponse au message d'origine."""
    bot: "Bot"
    chat_id: int
    thread_id: int | None = None
    reply_to: int | None = None

    async def send(self, text: str, markup: dict | None = None) -> int | None:
        try:
            res = await self.bot.tg.send_now(text, markup, reply_to=self.reply_to, chat_id=self.chat_id,
                                             thread_id=self.thread_id, quiet=True)
            return res["result"]["message_id"]
        except Exception as e:
            log.warning("Réponse Telegram impossible : %s", e)
            return None

    async def edit(self, message_id: int | None, text: str, markup: dict | None = None) -> None:
        if not message_id or not await self.bot.tg.edit_in(self.chat_id, message_id, text, markup):
            await self.send(text, markup)


class Bot:
    def __init__(self, cfg, db, tg, pipeline, agenda, watcher, xwatcher, stats: dict | None = None):
        self.cfg, self.db, self.tg, self.p, self.agenda = cfg, db, tg, pipeline, agenda
        self.watcher, self.xw, self.stats = watcher, xwatcher, stats or {}
        self.started = time.time()
        self.admins = set(getattr(cfg, "telegram_admins", []) or [])
        self._tasks: set[asyncio.Task] = set()

    # --- boucle de réception -----------------------------------------------------------------
    def allowed(self, chat_id, user_id) -> bool:
        return str(chat_id) == str(self.tg.chat_id) or user_id in self.admins

    async def run(self) -> None:
        offset = int(self.db.get("tg_offset") or 0)
        while True:
            try:
                updates = await self.tg.get_updates(offset)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Réception des commandes Telegram : %s (nouvel essai dans 15 s)", str(e) or type(e).__name__)
                await asyncio.sleep(15)
                continue
            for u in updates:
                offset = u["update_id"] + 1
                self.db.put("tg_offset", offset)
                try:
                    if "callback_query" in u:
                        await self.on_callback(u["callback_query"])
                    elif "message" in u:
                        await self.on_message(u["message"])
                except Exception:
                    log.exception("Commande Telegram en échec")

    def _spawn(self, coro) -> None:
        t = asyncio.create_task(in_background(coro))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    @staticmethod
    def _ctx_from(bot: "Bot", msg: dict) -> Ctx:
        thread = msg.get("message_thread_id") if msg.get("is_topic_message") else None
        return Ctx(bot, msg["chat"]["id"], thread, msg.get("message_id"))

    async def on_message(self, msg: dict) -> None:
        text = (msg.get("text") or "").strip()
        chat = msg.get("chat") or {}
        user = (msg.get("from") or {}).get("id")
        if not text:
            return
        if not self.allowed(chat.get("id"), user):
            if chat.get("type") == "private" and text.startswith("/"):
                await Ctx(self, chat["id"]).send("⛔ Ce radar est privé.")
            return
        ctx = self._ctx_from(self, msg)
        m = ADDRESS_RE.match(text)
        if m and is_solana_address(m.group(1)):
            await self.cmd_wallet(ctx, [m.group(1)])  # redirige vers la fiche token si c'est un contrat
            return
        if not text.startswith("/"):
            return
        cmd, *args = text.split()
        cmd = cmd[1:].split("@")[0].lower()
        cmd = {"start": "aide", "help": "aide", "status": "statut", "ca": "token", "follow": "suivre"}.get(cmd, cmd)
        handler = getattr(self, f"cmd_{cmd}", None)
        if handler:
            await handler(ctx, args)
        else:
            await ctx.send(f"Commande inconnue : /{esc(cmd)}. Tape /aide.")

    async def on_callback(self, cb: dict) -> None:
        msg = cb.get("message") or {}
        chat = msg.get("chat") or {}
        if not self.allowed(chat.get("id"), (cb.get("from") or {}).get("id")):
            await self.tg.answer_callback(cb["id"], "⛔ Radar privé")
            return
        kind, _, arg = (cb.get("data") or "").partition(":")
        ctx = self._ctx_from(self, msg) if msg else Ctx(self, int(self.tg.chat_id))
        nom = esc(self.p.label(arg) or A.short(arg)) if arg else ""
        if kind == "t":
            await self.tg.answer_callback(cb["id"], "🧬 Traçage lancé…")
            await self.cmd_tracer(ctx, [arg])
        elif kind == "s":
            await self.tg.answer_callback(cb["id"], "👁 Ajout à la surveillance…")
            await self.cmd_suivre(ctx, [arg])
        elif kind == "u":
            await self.tg.answer_callback(cb["id"], "❌ Retrait…")
            await self.cmd_retirer(ctx, [arg])
        elif kind == "m":
            self.db.put(f"mute:{arg}", int(time.time() + MUTE_S))
            await self.tg.answer_callback(cb["id"], "🔇 Coupé 24 h (créations toujours signalées)")
            await ctx.send(f"🔇 <b>{nom}</b> coupé pendant 24 h.\n<i>Ses créations de token restent signalées.</i>",
                           keyboard([("🔊 Réactiver", f"r:{arg}")]))
        elif kind == "r":
            self.db.put(f"mute:{arg}", None)
            await self.tg.answer_callback(cb["id"], "🔊 Réactivé")
            await ctx.send(f"🔊 <b>{nom}</b> : alertes réactivées.")
        elif kind == "k":
            await self.tg.answer_callback(cb["id"])
            await self.cmd_token(ctx, [arg])
        elif kind == "n":
            await self.tg.answer_callback(cb["id"], "🕸 Construction de la toile…")
            await self.cmd_reseau(ctx, [arg])
        elif kind == "N":
            await self.tg.answer_callback(cb["id"], "👁 Ajout du réseau…")
            await self.follow_network(ctx, arg)
        elif kind == "w":
            await self.tg.answer_callback(cb["id"])
            await self.cmd_wallet(ctx, [arg])
        elif kind == "c" and hasattr(self, f"cmd_{arg}"):
            await self.tg.answer_callback(cb["id"])
            await getattr(self, f"cmd_{arg}")(ctx, [])
        else:
            await self.tg.answer_callback(cb["id"], "Bouton expiré")

    # --- commandes -----------------------------------------------------------------------------
    async def cmd_aide(self, ctx: Ctx, args: list[str]) -> None:
        await ctx.send(
            "🛰 <b>MEMECOIN RADAR — COMMANDES</b>\n"
            "🚨 <i>Les alertes vérifiées arrivent en privé, dans ta conversation avec le bot (« À ne pas "
            "rater »). Le groupe garde tout le détail, sans son.</i>\n" + A.SEP + "\n"
            "📊 /statut — état du radar\n"
            "📅 /agenda — coins annoncés à venir\n"
            "🪙 /token &lt;CA&gt; — fiche complète d'un token\n"
            "👛 /wallet &lt;adresse&gt; — ce que le radar sait d'un wallet\n"
            "🧬 /tracer &lt;adresse&gt; — remonter son financement\n"
            "👁 /suivre &lt;adresse&gt; [nom] — le surveiller\n"
            "❌ /retirer &lt;adresse&gt; — ne plus le surveiller\n"
            "🐦 /x &lt;compte&gt; — fiabilité d'un compte X\n"
            "🕸 /reseau &lt;adresse&gt; — toile d'un dev : ses wallets, ses projets, leur sort\n"
            "📋 /watchlist — wallets surveillés\n"
            "🔕 /silence 60 — alertes sans son pendant 60 min (/silence off)\n"
            + A.SEP + "\n"
            "<b>Boutons sous les alertes</b> : 🧬 Tracer · 👁 Suivre · 🔇 Couper 24 h\n"
            "<i>Astuce : colle juste un CA ou une adresse, je te dis ce que c'est.</i>\n"
            "<i>Alerte uniquement : aucun trading, aucune clé privée.</i>", MENU)

    def status_text(self) -> str:
        now = time.time()

        def etat(down):
            return "🟢 connecté" if down is None else f"🔴 coupé depuis {A.age(int(now - down))}"

        rpc = self.p.rpc
        if rpc.auth_error:
            helius = f"🔴 clé refusée ({esc(rpc.auth_error)})"
        elif rpc.recent_failures() >= 5:
            helius = "🟠 sature (limite de débit)"
        else:
            helius = "🟢 OK"
        if self.xw is None:
            x = "⚪ inactive (connexion_x.bat)"
        else:
            reprise = quiet_until(self.cfg.x_quiet_hours)
            x = f"💤 pause de nuit jusqu'à {reprise:%H:%M}" if reprise else "🟢 active"
        par_type = self.db.alerts_since(int(now) - 86400)
        top = ", ".join(f"{KIND_LABELS.get(k, k)} {v}" for k, v in sorted(par_type.items(), key=lambda kv: -kv[1])[:4])
        son = (f"🔕 coupé jusqu'à {datetime.fromtimestamp(self.tg.silent_until, PARIS):%H:%M}"
               if self.tg.silent else "🔔 activé")
        derniere = f"il y a {A.age(int(now - self.tg.last_alert_ts))}" if self.tg.last_alert_ts else "aucune depuis le démarrage"
        return "\n".join([
            "📊 <b>ÉTAT DU RADAR</b>",
            f"⏱ En ligne depuis {A.age(int(now - self.started))}",
            A.SEP,
            f"🛰 Helius temps réel : {etat(self.watcher.down_since)}",
            f"🔑 Clé Helius : {helius}",
            f"💳 Crédits Helius ce mois : ~{int(self.db.get('rpc_month:' + time.strftime('%Y-%m')) or 0):,}"
            .replace(",", " ") + " / 1 000 000 gratuits"
            + (f" · {self.stats['filtrées']} tx de wallets très actifs non téléchargées" if self.stats.get("filtrées") else ""),
            f"🟣 PumpPortal : {etat(pumpportal.state['down_since'])} · {pumpportal.state['tokens']} tokens vus",
            f"🐦 Veille X : {x}",
            "🧠 IA locale : " + (f"🟢 {esc(self.agenda.llm.model)} · {self.agenda.llm.calls} lectures"
                                  + (f" · {self.agenda.llm.last_ms / 1000:.1f} s la dernière" if self.agenda.llm.calls else "")
                                  if self.agenda.llm.enabled and time.time() < self.agenda.llm._ok_until
                                  else "⚪ indisponible (Ollama éteint ?)"),
            f"🤖 IA Jev : {'🟢 active' if self.agenda.jev.enabled else '⚪ non configurée'}",
            A.SEP,
            f"👛 Wallets suivis : <b>{len(self.watcher.addresses)}</b> / {self.cfg.watch_max}",
            f"📜 Contrats en attente de lancement : {len(self.p.mints)}",
            f"📨 Transactions analysées : {self.stats.get('tx', 0)}",
            f"🧮 Depuis {datetime.fromtimestamp(self.p.decisions_since, PARIS):%H:%M} : "
            + esc(self.p.decisions_line()),
            f"🚨 Alertes 24 h : <b>{sum(par_type.values())}</b>" + (f" ({top})" if top else ""),
            f"🕐 Dernière alerte : {derniere}",
            f"🔔 Son : {son}",
            f"<i>Mis à jour {datetime.now(PARIS):%H:%M:%S}</i>",
        ])

    async def cmd_statut(self, ctx: Ctx, args: list[str]) -> None:
        await ctx.send(self.status_text(), keyboard([("🔄 Actualiser", "c:statut"), ("📋 Watchlist", "c:watchlist")]))

    async def cmd_agenda(self, ctx: Ctx, args: list[str]) -> None:
        await ctx.send(self.agenda.render(), keyboard([("🔄 Actualiser", "c:agenda")]))

    async def cmd_watchlist(self, ctx: Ctx, args: list[str]) -> None:
        rows = self.db.conn.execute(
            "SELECT COALESCE(NULLIF(grp,''),'(sans groupe)') g, COUNT(*) n, SUM(depth=0) d FROM wallets "
            "WHERE active=1 GROUP BY g ORDER BY n DESC").fetchall()
        veille = self.db.conn.execute("SELECT COUNT(*) FROM wallets WHERE active=0").fetchone()[0]
        lines = [f"📋 <b>WATCHLIST</b> — {len(self.watcher.addresses)} / {self.cfg.watch_max} suivis", A.SEP]
        for r in rows[:25]:
            lines.append(f"• <b>{esc(r['g'])}</b> : {r['n']}" + (f" <i>({r['d']} de départ)</i>" if r["d"] else ""))
        if len(rows) > 25:
            lines.append(f"… et {len(rows) - 25} autres groupes")
        if veille:
            lines.append(f"💤 En veille (inactifs) : {veille}")
        lines.append("<i>Ajouter : /suivre &lt;adresse&gt; [nom] · Retirer : /retirer &lt;adresse&gt;</i>")
        await ctx.send("\n".join(lines))

    async def cmd_silence(self, ctx: Ctx, args: list[str]) -> None:
        arg = (args[0] if args else "60").lower()
        minutes = 0 if arg in ("off", "0", "non") else int(arg) if arg.isdigit() else 60
        self.tg.set_silence(minutes)
        if minutes:
            fin = datetime.fromtimestamp(self.tg.silent_until, PARIS)
            await ctx.send(f"🔕 Alertes <b>sans son</b> jusqu'à {fin:%H:%M}. Elles arrivent quand même.",
                           keyboard([("🔔 Réactiver le son", "c:son")]))
        else:
            await ctx.send("🔔 Son des alertes réactivé.")

    async def cmd_son(self, ctx: Ctx, args: list[str]) -> None:
        await self.cmd_silence(ctx, ["off"])

    # --- token -------------------------------------------------------------------------------------
    def _address(self, args: list[str]) -> str | None:
        a = (args[0] if args else "").strip()
        return a if is_solana_address(a) else None

    def token_text(self, info: TokenInfo, trust_line: str | None = None) -> str:
        tx = f"{info.tx_count}{'+' if (info.tx_count or 0) >= 1000 else ''}" if info.tx_count is not None else "?"
        marche = [f"📜 <code>{info.mint}</code>", A.market_line(info), f"🧾 {tx} transactions"]
        sec = A.security_line(info)
        if sec:
            marche.append(sec)
        dev = [A.dev_line(info) or "🧬 Dev : inconnu"]
        if info.creator:
            etat = " · 👁 suivi" if info.creator in self.p.watched else ""
            lab = self.p.label(info.creator)
            dev.append(f"👤 Créateur{f' [{esc(lab)}]' if lab else ''}{etat} :\n<code>{info.creator}</code>")
        liens = [A.social_line(info)] + self.p._announcement_line(info)
        if trust_line:
            liens.append(trust_line)
        return A.card("🔎 <b>FICHE TOKEN</b>", info, self.p.rug_flags(info.creator), marche, dev, liens)

    def _token_markup(self, info: TokenInfo) -> dict:
        follow = info.creator if info.creator and info.creator not in self.p.watched else None
        return A.token_buttons(info, follow=follow)

    async def cmd_token(self, ctx: Ctx, args: list[str]) -> None:
        mint = self._address(args)
        if not mint:
            await ctx.send("Usage : /token &lt;CA du token&gt;")
            return
        mid = await ctx.send(f"⏳ Analyse de <code>{mint}</code>…")
        info = await token_info(self.p.rpc, self.p.http, mint)
        if info.supply_raw is None:
            await ctx.edit(mid, "❌ Ce n'est pas un contrat de token. C'est peut-être un wallet :",
                           keyboard([("👛 Fiche wallet", f"w:{mint}")]))
            return
        text, markup = self.token_text(info), self._token_markup(info)
        await ctx.edit(mid, text, markup)
        h = x_handle(info.twitter)
        if h and self.xw:
            self._spawn(self._token_x(ctx, mid, info, h))

    async def _token_x(self, ctx: Ctx, mid: int | None, info: TokenInfo, handle: str) -> None:
        """Complète la fiche avec la fiabilité du compte X (lecture lente, anti-ban)."""
        prof = await self.agenda._load_profile(handle)
        if not prof:
            return
        prof["handle"] = handle
        t = xlinks.account_trust(prof, info.symbol, info.name, info.mint)
        line = f"🐦 @{esc(handle)} : {t.icon} <b>{t.level}</b> ({t.score}/100)"
        if t.flags:
            line += " · 🚩 " + esc(" · ".join(t.flags[:3]))
        await ctx.edit(mid, self.token_text(info, line), self._token_markup(info))

    # --- wallet ------------------------------------------------------------------------------------
    async def cmd_wallet(self, ctx: Ctx, args: list[str]) -> None:
        addr = self._address(args)
        if not addr:
            await ctx.send("Usage : /wallet &lt;adresse&gt;")
            return
        rpc = self.p.rpc
        if await rpc.mint_info(addr):
            await self.cmd_token(ctx, [addr])
            return
        w = self.db.wallet(addr)
        solde, coins = await asyncio.gather(rpc.balance(addr), pumpfun.coins_by_creator(self.p.http, addr),
                                            return_exceptions=True)
        lines = ["👛 <b>FICHE WALLET</b>", f"<code>{addr}</code>", A.SEP]
        if w and w["active"]:
            lines.append(f"👁 Surveillé · <b>{esc(w['label'] or '')}</b>" + (f" · <i>{esc(w['grp'])}</i>" if w["grp"] else ""))
            if w["role"]:
                lines.append(f"🏷 {esc(w['role'])}")
        elif w:
            lines.append(f"💤 En veille (était : {esc(w['label'] or '')})")
        else:
            lines.append("⚪ Pas surveillé")
        exch = self.db.get_label(addr)
        if exch:
            lines.append(f"🏦 {esc(exch)}")
        if self.p.muted(addr):
            lines.append("🔇 Coupé à la main (créations toujours signalées)")
        if not isinstance(solde, Exception):
            lines.append(f"💰 Solde : <b>{solde:.3f} SOL</b>")
        if isinstance(coins, list):
            lines.append(A.SEP)
            if coins:
                aths = [c["ath"] for c in coins if c.get("ath")]
                morts = sum(1 for c in coins if c.get("rug"))
                lines.append(f"🪙 {len(coins)} token(s) pump.fun créés · ATH max {A.usd(max(aths) if aths else None)}"
                             + (f" · {morts} mort(s) (−99 %)" if morts else ""))
                for c in sorted(coins, key=lambda c: -(c.get("ath") or 0))[:3]:
                    lines.append(f"• ${esc(c.get('symbol') or '?')} · ATH {A.usd(c.get('ath'))} · "
                                 f"créé {datetime.fromtimestamp(c.get('created') or 0, PARIS):%d/%m/%y}")
            else:
                lines.append("🪙 Aucun token créé sur pump.fun")
        src = self.db.conn.execute("SELECT src, kind, amount FROM links WHERE dst=? ORDER BY ts LIMIT 1", (addr,)).fetchone()
        n_out = self.db.conn.execute("SELECT COUNT(DISTINCT dst) FROM links WHERE src=?", (addr,)).fetchone()[0]
        if src or n_out:
            lines.append(A.SEP)
            if src:
                slab = self.p.label(src["src"])
                lines.append(f"⬅️ Financé par{f' [{esc(slab)}]' if slab else ''} ({src['amount']:g} SOL, {esc(src['kind'])}) :\n"
                             f"<code>{src['src']}</code>")
            if n_out:
                lines.append(f"➡️ A financé {n_out} wallet(s) connus du radar")
        actions = [("🧬 Tracer", f"t:{addr}"), ("🕸 Réseau", f"n:{addr}")]
        if w and w["active"]:
            actions += [("❌ Retirer", f"u:{addr}"), ("🔇 Couper 24 h", f"m:{addr}")]
        else:
            actions.append(("👁 Suivre", f"s:{addr}"))
        await ctx.send("\n".join(lines), keyboard([("🔍 Solscan", f"https://solscan.io/account/{addr}"),
                                                   ("📊 GMGN", f"https://gmgn.ai/sol/address/{addr}")], actions))

    # --- traçage -----------------------------------------------------------------------------------
    async def cmd_tracer(self, ctx: Ctx, args: list[str]) -> None:
        addr = self._address(args)
        if not addr:
            await ctx.send("Usage : /tracer &lt;adresse&gt;")
            return
        mid = await ctx.send(f"🧬 Traçage de <code>{addr}</code>…\n"
                             "<i>De quelques secondes à 1 min : les alertes en direct restent prioritaires.</i>")
        self._spawn(self._trace_job(ctx, mid, addr))

    async def _trace_job(self, ctx: Ctx, mid: int | None, addr: str) -> None:
        try:
            labels = self.p.labels
            known = {a: lab for a, lab in labels.items() if "hot wallet" in lab.lower() or "exchange" in lab.lower()}
            res = await Tracer(self.p.rpc, self.cfg.hot_wallet_tx_threshold, known).trace(addr, self.cfg.trace_max_hops)
            save_result(res, self.db, False, self.cfg.trace_max_hops)
        except Exception as e:
            await ctx.edit(mid, f"❌ Traçage impossible : {esc(e)}")
            return
        lines = [f"🧬 <b>FINANCEMENT DE</b> {esc(self.p.label(addr) or A.short(addr))}", A.SEP]
        suivre = []
        for i, h in enumerate(res.hops[:10]):
            slab = self.p.label(h.source)
            quand = datetime.fromtimestamp(h.ts, PARIS).strftime("%d/%m %H:%M") if h.ts else "?"
            nb = f"{h.tx_count} tx" if h.tx_count is not None else "10 000+ tx"
            lines.append(f"{NUM[i]} <b>{esc(A.short(h.address))}</b> ({nb}) ⟵ <b>{h.amount:g} SOL</b> · {quand}"
                         + (" · relais" if h.is_relay else ""))
            lines.append(f"   de <code>{h.source}</code>" + (f" [{esc(slab)}]" if slab else ""))
            freres = [s for s in h.siblings if s.same_amount]
            if freres:
                lines.append(f"   👥 {len(freres)} wallet(s) frère(s) : même montant, même minute")
            if h.source_hot:
                lines.append(f"   ⛔ exchange / service ({esc(h.source_hot_info)})")
            elif h.source not in self.p.watched and len(suivre) < 3:
                suivre.append((f"👁 Suivre {A.short(h.source)}", f"s:{h.source}"))
        if not res.hops:
            lines.append("Aucun financement trouvé.")
        lines += [A.SEP, f"🛑 Arrêt : {esc(res.stop_reason)}"]
        await ctx.edit(mid, "\n".join(lines), keyboard(suivre, [("👛 Fiche wallet", f"w:{addr}")]))

    # --- suivre / retirer ---------------------------------------------------------------------------
    async def cmd_suivre(self, ctx: Ctx, args: list[str]) -> None:
        addr = self._address(args)
        if not addr:
            await ctx.send("Usage : /suivre &lt;adresse&gt; [nom]")
            return
        nom = re.sub(r"[^\w\-.$]", "_", " ".join(args[1:]))[:30] or f"MANUEL_{addr[:4]}"
        if addr in self.p.watched:
            await ctx.send(f"👁 Déjà surveillé : <b>{esc(self.p.label(addr) or nom)}</b>")
            return
        if not await self.p.watch(addr, nom, "manuel", "ajouté via Telegram", 0, None):
            raison = ("c'est un exchange / service" if self.db.get_label(addr)
                      else f"watchlist pleine ({self.cfg.watch_max})")
            await ctx.send(f"❌ Non ajouté : {raison}.")
            return
        self._csv_add(addr, nom)
        await ctx.send(f"👁 <b>Surveillé</b> : <b>{esc(nom)}</b>\n<code>{addr}</code>\n"
                       "<i>Alerte à chaque funding, création, achat ou vente.</i>",
                       keyboard([("🧬 Tracer", f"t:{addr}"), ("❌ Retirer", f"u:{addr}")]))

    async def cmd_retirer(self, ctx: Ctx, args: list[str]) -> None:
        addr = self._address(args)
        if not addr:
            await ctx.send("Usage : /retirer &lt;adresse&gt;")
            return
        if not self.db.wallet(addr):
            await ctx.send("Ce wallet n'est pas dans la watchlist.")
            return
        await self.p.unwatch([addr])
        self._csv_remove(addr)
        await ctx.send(f"❌ Retiré : <b>{esc(self.p.label(addr) or A.short(addr))}</b>",
                       keyboard([("↩️ Annuler", f"s:{addr}")]))

    def _csv_add(self, addr: str, nom: str) -> None:
        """Les ajouts manuels vont aussi dans data/watchlist.csv (sinon perdus au prochain démarrage)."""
        path = self.cfg.watchlist_path
        try:
            texte = path.read_text(encoding="utf-8-sig") if path.exists() else "group,label,address,role,notes\n"
            if addr in texte:
                return
            if not texte.endswith("\n"):
                texte += "\n"
            with open(path, "w", encoding="utf-8", newline="") as f:
                f.write(texte)
                csv.writer(f).writerow(["manuel", nom, addr, "ajouté via Telegram",
                                        f"le {datetime.now(PARIS):%d/%m/%Y}"])
        except OSError as e:
            log.error("watchlist.csv non mis à jour : %s", e)

    def _csv_remove(self, addr: str) -> None:
        path = self.cfg.watchlist_path
        try:
            lignes = path.read_text(encoding="utf-8-sig").splitlines(keepends=True)
            gardees = [ligne for ligne in lignes if addr not in ligne]
            if len(gardees) != len(lignes):
                path.write_text("".join(gardees), encoding="utf-8")
        except OSError as e:
            log.error("watchlist.csv non mis à jour : %s", e)

    # --- compte X ----------------------------------------------------------------------------------
    async def cmd_x(self, ctx: Ctx, args: list[str]) -> None:
        handles = _handles(" ".join(args))
        if not handles:
            await ctx.send("Usage : /x &lt;compte&gt; (ex. /x AshbornCoin)")
            return
        h = handles[0]
        if not self.xw:
            cached = self.db.x_account(h)
            if not cached:
                await ctx.send("⚪ Veille X inactive : double-clique sur connexion_x.bat, puis relance le radar.")
                return
        mid = await ctx.send(f"🐦 Lecture de @{esc(h)}…\n<i>Rythme lent anti-ban : jusqu'à quelques minutes.</i>")
        self._spawn(self._x_job(ctx, mid, h))

    async def _x_job(self, ctx: Ctx, mid: int | None, h: str) -> None:
        prof = await self.agenda._load_profile(h, 3600)
        if not prof:
            await ctx.edit(mid, f"❌ Profil @{esc(h)} illisible (compte inexistant ou session X déconnectée).")
            return
        prof["handle"] = h
        t = xlinks.account_trust(prof)
        lines = [f"🐦 <b>@{esc(h)}</b> — {t.icon} <b>{t.level}</b> ({t.score}/100)", A.SEP]
        chiffres = []
        if prof.get("followers") is not None:
            chiffres.append(f"👥 {prof['followers']:,} abonnés".replace(",", " "))
        if prof.get("following") is not None:
            chiffres.append(f"suit {prof['following']:,}".replace(",", " "))
        if chiffres:
            lines.append(" · ".join(chiffres))
        infos = []
        if prof.get("joined"):
            infos.append(f"📅 créé {prof['joined']}")
        if prof.get("username_changes") is not None:
            infos.append(f"✏️ {prof['username_changes']} changement(s) de nom")
        if prof.get("based_in"):
            infos.append(f"📍 {esc(prof['based_in'])}")
        if infos:
            lines.append(" · ".join(infos))
        badge = {"blue": "🔵 certif bleue : payante, ne prouve rien", "gold": "🟡 badge or : organisation vérifiée",
                 "grey": "⚪ badge gris : institution"}.get(prof.get("verified_type") or ("blue" if prof.get("verified") else ""))
        if badge:
            lines.append(badge)
        if prof.get("ai_role"):
            lines.append(f"🤖 IA Jev : {ROLE_LABELS.get(prof['ai_role'], prof['ai_role'])} ({prof.get('ai_p', 0):.0%})")
        if t.plus:
            lines.append(A.SEP)
            lines += [f"➕ {esc(p)}" for p in t.plus]
        lines += A.flags_block(t.flags)
        if prof.get("bio"):
            lines.append(f"📝 <blockquote expandable>{esc(prof['bio'])}</blockquote>")
        recherche = quote(f"from:{h} (pump OR CA OR contract OR solscan)")
        await ctx.edit(mid, "\n".join(lines), keyboard([("🐦 Ouvrir le profil", f"https://x.com/{h}"),
                                                        ("🔎 Ses CA publiés", f"https://x.com/search?q={recherche}&f=live")]))

    # --- réseau d'un dev -------------------------------------------------------------------------------
    async def cmd_reseau(self, ctx: Ctx, args: list[str]) -> None:
        addr = self._address(args)
        if not addr:
            await ctx.send("Usage : /reseau &lt;adresse du dev, ou CA d'un token&gt;")
            return
        mid = await ctx.send(f"🕸 Construction de la toile de <code>{addr}</code>…\n"
                             "<i>Financeurs, wallets frères, wallets financés, projets de chacun, vitesse de "
                             "revente du dev : 1 à 3 minutes (les alertes en direct restent prioritaires).</i>")
        self._spawn(self._reseau_job(ctx, mid, addr))

    async def _reseau_job(self, ctx: Ctx, mid: int | None, addr: str) -> None:
        from .analysis import network
        try:
            if await self.p.rpc.mint_info(addr):  # un CA : on part de son créateur
                info = await token_info(self.p.rpc, self.p.http, addr, with_dev_history=False)
                if not info.creator:
                    await ctx.edit(mid, "❌ Créateur de ce token introuvable.")
                    return
                addr = info.creator
            rep = await network.build(self.p, addr)
        except Exception as e:
            log.exception("Toile impossible pour %s", addr)
            await ctx.edit(mid, f"❌ Toile impossible : {esc(e)}")
            return
        label = self.p.label(addr) or A.short(addr)
        await ctx.edit(mid, network.telegram_text(rep, label),
                       keyboard([("👁 Suivre tout le réseau", f"N:{addr}"), ("🔍 Solscan", f"https://solscan.io/account/{addr}")]))
        try:
            await self.tg.send_document(ctx.chat_id, f"reseau_{label.replace('…', '_')}.html",
                                        network.to_html(rep, label).encode("utf-8"),
                                        caption=f"🕸 Toile interactive de <b>{esc(label)}</b> : ouvre le fichier dans "
                                                "ton navigateur.", thread_id=ctx.thread_id, reply_to=mid)
        except Exception as e:
            log.warning("Toile HTML non envoyée : %s", e)

    async def follow_network(self, ctx: Ctx, seed: str) -> None:
        """Met sous surveillance tous les wallets de la toile (réseau à rugs = groupe ⛔)."""
        from .analysis import network
        saved = self.db.get_network(seed)
        if not saved:
            await ctx.send("Toile introuvable : relance /reseau d'abord.")
            return
        rep = network.Report.from_json(saved[0])
        flag = rep.verdict()[1]
        groupe = "reseau-rugs" if flag and "rugs" in flag else f"réseau {A.short(seed)}"
        ajoutes = 0
        for a, w in rep.wallets.items():
            if w["role"] != "exchange" and a not in self.p.watched:
                ajoutes += await self.p.watch(a, w["label"][:30], groupe, f"réseau de {A.short(seed)} : {w['role']}",
                                              1, seed)
        await ctx.send(f"👁 <b>{ajoutes} wallet(s) du réseau</b> ajoutés à la surveillance (groupe « {esc(groupe)} »)"
                       + ("\n⛔ Réseau à rugs : leurs prochains tokens seront marqués à éviter." if groupe == "reseau-rugs" else ""))

    # --- tableau de bord épinglé ---------------------------------------------------------------------
    async def dashboard(self) -> None:
        """Message d'état épinglé (compartiment ⚙️ / 🤖), mis à jour toutes les 5 min."""
        await asyncio.sleep(30)
        while True:
            try:
                from .telegram import SECTION_INFO
                text = (self.status_text() + "\n<i>Tableau de bord mis à jour toutes les 5 min · /aide</i>\n\n"
                        + SECTION_INFO["system"])
                key = f"dashboard:{self.tg.place('system')}"
                mid = self.db.get(key)
                if not (mid and await self.tg.edit_now(int(mid), text, MENU)):
                    res = await self.tg.send_now(text, MENU, topic="system", quiet=True)
                    mid = res["result"]["message_id"]
                    self.db.put(key, mid)
                    await self.tg.pin(mid)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("Tableau de bord non mis à jour : %s", e)
            await asyncio.sleep(DASHBOARD_EVERY_S)
