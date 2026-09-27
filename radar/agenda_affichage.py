"""📆 Agenda X — Affichage : fiche de chaque coin, tableau épinglé, rappels avant le lancement."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from urllib.parse import quote

from . import alerts as A
from .agenda_outils import MATCH_WINDOW_S, PARIS, PLAN_EVERY_S, countdown, paris, utc
from .analysis import xlinks
from .analysis.jev import ROLE_LABELS
from .telegram import buttons, esc

log = logging.getLogger("agenda")


class AffichageMixin:
    """Affichage : fiche de chaque coin, tableau épinglé, rappels avant le lancement.
    Mélangé à Agenda : utilise ses attributs (db, tg, p, xw…) et ses méthodes."""

    def _posted(self, ann_id: int) -> bool:
        return self.db.alert_already_sent(f"ann:{ann_id}")

    def _should_post(self, row) -> bool:
        """Une fiche seulement pour un lancement à venir : heure connue (−1 h à +48 h) ou CA frais."""
        if row["ca"]:
            return True
        det = json.loads(row["details"] or "{}")
        if det.get("dev_candidates") or det.get("fakes"):
            return True  # rumeur sans heure, mais on a trouvé le dev ou un faux coin : ça vaut une fiche
        lt = row["launch_ts"]
        return bool(lt and time.time() - 3600 <= lt <= time.time() + 48 * 3600)

    async def _maybe_post(self, ann_id: int) -> None:
        row = self.db.announcement(ann_id)
        if not row or self._posted(ann_id) or not self._should_post(row):
            return
        text, markup = self.card(row)
        topic = "scams" if json.loads(row["flags"] or "[]") else "agenda"
        self.tg.enqueue(text, markup, key=f"ann:{ann_id}", kind="agenda", topic=topic,
                        on_sent=lambda mid, i=ann_id: self.db.update_announcement(i, msg_id=mid))
        self.p._spawn(self._profile(ann_id, self._official(row)[0]))
        if row["ca"]:
            self.p._spawn(self._resolve_later(ann_id))
        elif not json.loads(row["details"] or "{}").get("hunt_done"):
            self.p._spawn(self.hunt_dev(ann_id))
        self._dirty = True

    def card(self, row) -> tuple[str, dict]:
        """Fiche complète d'un coin annoncé (mise à jour en place à chaque nouvelle info)."""
        now = int(time.time())
        tick = f"${esc(row['ticker'])}" if row["ticker"] else "(ticker ?)"
        if row["name"]:
            tick += f" ({esc(row['name'])})"
        acc = json.loads(row["account"] or "{}")
        det = json.loads(row["details"] or "{}") if "details" in row.keys() else {}
        flags = json.loads(row["flags"] or "[]")
        sats = json.loads(row["satellites"] or "[]")
        sources = json.loads(row["sources"] or "[]")

        head = "🚩" if flags else "📣"
        lines = [f"{head} <b>FICHE COIN — {tick}</b>"]
        # Quand
        if row["launch_ts"]:
            lines.append(f"🕐 Lancement : <b>{paris(row['launch_ts'])} (Paris)</b> · {utc(row['launch_ts'])} · "
                         f"{countdown(row['launch_ts'])}  <i>« {esc(row['launch_txt'])} »</i>")
        else:
            lines.append("🕐 Lancement : heure non précisée")
        if det.get("launch_alts"):
            lines.append("   ⚠️ fuseau non précisé dans le tweet : " + " · ".join(
                f"{paris(h)}" for h in det["launch_alts"]) + " (Paris) selon le pays du compte")
        st = row["status"] or "annoncé"
        if st == "annoncé" and row["launch_ts"] and row["launch_ts"] < now - 1800:
            st = "heure passée, pas encore vu on-chain"
        lines.append(f"{self.STATUS_ICON.get(st, '⏰')} Statut : <b>{esc(st)}</b>"
                     + (f" · {esc(row['platform'])}" if row["platform"] else ""))
        if det.get("market"):
            lines.append(f"📈 {esc(det['market'])}")
        # Qui annonce : le compte officiel probable n'est pas forcément le 1er à en parler (callers)
        official, why = self._official(row)
        lines.append(f"\n👑 Compte officiel probable : @{esc(official)}"
                     + (f" <i>({esc(', '.join(why))})</i>" if why else ""))
        if acc and (acc.get("handle") or "").lower() == (official or "").lower() and "followers" in acc:
            trust = xlinks.account_trust(acc, row["ticker"], row["name"], row["ca"])
            bits = []
            if acc.get("followers") is not None:
                bits.append(f"{acc['followers']:,} abonnés".replace(",", " "))
            if acc.get("joined"):
                bits.append(f"créé {acc['joined']}")
            if acc.get("username_changes") is not None:
                bits.append(f"{acc['username_changes']} changement(s) de nom")
            badge = {"blue": "certif bleue (payante, ne prouve rien)", "gold": "badge or (organisation)",
                     "grey": "badge gris"}.get(acc.get("verified_type") or ("blue" if acc.get("verified") else ""))
            if badge:
                bits.append(badge)
            if acc.get("ai_role"):
                bits.append(f"IA : {ROLE_LABELS.get(acc['ai_role'], acc['ai_role'])} ({acc.get('ai_p', 0):.0%})")
            lines.append(f"   Fiabilité : {trust.icon} <b>{trust.level}</b> ({trust.score}/100)"
                         + (" · " + esc(" · ".join(bits)) if bits else ""))
        if row["handle"] and (row["handle"] or "").lower() != (official or "").lower():
            lines.append(f"   1re annonce vue chez @{esc(row['handle'])}")
        if len(sources) > 1:
            others = sorted({s['handle'] for s in sources if s.get('handle') and s['handle'] != row['handle']})
            lines.append(f"   + {len(sources) - 1} autre(s) tweet(s)" + (f" : @{', @'.join(esc(h) for h in others[:5])}" if others else ""))
        # Contrat
        lines.append(f"\n📜 CA : <code>{row['ca']}</code>" if row["ca"]
                     else "\n📜 CA : pas encore publié — je guette sa création 👀")
        # Dev
        if row["dev"]:
            lab = self.p.label(row["dev"])
            lines.append(f"👤 Dev : <code>{row['dev']}</code>" + (f" [{esc(lab)}]" if lab else ""))
            if det.get("dev_how"):
                lines.append(f"   <i>trouvé comme : {esc(det['dev_how'])}</i>")
            if det.get("funding"):
                lines.append(f"   Financement : {esc(det['funding'])}")
            if det.get("dev_history"):
                lines.append(f"   {esc(det['dev_history'])}")
        elif row["ca"]:
            lines.append("👤 Dev : recherche en cours…")
        elif det.get("dev_candidates"):
            lines.append("🕵️ Dev probable (CA pas encore publié) — sous surveillance :")
            for c in det["dev_candidates"]:
                lines.append(f" • <code>{c['address']}</code> — <i>{esc(c['reason'])}</i>")
            dsats = det.get("dev_satellites") or []
            if dsats:
                lines.append(f"🛰 Satellites du dev probable ({len(dsats)}) — sous surveillance :")
                for s in dsats[:12]:
                    lines.append(f" • <code>{s['address']}</code> — {esc(s['role'])}")
                if len(dsats) > 12:
                    lines.append(f"   … et {len(dsats) - 12} autres")
        elif det.get("hunt_done"):
            lines.append("🕵️ Dev : aucune piste trouvée (X, Telegram, anciens coins). Je guette la création du ticker.")
        else:
            lines.append("🕵️ Dev : chasse en cours (X, Telegram, anciens coins)…")
        # Satellites
        if sats:
            lines.append(f"🛰 Satellites ({len(sats)}) :")
            for s in sats[:12]:
                lab = self.p.label(s["address"])
                lines.append(f" • <code>{s['address']}</code> — {esc(s['role'])}" + (f" [{esc(lab)}]" if lab else ""))
            if len(sats) > 12:
                lines.append(f"   … et {len(sats) - 12} autres (fiche 🧬)")
        elif row["dev"]:
            lines.append("🛰 Satellites : aucun trouvé")
        for f in det.get("fakes", []):
            lines.append(f"🎭 Faux coin : <code>{f['mint']}</code> — {esc(f['verdict'])}")
        if self._copies.get(row["id"]):
            lines.append(f"⚠️ {self._copies[row['id']]} autre(s) token(s) avec le même ticker créés (copies)")
        for f in flags:
            lines.append(f"🚩 {esc(f)}")
        txt = (row["tweet_text"] or "").strip().replace("\n", " ")
        lines.append(f"\n<i>« {esc(txt[:250])}{'…' if len(txt) > 250 else ''} »</i>")
        lines.append(f"<i>Mis à jour {datetime.now(PARIS).strftime('%H:%M')}</i>")

        links = [("Tweet", row["tweet_url"]), ("Compte officiel", f"https://x.com/{official or row['handle']}")]
        if row["ticker"]:
            links.append((f"🔎 ${row['ticker']} sur X", f"https://x.com/search?q={quote('$' + row['ticker'])}&f=live"))
        if row["ca"]:
            links += [("pump.fun", f"https://pump.fun/coin/{row['ca']}"),
                      ("DexScreener", f"https://dexscreener.com/solana/{row['ca']}"),
                      ("Solscan", f"https://solscan.io/token/{row['ca']}")]
        if row["dev"]:
            links.append(("Wallet dev", f"https://solscan.io/account/{row['dev']}"))
        return "\n".join(lines), buttons(*links, per_row=3)

    async def update_card(self, ann_id: int) -> None:
        row = self.db.announcement(ann_id)
        if row and row["msg_id"]:
            text, markup = self.card(row)
            await self.tg.edit_now(int(row["msg_id"]), text, markup)

    def _token_buttons(self, mint: str, ann) -> dict:
        return buttons(("pump.fun", f"https://pump.fun/coin/{mint}"), ("Solscan", f"https://solscan.io/token/{mint}"),
                       ("DexScreener", f"https://dexscreener.com/solana/{mint}"),
                       ("Annonce", ann["tweet_url"]), per_row=2)
    # ------------------------------------------------------------------ message épinglé
    def render(self) -> str:
        now = int(time.time())
        today = datetime.now(PARIS).replace(hour=0, minute=0, second=0, microsecond=0)
        t0, t1, t2 = (int((today + timedelta(days=d)).timestamp()) for d in (0, 1, 2))
        rows = self.db.announcements_since(now - 24 * 3600)
        sections = {"next": [], "today": [], "past": [], "tomorrow": [], "unknown": [], "suspect": []}
        for r in rows:
            flags = json.loads(r["flags"] or "[]")
            lt = r["launch_ts"]
            if not self._posted(r["id"]):
                # Rumeur sans heure : listée seulement si au moins 2 comptes différents en parlent
                handles = {s.get("handle") for s in json.loads(r["sources"] or "[]")}
                if not lt and len(handles) >= 2 and not flags:
                    sections["unknown"].append(r)
                continue
            if flags:
                sections["suspect"].append(r)
            elif lt and t0 <= lt < t1:
                sections["past" if lt < now - 1800 else "today"].append(r)
            elif lt and t1 <= lt < t2:
                sections["tomorrow"].append(r)
            elif not lt:
                sections["unknown"].append(r)
        upcoming = sorted((r for r in sections["today"] + sections["tomorrow"] if r["launch_ts"] >= now - 1800),
                          key=lambda r: r["launch_ts"])
        if upcoming:
            n = upcoming[0]
            sections["next"] = [n]
        head = (f"📅 <b>AGENDA DES LANCEMENTS — {datetime.now(PARIS).strftime('%d/%m')}</b> (heure de Paris)\n"
                f"<i>Mis à jour {datetime.now(PARIS).strftime('%H:%M')} · une fiche détaillée par coin plus bas</i>")
        out = [head]
        titles = {"next": "⏭ PROCHAIN LANCEMENT", "today": "🗓 Aujourd'hui (par heure)",
                  "tomorrow": "🗓 Demain", "past": "✔️ Déjà passés aujourd'hui",
                  "unknown": "❔ Rumeurs sans heure (plusieurs comptes en parlent)",
                  "suspect": "🚩 Suspects (ne pas toucher sans vérifier)"}
        for key, title in titles.items():
            if sections[key]:
                out.append(f"\n<b>{title}</b>")
                out += [self._line(r, now) for r in sorted(sections[key], key=lambda r: r["launch_ts"] or 0)]
        if len(out) == 1:
            out.append("\nAucun lancement daté pour l'instant. La veille X tourne 👀")
        from .telegram import SECTION_INFO
        legende = "\n\n" + SECTION_INFO["agenda"]
        text = "\n".join(out)
        if len(text) + len(legende) > 4000:
            text = text[:3950 - len(legende)].rsplit("\n", 1)[0] + "\n… (liste tronquée)"
        return text + legende

    def _line(self, r, now: int) -> str:
        when = f"<b>{paris(r['launch_ts'])}</b>" if r["launch_ts"] else "<b>--:--</b>"
        tick = f"${esc(r['ticker'])}" if r["ticker"] else "?"
        if r["name"]:
            tick += f" ({esc(r['name'][:24])})"
        acc = json.loads(r["account"] or "{}")
        official = self._official(r)[0] or r["handle"]
        who = f"👑 <a href=\"https://x.com/{esc(official)}\">@{esc(official)}</a>"
        if (acc.get("handle") or "").lower() == (official or "").lower() and "followers" in acc:
            t = xlinks.account_trust(acc, r["ticker"], r["name"], r["ca"])
            who += f" {t.icon}"
            if acc.get("followers") is not None:
                f = acc["followers"]
                who += f" ({f / 1000:.1f} k ab.)" if f >= 1000 else f" ({f} ab.)"
        parts = [f"{when} · {tick}" + (f" · {esc(r['platform'])}" if r["platform"] else "") + f" · {who}"]
        st = r["status"] or "annoncé"
        icon = {"trading ouvert": "🟢", "annoncé": "⏳", "contrat prêt, pas de pool": "📜"}.get(st, "🔵")
        if st == "annoncé" and r["launch_ts"] and r["launch_ts"] < now - 1800:
            icon, st = "⏰", "heure passée, pas encore vu on-chain"
        detail = [f"{icon} {esc(st)}"]
        if r["launch_ts"] and r["launch_ts"] > now:
            detail.append(countdown(r["launch_ts"]))
        if r["ca"]:
            detail.append(f"CA <code>{r['ca']}</code>")
        if r["dev"]:
            detail.append(f"Dev <code>{A.short(r['dev'])}</code>")
        sats = json.loads(r["satellites"] or "[]")
        if sats:
            detail.append(f"{len(sats)} satellite(s)")
        if self._copies.get(r["id"]):
            detail.append(f"⚠️ {self._copies[r['id']]} copie(s) du ticker")
        n_src = len(json.loads(r["sources"] or "[]"))
        if n_src > 1:
            detail.append(f"{n_src} tweets")
        for f in json.loads(r["flags"] or "[]")[:2]:
            detail.append(f"🚩 {esc(f)}")
        return parts[0] + "\n    " + " · ".join(detail)

    async def refresh_loop(self) -> None:
        while True:
            try:
                await self._refresh()
            except Exception:
                log.exception("Agenda non mis à jour")
            await asyncio.sleep(20)

    async def _post_pending(self) -> None:
        """Fiches à publier (ajouts manuels, rumeurs devenues datées) + rappels horaires."""
        now = int(time.time())
        rumour_hunts = sum(1 for i in self._hunting if not self._posted(i))
        for row in self.db.announcements_since(now - MATCH_WINDOW_S):
            hunt_needed = not row["ca"] and not json.loads(row["details"] or "{}").get("hunt_done") \
                and row["id"] not in self._hunting
            if not self._posted(row["id"]):
                await self._maybe_post(row["id"])
                # Rumeur sans heure (ex. $MOONKEY) : chasse au dev aussi, une à la fois (rythme X lent)
                if hunt_needed and rumour_hunts < 1 and not self._posted(row["id"]):
                    rumour_hunts += 1
                    self.p._spawn(self.hunt_dev(row["id"]))
                continue
            if hunt_needed:
                self.p._spawn(self.hunt_dev(row["id"]))
            lt = row["launch_ts"]
            if not lt or row["status"] == "trading ouvert" or not row["msg_id"]:
                continue
            tick = esc(row["ticker"] or "?")
            if 0 < lt - now <= 1800:
                self.tg.enqueue(f"⏰ <b>Dans {A.age(lt - now)} : ${tick}</b> ({paris(lt)} Paris)"
                                + (f"\nCA : <code>{row['ca']}</code>" if row["ca"] else "\nCA pas encore publié 👀"),
                                topic="agenda", reply_to=row["msg_id"], key=f"remind30:{row['id']}", kind="agenda")
            elif -600 <= lt - now <= 0:
                self.tg.enqueue(f"🚀 <b>C'est l'heure : ${tick}</b> ({paris(lt)} Paris) — j'attends le pool / la création",
                                topic="agenda", reply_to=row["msg_id"], key=f"remind0:{row['id']}", kind="agenda")

    async def _resolve_later(self, ann_id: int) -> None:
        await asyncio.sleep(5)  # laisse le temps à la fiche d'être publiée (pour la mettre à jour ensuite)
        await self.resolve(ann_id)

    async def _refresh(self) -> None:
        try:
            await self._refresh_fakes()
        except Exception:
            log.exception("Compartiment faux coins non mis à jour")
        await self._post_pending()
        # Quota gratuit de Gemini : le chef d'orchestre passe toutes les 30 min au lieu de 10
        if time.time() - self._last_plan > PLAN_EVERY_S * (3 if self.llm.provider == "gemini" else 1):
            self._last_plan = time.time()
            self.p._spawn(self._plan_x())
        day = datetime.now(PARIS).strftime("%Y-%m-%d")
        text = self.render()
        stale = time.time() - self._last_edit > 300  # compte à rebours rafraîchi toutes les 5 min
        if text == self._last_render and not stale and not self._dirty:
            return
        key = f"agenda_msg:{self.tg.place('agenda')}:{day}"
        msg_id = self.db.get(key)
        ok = False
        if msg_id:
            ok = await self.tg.edit_now(int(msg_id), text)
        if not ok:
            res = await self.tg.send_now(text, topic="agenda")
            msg_id = res["result"]["message_id"]
            self.db.put(key, msg_id)
            await self.tg.pin(int(msg_id))
        self._last_render, self._last_edit, self._dirty = text, time.time(), False
