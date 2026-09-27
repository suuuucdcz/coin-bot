"""📆 Agenda X — Faux coins : copies d'un coin annoncé (bougie puis rug), preuves, section 🎭."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from urllib.parse import quote

import aiohttp

from . import alerts as A
from .agenda_outils import COMMON_FUNDER_STRONG, PARIS, countdown, norm_ticker, paris
from .analysis.enrich import x_handle
from .telegram import esc

log = logging.getLogger("agenda")


class FauxCoinsMixin:
    """Faux coins : copies d'un coin annoncé (bougie puis rug), preuves, section 🎭.
    Mélangé à Agenda : utilise ses attributs (db, tg, p, xw…) et ses méthodes."""

    # ------------------------------------------------------------------ 🎭 faux coins
    async def _watch_copy(self, ann_id: int, mint: str) -> None:
        """Un token au ticker annoncé : on le réanalyse à +15 min, +45 min et +2 h (bougie puis rug ?)."""
        if mint in self._copy_watch:
            return
        self._copy_watch.add(mint)
        for delay in (15 * 60, 30 * 60, 75 * 60):
            await asyncio.sleep(delay)
            if await self.check_fake(ann_id, mint):
                return
    async def _scan_existing_copies(self, ann_id: int) -> None:
        """Tokens au même ticker créés ces dernières 48 h (DexScreener) : faux coins déjà passés ?"""
        row = self.db.announcement(ann_id)
        if not row or not row["ticker"]:
            return
        try:
            async with self.p.http.get(f"https://api.dexscreener.com/latest/dex/search?q={quote(row['ticker'])}",
                                       timeout=aiohttp.ClientTimeout(total=10)) as r:
                pairs = (await r.json(content_type=None)).get("pairs") or []
        except Exception:
            return
        seen: set[str] = set()
        for pr in pairs:
            base = pr.get("baseToken") or {}
            mint = base.get("address")
            if (pr.get("chainId") != "solana" or not mint or mint in seen
                    or norm_ticker(base.get("symbol")) != norm_ticker(row["ticker"])):
                continue
            age = time.time() - (pr.get("pairCreatedAt") or 0) / 1000
            if age > 48 * 3600:
                continue
            seen.add(mint)
            if not await self.check_fake(ann_id, mint) and age < 2 * 3600:
                self.p._spawn(self._watch_copy(ann_id, mint))  # encore jeune : on revérifie plus tard
            if len(seen) >= 6:
                break
    def _with_proof(self, row, proof: str) -> str:
        det = json.loads(row["details"] or "{}")
        det["ca_proof"] = proof
        return json.dumps(det)
    def _protected(self, ann_id: int, mint: str) -> bool:
        """CA relié par une preuve forte (dev on-chain, compte officiel) : une chute de −80 % après le
        lancement est banale pour un vrai coin, ce n'est pas un faux coin."""
        row = self.db.announcement(ann_id)
        if not row or row["ca"] != mint:
            return False
        return json.loads(row["details"] or "{}").get("ca_proof") in ("fort", "officiel", "annonce")
    async def check_fake(self, ann_id: int, mint: str) -> bool:
        from . import fakes
        if mint in self._fakes_done:
            return True
        if self._protected(ann_id, mint):
            return True  # rien à vérifier : c'est le coin confirmé
        try:
            rep = await fakes.analyze(self.p, mint)
        except Exception:
            log.exception("Analyse faux coin impossible : %s", mint)
            return False
        if not rep.is_fake:
            return False
        self._fakes_done.add(mint)
        await self._on_fake(ann_id, rep)
        return True
    async def _on_fake(self, ann_id: int, rep) -> None:
        ann = self.db.announcement(ann_id)
        if not ann:
            return
        tick = esc(ann["ticker"])
        group = f"${ann['ticker']}"
        det = json.loads(ann["details"] or "{}")
        known_devs = {c["address"] for c in det.get("dev_candidates", [])}
        creator_funder = rep.funded_by.get(rep.creator or "")
        top_funder, top_n = (rep.funders[0] if rep.funders else (None, 0))

        # Est-ce le faux coin DU DEV (et pas une copie opportuniste d'un inconnu) ? Seules les preuves
        # on-chain comptent : n'importe qui peut copier le lien X du projet dans ses métadonnées.
        official, _why = self._official(ann)
        preuves: list[str] = []
        indices: list[str] = []
        meta_handle = x_handle(rep.twitter)
        if meta_handle and official and meta_handle.lower() == official.lower():
            indices.append(f"ses métadonnées pointent vers @{meta_handle}, le compte officiel (copiable : simple indice)")
        if rep.creator in known_devs or creator_funder in known_devs or top_funder in known_devs:
            preuves.append("lié au dev probable déjà identifié par la chasse au dev")
        # Acheteurs financés par un même wallet = opération organisée : c'est un opérateur de faux coins
        # (vu en vrai : ses acheteurs sont une ferme de bots qui achète tous les lancements puis revend),
        # PAS une preuve que c'est le dev du vrai coin.
        organise = bool(top_funder and (top_n >= COMMON_FUNDER_STRONG or creator_funder == top_funder))
        if organise:
            indices.append(f"acheteurs financés par un même wallet ({top_n}) : opération organisée")
        from_dev = bool(preuves)

        # Wallet du dev à traquer : financeur commun (créateur + acheteurs) > financeur principal > créateur
        dev_wallet = (creator_funder if creator_funder and creator_funder == top_funder
                      else top_funder if top_n >= 2 else rep.creator)
        tracked: list[tuple[str, str]] = []
        sats = det.get("dev_satellites", [])
        if from_dev:
            for addr, reason in [(dev_wallet, "wallet du dev (financeur du faux coin)"),
                                 (rep.creator, f"créateur du faux coin ${rep.symbol or tick}")]:
                if addr and addr not in {a for a, _ in tracked}:
                    tracked.append((addr, reason))
            cands = det.get("dev_candidates", [])
            for addr, reason in tracked:
                self._dev_cands[addr] = ann_id
                if addr not in {c["address"] for c in cands}:
                    cands.insert(0, {"address": addr, "reason": reason})
                await self.p.watch(addr, f"DEV_FAKE_{ann['ticker']}"[:40], group, f"dev probable : {reason}", 1, None)
            det["dev_candidates"] = cands[:8]
            # Les petits acheteurs ne sont PAS suivis : ce sont des bots qui achètent tous les lancements
            det["dev_satellites"] = sats
        elif organise:
            # Organisateur du faux coin : suivi comme opérateur à éviter (ses prochains tokens seront ⛔)
            for addr, role in ((dev_wallet, "organisateur de faux coins"), (rep.creator, "créateur d'un faux coin")):
                if addr:
                    await self.p.watch(addr, f"FAUX_{ann['ticker']}_{addr[:4]}"[:40], "faux-coins", role, 1, None)

        fake = {"mint": rep.mint, "verdict": rep.verdict, "creator": rep.creator, "dev": dev_wallet if from_dev else None,
                "from_dev": from_dev, "preuves": preuves, "ts": int(time.time()), "real": None,
                "symbol": rep.symbol, "buyers": len(rep.buyers)}
        det.setdefault("fakes", []).append(fake)
        up = {"details": json.dumps(det)}
        was_linked = ann["ca"] == rep.mint
        if was_linked:
            up.update(ca=None, dev=None, status="faux coin détecté — j'attends le vrai")
        self.db.update_announcement(ann_id, **up)
        self._dirty = True
        await self.update_card(ann_id)

        lines = [f"🎭 <b>FAUX COIN ${tick}</b> — NE PAS ACHETER",
                 f"Annonce : @{esc(ann['handle'])}" + (f" · lancement prévu {paris(ann['launch_ts'])} (Paris)" if ann["launch_ts"] else ""),
                 f"CA du faux : <code>{rep.mint}</code>", f"📉 {esc(rep.verdict)}"]
        if was_linked:
            lines.append("⚠️ C'est le coin que j'avais relié à l'annonce : liaison annulée.")
        if from_dev:
            lines.append("\n✅ <b>Faux coin DU DEV</b> :")
            lines += [f" • {esc(p)}" for p in preuves]
            lines.append(f"\n👤 <b>Wallet du dev à traquer</b> : <code>{dev_wallet}</code>")
            if rep.creator and rep.creator != dev_wallet:
                lines.append(f"   créateur du faux : <code>{rep.creator}</code>")
            lines.append(f"🛰 + {len(sats)} petit(s) wallet(s) financé(s) par lui, sous surveillance")
            lines.append(f"\n🔔 Notification ici dès que ce dev lance le VRAI ${tick}.")
        else:
            lines.append("\n❓ Lien avec le dev non prouvé (copie opportuniste possible) : "
                         "ses wallets ne sont PAS traités comme ceux du dev.")
            lines += [f" • indice : {esc(i)}" for i in indices]
            if rep.creator:
                lines.append(f"Créateur : <code>{rep.creator}</code>")
        markup = self._token_buttons(rep.mint, ann)
        self.tg.enqueue("\n".join(lines), markup, topic="fakes", key=f"fake:{rep.mint}", kind="fakes")
        self.tg.enqueue(f"🎭 Faux coin ${tick} détecté (<code>{rep.mint}</code>)"
                        + (f" — wallet du dev trouvé : <code>{dev_wallet}</code>" if from_dev else "")
                        + " · détails dans 🎭 Faux coins du jour",
                        topic="agenda", reply_to=ann["msg_id"], key=f"fake-ag:{rep.mint}", kind="agenda")
        self._fakes_dirty = True
    def _fake_of_dev(self, ann, creator: str | None) -> dict | None:
        """Le créateur est-il un wallet du dev repéré grâce à un faux coin de cette annonce ?"""
        for f in json.loads(ann["details"] or "{}").get("fakes", []):
            if f.get("from_dev") and creator in (f.get("dev"), f.get("creator")):
                return f
        return None
    async def _notify_real(self, ann, mint: str, creator: str | None) -> None:
        """🎯 Le dev du faux coin lance un nouveau coin : très probablement le VRAI."""
        f = self._fake_of_dev(ann, creator)
        if not f or f.get("mint") == mint:
            return
        det = json.loads(ann["details"] or "{}")
        for x in det.get("fakes", []):
            if x["mint"] == f["mint"]:
                x["real"] = mint
        self.db.update_announcement(ann["id"], details=json.dumps(det))
        self._fakes_dirty = True
        self.tg.enqueue(
            f"🎯🎯 <b>LE DEV DU FAUX COIN LANCE UN NOUVEAU ${esc(ann['ticker'])}</b>\n"
            f"CA : <code>{mint}</code>\nCréé par : <code>{creator}</code> (même dev que le faux <code>{A.short(f['mint'])}</code>)\n"
            + (f"Lancement annoncé : {paris(ann['launch_ts'])} (Paris) — on est {countdown(ann['launch_ts'])}\n" if ann["launch_ts"] else "")
            + "⚡ Probablement le VRAI, avant le call public. Je le réanalyse dans 15 min au cas où ce serait un 2e faux.",
            self._token_buttons(mint, ann), topic="fakes", key=f"real:{mint}", kind="fakes")
    def render_fakes(self) -> str:
        """Message épinglé du compartiment 🎭 : faux coins des coins annoncés aujourd'hui."""
        today = int(datetime.now(PARIS).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        out = [f"🎭 <b>FAUX COINS DU JOUR — {datetime.now(PARIS).strftime('%d/%m')}</b>",
               f"<i>Mis à jour {datetime.now(PARIS).strftime('%H:%M')} · faux coins des tickers annoncés</i>"]
        n = 0
        for r in self.db.announcements_since(today - 12 * 3600):
            fakes = [f for f in json.loads(r["details"] or "{}").get("fakes", []) if f.get("ts", 0) >= today - 12 * 3600]
            if not fakes:
                continue
            when = f" · lancement {paris(r['launch_ts'])}" if r["launch_ts"] else ""
            out.append(f"\n<b>${esc(r['ticker'])}</b> (@{esc(r['handle'])}{when})")
            for f in fakes:
                n += 1
                out.append(f" 🎭 <code>{f['mint']}</code>\n    {esc(f['verdict'])}")
                if f.get("from_dev"):
                    out.append(f"    ✅ du dev · wallet à traquer <code>{f['dev']}</code>")
                    out.append(f"    🎯 VRAI lancé : <code>{f['real']}</code>" if f.get("real")
                               else "    ⏳ en attente du vrai lancement (surveillé)")
                else:
                    out.append("    ❓ lien avec le dev non prouvé (copie possible)")
        if not n:
            out.append("\nAucun faux coin détecté aujourd'hui pour l'instant.")
        from .telegram import SECTION_INFO
        text = "\n".join(out)
        text = text[:3700] + ("\n… (tronqué)" if len(text) > 3700 else "")
        return text + "\n\n" + SECTION_INFO["fakes"]
    async def _refresh_fakes(self) -> None:
        text = self.render_fakes()
        if text == self._last_fakes_render and not self._fakes_dirty:
            return
        key = f"fakes_msg:{self.tg.place('fakes')}:{datetime.now(PARIS).strftime('%Y-%m-%d')}"
        msg_id = self.db.get(key)
        ok = bool(msg_id) and await self.tg.edit_now(int(msg_id), text)
        if not ok:
            res = await self.tg.send_now(text, topic="fakes")
            self.db.put(key, res["result"]["message_id"])
            await self.tg.pin(res["result"]["message_id"])
        self._last_fakes_render, self._fakes_dirty = text, False
