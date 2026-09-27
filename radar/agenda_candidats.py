"""📆 Agenda X — Retrouver LE token d'une annonce : nouveaux tokens pump.fun, DexScreener, preuves du lien, confirmation."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from urllib.parse import quote

import aiohttp

from .agenda_outils import (
    FRESH_CA_S, MATCH_WINDOW_S, PROBABLE_WINDOW_S, PROFILE_FRESH_S, VERIFY_DELAYS_S, norm_ticker)
from .analysis import xlinks
from .analysis.enrich import token_info
from .telegram import esc

log = logging.getLogger("agenda")


class CandidatsMixin:
    """Retrouver LE token d'une annonce : nouveaux tokens pump.fun, DexScreener, preuves du lien, confirmation.
    Mélangé à Agenda : utilise ses attributs (db, tg, p, xw…) et ses méthodes."""

    # ------------------------------------------------------------------ tri : que publier ?
    async def _fresh_ca(self, ca: str) -> bool:
        """CA d'un coin PAS ENCORE lancé (pas de pool, cas $ASH) ou lancé il y a moins de 3 h."""
        try:
            info = await token_info(self.p.rpc, self.p.http, ca, with_dev_history=False)
        except Exception:
            return False
        if info.supply_raw is None:
            return False  # pas un contrat de token
        if not info.has_pool:
            return True
        if info.crowded:
            return False  # déjà lancé et callé
        return info.age_s is not None and info.age_s < FRESH_CA_S
    # ------------------------------------------------------------------ nouveaux tokens
    def _open_announcements(self, ticker: str) -> list:
        since = int(time.time()) - MATCH_WINDOW_S
        return [r for r in self.db.announcements_since(since) if not r["ca"] and norm_ticker(r["ticker"]) == ticker]
    async def on_new_token(self, msg: dict) -> None:
        """PumpPortal : un token pump.fun vient d'être créé. Est-ce un coin annoncé ?"""
        creator = msg.get("traderPublicKey")
        if creator in self._dev_cands:
            # Un dev probable vient de créer un coin : c'est très probablement le coin annoncé
            ann = self.db.announcement(self._dev_cands[creator])
            if ann and not ann["ca"] and self._same_ticker(ann, msg.get("symbol")):
                await self._candidate(ann, msg["mint"], creator, None, "pump.fun",
                                      f"créé par le dev probable · {msg.get('solAmount', 0):.2f} SOL achetés",
                                      by_dev=True)
                return
        ticker = norm_ticker(msg.get("symbol"))
        if not ticker:
            return
        anns = self._open_announcements(ticker)
        if not anns:
            return
        meta = await self._meta(msg.get("uri"))
        twitter = (meta.get("twitter") or (meta.get("extensions") or {}).get("twitter")) if meta else None
        for ann in anns:
            await self._candidate(ann, msg["mint"], msg.get("traderPublicKey"), twitter, "pump.fun",
                                  f"{msg.get('solAmount', 0):.2f} SOL achetés par le créateur")
    async def _meta(self, uri: str | None) -> dict:
        if not uri:
            return {}
        try:
            async with self.p.http.get(uri, timeout=aiohttp.ClientTimeout(total=5)) as r:
                d = await r.json(content_type=None)
                return d if isinstance(d, dict) else {}
        except Exception:
            return {}
    async def _candidate(self, ann, mint: str, creator: str | None, meta_twitter: str | None, where: str,
                         detail: str = "", by_dev: bool = False, verify: bool = False,
                         created_ts: int | None = None) -> None:
        """Un token au ticker annoncé : est-ce LE coin ? Réponse avec un niveau de preuve.

        meta_twitter = lien X brut des métadonnées du token (profil, tweet ou communauté).
        """
        seen = self._seen_mints.get(ann["id"])
        if seen is None:
            # Gardé en base : sinon chaque redémarrage réévaluait tous les tokens du même ticker (vu en vrai :
            # 18 vieux $STARTUP / $HOTEL / $DOG revérifiés à chaque relance, ~100 crédits Helius à chaque fois)
            seen = self._seen_mints[ann["id"]] = set(json.loads(self.db.get(f"ann_seen:{ann['id']}") or "[]"))
        if mint in seen:
            return  # déjà évalué (DexScreener repasse toutes les 90 s)
        seen.add(mint)
        self.db.put(f"ann_seen:{ann['id']}", json.dumps(sorted(seen)))
        if ann["ca"] and ann["ca"] != mint:
            # L'annonce est déjà reliée à un contrat : un autre token du même ticker est une copie
            self._copies[ann["id"]] = self._copies.get(ann["id"], 0) + 1
            self._dirty = True
            self.p._spawn(self._watch_copy(ann["id"], mint))
            return
        if where != "pump.fun":  # un token pump.fun qui vient d'être créé n'est pas encore callé
            info = await token_info(self.p.rpc, self.p.http, mint, with_dev_history=False)
            if info.crowded:
                log.info("$%s : %s déjà lancé et callé (%s tx), pas d'alerte", ann["ticker"], mint[:6], info.tx_count)
                return
            meta_twitter = meta_twitter or info.twitter
            creator = creator or info.creator
            created_ts = created_ts or info.created_ts
        # Chaque token au ticker annoncé est analysé plus tard : est-ce un FAUX coin (bougie puis rug) ?
        self.p._spawn(self._watch_copy(ann["id"], mint))
        official, _why = self._official(ann)
        annonceurs = {src.get("handle") or "" for src in json.loads(ann["sources"] or "[]")}
        dev_link = by_dev or (creator is not None and self._dev_cands.get(creator) == ann["id"])
        # Le lien par le dev n'est une preuve forte que si ce wallet a été trouvé de façon fiable
        dev_link = dev_link and self.p.trust(creator) in ("référence", "prouvé", "lié")
        # Heure de création du token (pas l'heure où on le voit : DexScreener est relu toutes les 90 s)
        cree = created_ts or time.time()
        heures = [ann["launch_ts"]] if ann["launch_ts"] else []
        heures += json.loads(ann["details"] or "{}").get("launch_alts", [])
        probable = any(abs(cree - h) < PROBABLE_WINDOW_S for h in heures)
        ev = xlinks.link_evidence(official, meta_twitter, by_dev=dev_link, time_match=probable,
                                  other_handles=annonceurs)
        if ev.level == "fort":
            await self._confirm(ann, mint, creator, where, detail, ev)
            return
        if verify or ev.level == "moyen" or probable:
            self.p._spawn(self._verify_official(ann["id"], mint, creator, where))
        if ev.level == "moyen" or probable:
            n = self._candidates.get(ann["id"], 0)
            if n >= 3:
                return
            self._candidates[ann["id"]] = n + 1
            tick = esc(ann["ticker"])
            txt = (f"❓ <b>Candidat pour ${tick}</b> — preuve {ev.icon} {ev.level}\n"
                   f"CA : <code>{mint}</code>\nCréateur : <code>{creator or '?'}</code> · {esc(where)}"
                   + (f" · {esc(detail)}" if detail else "") + "\n"
                   + "\n".join(esc(line) for line in ev.lines()) + "\n"
                   f"🔎 Je vérifie si @{esc(official)} affiche ce CA (tweets, bio, site de sa bio)…\n"
                   "⚠️ Les copies du même ticker sont fréquentes : n'achète pas sur ce seul message.")
            self.tg.enqueue(txt, self._token_buttons(mint, ann), key=f"cand:{mint}", kind="agenda",
                            topic="agenda", reply_to=ann["msg_id"])
        else:
            self._copies[ann["id"]] = self._copies.get(ann["id"], 0) + 1
            self._dirty = True
    async def _confirm(self, ann, mint: str, creator: str | None, where: str, detail: str,
                       ev: xlinks.Evidence) -> None:
        """Preuve forte : le token est relié à l'annonce (alerte 🎯, puis dev et satellites)."""
        if not creator:
            try:
                creator = (await token_info(self.p.rpc, self.p.http, mint, with_dev_history=False)).creator
            except Exception:
                creator = None
        official, _why = self._official(ann)
        self.db.update_announcement(ann["id"], ca=mint, dev=creator, status=f"créé ({where})",
                                    details=self._with_proof(self.db.announcement(ann["id"]), ev.level))
        tick = esc(ann["ticker"])
        txt = (f"🎯 <b>COIN ANNONCÉ CRÉÉ — ${tick}</b> · preuve {ev.icon} {ev.level}\n"
               f"Compte officiel : @{esc(official)}\n"
               + "\n".join(esc(line) for line in ev.lines()) + "\n"
               f"CA : <code>{mint}</code>\nCréateur : <code>{creator or '?'}</code>\n"
               f"{esc(where)}{' · ' + esc(detail) if detail else ''}\n"
               "⚡ Avant le call public : vérifie la fiche dev qui arrive dans 🧬")
        self.tg.enqueue(txt, self._token_buttons(mint, ann), key=f"match:{mint}", kind="agenda",
                        topic="agenda", reply_to=ann["msg_id"])
        self._dirty = True
        await self.update_card(ann["id"])
        await self._notify_real(ann, mint, creator)
        if getattr(self.p, "results", None) is not None:
            # Suivi 📈 : ce coin annoncé sur X, une fois lancé, a-t-il monté ?
            self.p.results.record(f"match:{mint}", mint, "annonce", symbol=ann["ticker"],
                                  grp=f"annonce @{official}" if official else None)
        if ev.level == "fort" and not getattr(self.p, "dry_run", False) and hasattr(self.tg, "enqueue_top"):
            # Coin annoncé sur X ET lien vérifié : candidat « à ne pas rater ». Même contrôle que les alertes
            # on-chain (drapeaux graves, drapeaux de l'annonce, données complètes, âge), plus de chemin à part.
            try:
                from .pipeline import Alert
                info = await token_info(self.p.rpc, self.p.http, mint, creator)
                preuve = (ev.strong or ["lien vérifié"])[0]
                self.p._emit_top(Alert(f"match:{mint}", "match", "", None,
                                       top_title=f"${ann['ticker']} ANNONCÉ SUR X EST LANCÉ",
                                       top_why=f"Annoncé par <b>@{esc(official)}</b> · ✅ {esc(preuve)}",
                                       info=info, flags=self.p.rug_flags(creator)))
            except Exception:
                log.exception("Alerte « à ne pas rater » impossible pour %s", mint)
        self.p._spawn(self.resolve(ann["id"]))
    async def _verify_official(self, ann_id: int, mint: str, creator: str | None, where: str) -> None:
        """Lien dans l'autre sens : le compte officiel affiche-t-il CE contrat ? (= preuve forte)"""
        if (ann_id, mint) in self._verifying:
            return
        self._verifying.add((ann_id, mint))
        try:
            for delay in VERIFY_DELAYS_S:
                await asyncio.sleep(delay)
                row = self.db.announcement(ann_id)
                if not row or row["ca"]:
                    return  # déjà relié (à ce token ou à un autre)
                official, _why = self._official(row)
                proof = await self._official_shows(official, mint)
                if proof:
                    await self._confirm(row, mint, creator, where, "", xlinks.Evidence(strong=[proof]))
                    return
        except Exception:
            log.exception("Vérification impossible pour %s", mint)
        finally:
            self._verifying.discard((ann_id, mint))
    async def _official_shows(self, handle: str | None, mint: str) -> str | None:
        from .hunt import URL_RE, _fetch, _is_site
        if not handle or not self.xw:
            return None
        prof = await self._load_profile(handle, PROFILE_FRESH_S) or {}
        if mint in (prof.get("bio") or "") + " ".join(prof.get("links") or []):
            return f"la bio de @{handle} affiche ce CA"
        sites = [u for u in URL_RE.findall(" ".join(prof.get("links") or [])) if _is_site(u, allow_tco=True)]
        for url in sites[:2]:
            if mint in await _fetch(self.p.http, url):
                return f"le site lié dans la bio de @{handle} affiche ce CA"
        for tw in await self.xw.user_tweets(handle) or []:
            if mint in (tw.get("text") or "") + " ".join(tw.get("links") or []):
                return f"@{handle} a publié ce CA sur X"
        return None
    async def poll_dexscreener(self) -> None:
        """Lancements hors pump.fun : cherche le ticker sur DexScreener autour de l'heure annoncée."""
        while True:
            await asyncio.sleep(90)
            now = time.time()
            for ann in self.db.announcements_since(int(now) - MATCH_WINDOW_S):
                if ann["ca"] or not ann["ticker"]:
                    continue
                near = ann["launch_ts"] and -600 < now - ann["launch_ts"] < 3 * 3600
                if not near and not (ann["launch_ts"] is None and int(now) // 90 % 7 == 0):
                    continue
                try:
                    async with self.p.http.get(f"https://api.dexscreener.com/latest/dex/search?q={quote(ann['ticker'])}",
                                               timeout=aiohttp.ClientTimeout(total=8)) as r:
                        pairs = (await r.json(content_type=None)).get("pairs") or []
                except Exception:
                    continue
                for pr in pairs:
                    base = pr.get("baseToken") or {}
                    if pr.get("chainId") != "solana" or norm_ticker(base.get("symbol")) != norm_ticker(ann["ticker"]):
                        continue
                    if (pr.get("pairCreatedAt") or 0) / 1000 < ann["first_seen"] - 86400:
                        continue
                    socials = {s.get("type"): s.get("url") for s in (pr.get("info") or {}).get("socials") or []}
                    await self._candidate(ann, base.get("address"), None, socials.get("twitter"),
                                          pr.get("dexId") or "DEX",
                                          created_ts=int((pr.get("pairCreatedAt") or 0) / 1000) or None)
                await asyncio.sleep(2)
    async def mark_launched(self, mint: str) -> None:
        row = self.db.find_announcement(None, mint, 0)
        if row:
            self.db.update_announcement(row["id"], status="trading ouvert")
            self._dirty = True
            await self.update_card(row["id"])
            self.tg.enqueue(f"🟢 <b>${esc(row['ticker'])} : TRADING OUVERT</b>\n<code>{mint}</code>",
                            self._token_buttons(mint, row), topic="agenda", reply_to=row["msg_id"],
                            key=f"ann-live:{row['id']}", kind="agenda")
