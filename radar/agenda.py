"""📅 Agenda des coins annoncés sur X : heure, liens, CA, dev, satellites, statut.

Chaîne de liaison :
  tweet d'annonce (ticker, heure, CA éventuel)
    -> CA publié ?  oui : dev + satellites retrouvés tout de suite (cas $ASH)
                    non : on guette la création du token (PumpPortal pour pump.fun, DexScreener sinon)
                          et on la relie à l'annonce (même ticker + même compte X dans les métadonnées)
    -> 🎯 alerte « le coin annoncé vient d'être créé » AVANT le call public
    -> 🟢 statut « trading ouvert » quand le pool apparaît
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone

from . import alerts as A
from . import devs
from .agenda_affichage import AffichageMixin
from .agenda_candidats import CandidatsMixin
from .agenda_faux import FauxCoinsMixin
from .agenda_outils import MATCH_WINDOW_S, PARIS, READ_PER_BATCH, countdown, norm_ticker, paris
from .analysis import xlinks
from .analysis.enrich import token_info, x_handle
from .analysis.jev import Jev, ROLE_LABELS
from .analysis.llm import HANDLE_RE, LocalLLM, fetch_images, time_in_text
from .analysis.xparse import parse_tweet
from .telegram import esc

# Réexportés : main.py et pipeline.py les importent depuis radar.agenda
__all__ = ["Agenda", "PARIS", "paris"]

log = logging.getLogger("agenda")


class Agenda(CandidatsMixin, FauxCoinsMixin, AffichageMixin):
    def __init__(self, pipeline, tg, xwatcher=None):
        self.p = pipeline
        self.db = pipeline.db
        self.tg = tg
        self.xw = xwatcher
        self._dirty = True
        self._last_render = ""
        self._last_edit = 0.0
        self._resolving: set[int] = set()
        self._copies: dict[int, int] = {}
        self._candidates: dict[int, int] = {}
        pipeline.on_launch = self.mark_launched
        pipeline.on_ann_update = self.update_card
        pipeline.on_create = self.on_watched_create
        pipeline.on_funding = self.on_dev_funding
        self._dev_parent: dict[str, str] = {}      # wallet financé par un dev probable -> ce dev
        pipeline.on_cluster_entry = self.on_cluster_entry
        # Devs probables (chasse au dev) : wallet -> annonce
        self._dev_cands: dict[str, int] = {}
        self._hunting: set[int] = set()
        self._copy_watch: set[str] = set()
        self._fakes_done: set[str] = set()
        self._fakes_dirty = True
        self._last_fakes_render = ""
        self._seen_mints: dict[int, set[str]] = {}     # tokens déjà évalués pour chaque annonce
        self._verifying: set[tuple[int, str]] = set()
        self._profiling: set[str] = set()
        self.jev = Jev(getattr(pipeline.cfg, "typesafe_api_key", ""), pipeline.http)
        cfg = pipeline.cfg
        gemini = getattr(cfg, "llm_provider", "ollama") == "gemini" and getattr(cfg, "gemini_api_key", "")
        self.llm = LocalLLM(getattr(cfg, "llm_url", "http://127.0.0.1:11434"),
                            getattr(cfg, "gemini_model", "gemini-3.5-flash-lite") if gemini
                            else getattr(cfg, "llm_model", "gemma4:e4b"),
                            pipeline.http, getattr(cfg, "llm_enabled", False),
                            provider="gemini" if gemini else "ollama", api_key=getattr(cfg, "gemini_api_key", ""),
                            daily_max=getattr(cfg, "llm_daily_max", 450), store=self.db)
        self._last_plan = 0.0
        for r in self.db.announcements_since(int(time.time()) - MATCH_WINDOW_S):
            for c in json.loads(r["details"] or "{}").get("dev_candidates", []):
                self._dev_cands[c["address"]] = r["id"]
                if c.get("parent"):
                    self._dev_parent[c["address"]] = c["parent"]

    async def on_cluster_entry(self, grp: str, mint: str, wallets: list[str], info, alert) -> None:
        """Le dev/les satellites d'un coin annoncé achètent un token : on l'accroche à sa fiche."""
        ticker = norm_ticker(grp) if grp.startswith("$") else None
        ann = self.db.find_announcement(ticker, None, int(time.time()) - MATCH_WINDOW_S) if ticker else None
        if not ann:
            ann = self.db.find_announcement(None, mint, 0)
        if not ann or info.crowded:
            return
        same = norm_ticker(info.symbol) == norm_ticker(ann["ticker"])
        self.tg.enqueue(
            f"🎯 <b>${esc(ann['ticker'])} : le cluster entre dans {A.ticker(info)}</b>"
            + (" — <b>MÊME TICKER que l'annonce</b>" if same else "")
            + f"\nCA : <code>{mint}</code> · {len(wallets)} wallet(s) du groupe\n"
              f"{A.market_line(info)}\n"
            + ("⚡ Probablement le coin annoncé, avant le call public" if same
               else "⚠️ Ticker différent de l'annonce : autre coin du même cluster, à vérifier"),
            A.token_buttons(info), topic="agenda", reply_to=ann["msg_id"],
            key=f"annclu:{ann['id']}:{mint}", kind="agenda")
        fakes_dev = [f for f in json.loads(ann["details"] or "{}").get("fakes", []) if f.get("from_dev")]
        if fakes_dev and mint not in {f["mint"] for f in fakes_dev}:
            self.tg.enqueue(
                f"🎯 <b>Les wallets du dev du faux ${esc(ann['ticker'])} entrent dans {A.ticker(info)}</b>\n"
                f"CA : <code>{mint}</code> · {len(wallets)} wallet(s)\n{A.market_line(info)}\n"
                "⚡ Probablement le VRAI coin, avant le call public.",
                A.token_buttons(info), topic="fakes", key=f"fakeclu:{ann['id']}:{mint}", kind="fakes")
        if not ann["ca"] and same:
            # Relié seulement si le ticker est le même : 2 wallets du groupe qui achètent un autre token
            # (souvent des snipers) ne font pas de ce token le coin annoncé.
            self.db.update_announcement(ann["id"], ca=mint, status="le cluster est entré",
                                        details=self._with_proof(ann, "cluster"))
            self._dirty = True
            await self.update_card(ann["id"])
            self.p._spawn(self.resolve(ann["id"]))

    async def on_dev_funding(self, src: str, dst: str, sol: float) -> None:
        """Un dev probable d'un coin annoncé finance un wallet neuf : c'est souvent le wallet qui lancera le vrai
        token (ou un test / un faux pour chauffer). Il rejoint la famille du dev : s'il crée le token, c'est une
        preuve forte, même avant l'heure annoncée."""
        ann_id = self._dev_cands.get(src)
        ann = self.db.announcement(ann_id) if ann_id else None
        if not ann or ann["ca"] or dst in self._dev_cands:
            return
        self._dev_cands[dst] = ann_id
        self._dev_parent[dst] = src
        details = json.loads(ann["details"] or "{}")
        details.setdefault("dev_candidates", []).append(
            {"address": dst, "reason": f"wallet neuf financé par le dev probable ({sol:g} SOL)", "parent": src})
        self.db.update_announcement(ann_id, details=json.dumps(details))
        quand = f" · lancement prévu {paris(ann['launch_ts'])} (Paris)" if ann["launch_ts"] else ""
        self.tg.enqueue(f"💸 <b>${esc(ann['ticker'])} : le dev probable finance un wallet neuf</b> ({sol:g} SOL){quand}\n"
                        f"<code>{dst}</code>\n<i>Souvent le wallet qui lancera le vrai token (ou un test). S'il crée "
                        f"${esc(ann['ticker'])}, ce sera relié au dev (preuve forte), même avant l'heure.</i>",
                        key=f"devfund:{dst}", kind="agenda", topic="agenda", reply_to=ann["msg_id"])

    def dev_fiable(self, creator: str | None) -> bool:
        """Wallet du dev jugé fiable : lui-même, ou le dev probable qui l'a financé."""
        ok = ("référence", "prouvé", "lié")
        return bool(creator) and (self.p.trust(creator) in ok or self.p.trust(self._dev_parent.get(creator)) in ok)

    async def financeur(self, creator: str) -> str | None:
        """Qui a financé ce créateur ? Base d'abord (aucun crédit), sinon traceur anti-leurre (~3 crédits)."""
        w = self.db.wallet(creator)
        if w is not None and w["parent"]:
            return w["parent"]
        r = self.db.conn.execute("SELECT src FROM links WHERE dst=? ORDER BY ts LIMIT 1", (creator,)).fetchone()
        if r:
            return r["src"]
        if self.p.rpc is None:
            return None
        from .analysis.tracer import Tracer
        try:
            funding, _nb, _r = await Tracer(self.p.rpc, self.p.cfg.hot_wallet_tx_threshold, {}).first_funding(creator)
        except Exception:
            return None
        return funding["source"] if funding else None

    async def on_watched_create(self, creator: str, mint: str) -> None:
        """Création vue par Helius (toutes plateformes) par un dev probable -> relier à l'annonce."""
        ann_id = self._dev_cands.get(creator)
        ann = self.db.announcement(ann_id) if ann_id else None
        if ann and not ann["ca"]:
            info = await token_info(self.p.rpc, self.p.http, mint, creator, with_dev_history=False)
            if not self._same_ticker(ann, info.symbol):
                # Un test ou un faux pour chauffer avant l'heure : pas le coin, mais le dev s'active
                log.info("$%s : le dev probable a créé $%s (autre ticker) : test ou leurre", ann["ticker"], info.symbol)
                self.tg.enqueue(f"🧪 <b>${esc(ann['ticker'])} : le dev probable vient de créer ${esc(info.symbol or '?')}"
                                f"</b> (autre ticker)\n<code>{mint}</code>\n<i>Test ou leurre avant le lancement : "
                                "le dev s'active, le vrai token approche peut-être.</i>",
                                self._token_buttons(mint, ann), key=f"test:{mint}", kind="agenda", topic="agenda",
                                reply_to=ann["msg_id"])
                return
            await self._candidate(ann, mint, creator, None, "on-chain", "créé par le dev probable", by_dev=True)

    @staticmethod
    def _same_ticker(ann, symbol: str | None) -> bool:
        """Vu en vrai : un « dev probable » a créé un token nommé « $TICKER », relié à l'annonce $DOG."""
        return bool(symbol) and norm_ticker(symbol) == norm_ticker(ann["ticker"])

    async def hunt_dev(self, ann_id: int) -> None:
        """CA pas encore publié : on cherche le wallet du dev pour le surveiller avant le lancement."""
        from . import hunt
        if ann_id in self._hunting:
            return
        self._hunting.add(ann_id)
        try:
            # Faux coins déjà créés avec ce ticker (avant qu'on voie l'annonce) : analysés tout de suite
            self.p._spawn(self._scan_existing_copies(ann_id))
            cands = await hunt.hunt(self, ann_id)
            row = self.db.announcement(ann_id)
            if not row or row["ca"]:
                return
            if cands and not self._posted(ann_id):
                # Rumeur : maintenant qu'on a une piste de dev, elle mérite sa fiche
                det0 = json.loads(row["details"] or "{}")
                det0["dev_candidates"] = [{"address": c.address, "reason": c.reason} for c in cands[:6]]
                self.db.update_announcement(ann_id, details=json.dumps(det0))
                await self._maybe_post(ann_id)
                for _ in range(15):
                    await asyncio.sleep(2)
                    row = self.db.announcement(ann_id)
                    if row["msg_id"]:
                        break
            det = json.loads(row["details"] or "{}")
            det["dev_candidates"] = [{"address": c.address, "reason": c.reason} for c in cands[:6]]
            det["hunt_done"] = int(time.time())
            self.db.update_announcement(ann_id, details=json.dumps(det))
            group = f"${row['ticker'] or '?'}"
            for c in cands[:6]:
                self._dev_cands[c.address] = ann_id
                await self.p.watch(c.address, f"DEVPROB_{row['ticker']}"[:40], group, f"dev probable : {c.reason}", 1, None)
            await self.update_card(ann_id)
            if not cands:
                return
            best = cands[0]
            self.tg.enqueue(
                f"🕵️ <b>${esc(row['ticker'])} : dev probable identifié</b>\n<code>{best.address}</code>\n"
                f"<i>{esc(best.reason)}</i>\n"
                + (f"+ {len(cands) - 1} autre(s) piste(s) dans la fiche ⬆️\n" if len(cands) > 1 else "")
                + "👀 Sous surveillance. Je cartographie maintenant ses satellites…",
                topic="agenda", reply_to=row["msg_id"], key=f"hunt:{ann_id}:{best.address}", kind="agenda")

            # Cartographie on-chain autour des meilleurs candidats : tous sous surveillance
            sats: list[dict] = []
            for c in [c for c in cands if c.score >= 2][:3]:
                for s in await devs.expand(self.p, c.address):
                    if s.address in {x["address"] for x in sats} or s.address in self._dev_cands:
                        continue
                    sats.append({"address": s.address, "role": s.role, "dev": c.address})
                    await self.p.watch(s.address, f"SAT_{s.address[:4]}", group, f"satellite de {c.address[:4]} : {s.role}", 2, c.address)
            det = json.loads(self.db.announcement(ann_id)["details"] or "{}")
            det["dev_satellites"] = sats
            self.db.update_announcement(ann_id, details=json.dumps(det))
            await self.update_card(ann_id)
            if sats:
                roles: dict[str, int] = {}
                for s in sats:
                    k = s["role"].split(" (")[0]
                    roles[k] = roles.get(k, 0) + 1
                resume = " · ".join(f"{n} {esc(k)}" for k, n in sorted(roles.items(), key=lambda x: -x[1]))
                self.tg.enqueue(
                    f"🛰 <b>${esc(row['ticker'])} : {len(sats)} satellite(s) sous surveillance</b>\n{resume}\n"
                    "🎯 Alerte dès que le dev ou 2 satellites entrent dans le même coin.",
                    topic="agenda", reply_to=row["msg_id"], key=f"huntsat:{ann_id}", kind="agenda")
        except Exception:
            log.exception("Chasse au dev impossible pour l'annonce %s", ann_id)
        finally:
            self._hunting.discard(ann_id)

    # ------------------------------------------------------------------ entrée : tweets
    @staticmethod
    def _worth_reading(t: dict, info, strict: bool = False) -> bool:
        """Tweets qui méritent l'IA : annonce probable, ticker, CA, ou image (l'heure est souvent dessus).
        strict (quota gratuit de Gemini) : seulement ce que les règles prennent déjà pour une annonce."""
        if strict:
            return bool(info.is_candidate or info.cas)
        return bool(info.is_candidate or info.tickers or info.cas or t.get("images"))

    @staticmethod
    def _merge_reading(info, lu: dict, tweet_time: datetime, texte: str = "") -> bool:
        """Complète la lecture par règles avec celle de l'IA. False = tweet à ignorer (bruit sûr)."""
        from .analysis.xparse import AMBIGUOUS_ZONES, PLATFORMS, _at, explicit_date, zone_tz
        if lu["type"] == "autre" and lu["confiance"] >= 0.85 and not info.cas and not info.launch_ts:
            return False
        if lu["type"] == "arnaque" and lu["confiance"] >= 0.75:
            info.scam.append(f"IA : arnaque probable ({lu['confiance']:.0%}) — {lu['raison']}")
        if not info.tickers and lu["ticker"]:
            info.tickers = [lu["ticker"]]
        if not info.cas and lu["contrat"]:
            info.cas = [lu["contrat"]]
        if not info.launch_ts and lu["heure"] and (lu["heure_sur_image"] or time_in_text(lu["heure"], texte)):
            h, mi = (int(x) for x in lu["heure"].split(":"))
            demain = lu["jour"] == "demain"
            date = explicit_date(texte, tweet_time)
            ou = " (lue sur l'image)" if lu["heure_sur_image"] else ""
            if lu["fuseau"]:
                info.launch_ts = int(_at(tweet_time, zone_tz(lu["fuseau"]), h, mi, demain, date).timestamp())
                info.launch_txt = f"{lu['heure']} {lu['fuseau']}{ou} · lu par l'IA"
            else:
                info.launch_alts = sorted({int(_at(tweet_time, z, h, mi, demain, date).timestamp())
                                           for z in AMBIGUOUS_ZONES})
                info.launch_ts = int(_at(tweet_time, "UTC", h, mi, demain, date).timestamp())
                info.launch_txt = f"{lu['heure']} (fuseau ?){ou} · lu par l'IA"
        if not info.platform and lu["plateforme"]:
            info.platform = next((nom for rx, nom in PLATFORMS if rx.search(lu["plateforme"])), None)
        if lu["type"] == "annonce_projet" and lu["confiance"] >= 0.6:
            info.launch_words = True
        return True

    async def _plan_x(self) -> None:
        """Chef d'orchestre de la veille X : l'IA choisit 2 actions parmi celles préparées par le code
        (chercher le CA d'un coin annoncé, lire son compte officiel). Sans IA : la plus proche du lancement."""
        if not self.xw:
            return
        now = time.time()
        rows = [r for r in self.db.announcements_since(int(now) - MATCH_WINDOW_S)
                if not r["ca"] and r["ticker"] and not json.loads(r["flags"] or "[]")]
        rows.sort(key=lambda r: abs((r["launch_ts"] or now + 86400) - now))
        actions, jobs, contexte = [], [], []
        for r in rows[:10]:
            official = self._official(r)[0]
            n = len(json.loads(r["sources"] or "[]")) or 1
            quand = countdown(r["launch_ts"]) if r["launch_ts"] else "heure inconnue"
            contexte.append(f"- ${r['ticker']} : {quand}, {n} tweet(s), compte officiel probable @{official}")
            actions.append(f"chercher le contrat de ${r['ticker']} (tweets « ${r['ticker']} CA »)")
            jobs.append(("search", f'"${r["ticker"]}" (CA OR contract OR pump OR solscan)'))
            if official and HANDLE_RE.match(official):
                actions.append(f"lire les derniers tweets de @{official} (compte de ${r['ticker']})")
                jobs.append(("timeline", official))
        if not actions:
            return
        choix = await self.llm.choose("\n".join(contexte), actions, 2)
        if choix is None:
            choix = list(range(min(2, len(actions))))   # sans IA : les coins les plus proches du lancement
        self.xw.extra_jobs = [jobs[i] for i in choix]
        log.info("Veille X : prochaines actions %s", " · ".join(actions[i] for i in choix))

    async def on_tweets(self, tweets: list[dict]) -> None:
        lus = 0
        for t in tweets:
            url = t.get("url")
            if not url or self.db.tweet_seen(url):
                continue
            dt = datetime.fromisoformat(t["time"].replace("Z", "+00:00")) if t.get("time") else None
            if dt and datetime.now(timezone.utc) - dt > timedelta(hours=36):
                continue
            info = parse_tweet(t.get("text", ""), dt, t.get("links"))
            quota = min(READ_PER_BATCH, self.llm.per_batch)
            strict = self.llm.provider == "gemini"   # quota gratuit : seulement les annonces probables
            if lus >= quota and self._worth_reading(t, info, strict) and await self.llm.available():
                # Quota de lecture atteint : relu au prochain passage plutôt que jugé sur les seules règles
                # (vu en vrai : une recherche renvoyait 35 tweets, les 23 derniers entraient sans relecture)
                self.db.unsee_tweet(url)
                continue
            if lus < quota and self._worth_reading(t, info, strict) and await self.llm.available():
                lus += 1
                images = await fetch_images(self.p.http, t.get("images") or []) if t.get("images") else []
                lu = await self.llm.read_tweet(t.get("text", ""), t.get("handle"), images)
                if lu:
                    t["ai_local"] = lu
                    if not self._merge_reading(info, lu, dt or datetime.now(timezone.utc), t.get("text", "")):
                        log.info("Ignoré (IA : %s à %.0f %%) : %s", lu["type"], 100 * lu["confiance"], url)
                        continue
                    if self._promo_only(info, lu):
                        log.info("Ignoré (IA : promo d'un tiers sans CA ni heure) : %s", url)
                        continue
            if not info.is_candidate:
                continue
            if self.jev.enabled:
                ai = await self.jev.classify_tweet(t.get("text", ""), t.get("handle"))
                if ai:
                    t["ai"] = ai
                    if ai["type"] == "arnaque" and ai["p_type"] >= 0.8:
                        info.scam.append(f"IA Jev : arnaque probable ({ai['p_type']:.0%})")
                    elif ai["type"] == "autre" and ai["p_type"] >= 0.9 and not info.cas and not info.launch_ts:
                        log.info("Ignoré (IA Jev : pas une annonce, %.0f %%) : %s", 100 * ai["p_type"], url)
                        continue
            try:
                await self.upsert(t, info)
            except Exception:
                log.exception("Annonce non enregistrée : %s", url)

    def _promo_only(self, info, lu: dict) -> bool:
        """Un caller qui parle d'un token (« I bought $PAID », « my $musebook call », « let me explain $YAP ») n'annonce
        pas de lancement : sans CA ni heure, il ne crée pas d'entrée dans l'agenda (vu en vrai : 3 entrées fantômes).
        Il reste ajouté comme source si le coin est déjà à l'agenda."""
        if lu["type"] != "promo_tiers" or lu["confiance"] < 0.85 or info.cas or info.launch_ts:
            return False
        ticker = info.tickers[0] if info.tickers else None
        return not (ticker and self.db.find_announcement(ticker, None, int(time.time()) - MATCH_WINDOW_S))

    async def upsert(self, t: dict, info) -> None:
        ticker = info.tickers[0] if info.tickers else None
        ca = info.cas[0] if info.cas else None
        since = int(time.time()) - MATCH_WINDOW_S
        row = self.db.find_announcement(ticker, ca, since)
        lu = t.get("ai_local")
        annonce_ia = bool(lu and lu["type"] == "annonce_projet" and lu["confiance"] >= 0.6) or bool(
            t.get("ai") and t["ai"].get("type") == "annonce_projet")
        if row is None and not ca and not info.launch_ts and not annonce_ia:
            # Sans CA ni heure, seule une annonce CONFIRMÉE par l'IA crée une fiche. Un ticker et un mot comme
            # « launch » ne suffisent pas (vu en vrai sur le serveur : une réponse, une promo et « use the launch
            # pad » devenaient des lancements, et leur chasse au dev ajoutait 20 wallets inutiles). Idem quand l'IA
            # a lu le tweet sans y voir une annonce (« autre » ou « promo » pas assez sûrs pour être écartés).
            log.info("Ignoré (pas d'annonce confirmée, ni CA ni heure de lancement) : %s", t.get("url"))
            return
        if row and ca and row["ca"] and ca != row["ca"]:
            # Même ticker, AUTRE contrat que celui de l'annonce : copie ou autre projet. Pas fusionné :
            # on le vérifie comme un candidat (et il sera suivi comme copie possible).
            self.p._spawn(self._candidate(row, ca, None, None, f"autre CA cité par @{t.get('handle')}", verify=True))
            return
        if row and info.launch_ts and row["launch_ts"] and abs(info.launch_ts - row["launch_ts"]) > 6 * 3600:
            row = None  # même ticker, lancement à une heure très différente : sans doute un autre projet
        src = {"handle": t.get("handle"), "url": t["url"], "text": (t.get("text") or "")[:280]}
        if t.get("ai"):
            src["p_off"] = round(t["ai"]["p_officiel"], 2)
        elif t.get("ai_local"):
            lu = t["ai_local"]
            src["p_off"] = round(lu["confiance"] if lu["type"] == "annonce_projet"
                                 else 1 - lu["confiance"] if lu["type"] == "promo_tiers" else 0.3, 2)
        if row:
            up: dict = {}
            sources = json.loads(row["sources"] or "[]")
            if t["url"] not in {s["url"] for s in sources}:
                sources.append(src)
                up["sources"] = json.dumps(sources[-30:])
            if not row["launch_ts"] and info.launch_ts:
                up.update(launch_ts=info.launch_ts, launch_txt=info.launch_txt)
            if not row["platform"] and info.platform:
                up["platform"] = info.platform
            if not row["ticker"] and ticker:
                up["ticker"] = ticker
            if row["tweet_text"] == "(ajouté à la main)":
                # Coin ajouté à la main : le vrai tweet d'annonce remplace le lien provisoire
                up.update(tweet_url=t["url"], tweet_text=(t.get("text") or "")[:1000])
                if row["handle"] in (None, "?"):
                    up["handle"] = t.get("handle")
            official, _why = self._official(row, sources)
            flags = set(json.loads(row["flags"] or "[]")) | set(info.scam) | self._lookalike_flags(official, sources)
            if len(flags) != len(json.loads(row["flags"] or "[]")):
                up["flags"] = json.dumps(sorted(flags))
            new_ca = ca and not row["ca"]
            if new_ca and (t.get("handle") or "").lower() != (official or "").lower():
                # CA donné par un autre compte que le compte officiel (caller, copieur…) : on le vérifie
                # comme un candidat au lieu de le relier directement à l'annonce.
                new_ca = False
                self.p._spawn(self._candidate(row, ca, None, None, f"cité sur X par @{t.get('handle')}",
                                              verify=True))
            if new_ca:
                up["ca"] = ca
                up["details"] = self._with_proof(row, "officiel")  # publié par le compte officiel
            if new_ca and not await self._fresh_ca(ca):
                new_ca = False
                up.pop("ca", None)
                up.pop("details", None)
            self.db.update_announcement(row["id"], **up)
            if not up:
                return
            self._dirty = True
            if not self._posted(row["id"]):
                await self._maybe_post(row["id"])  # une rumeur peut devenir un lancement daté
                return
            self.p._spawn(self.update_card(row["id"]))
            acc = json.loads(row["account"] or "{}")
            if official and (acc.get("handle") or "").lower() != official.lower():
                self.p._spawn(self._profile(row["id"], official))  # le compte officiel a changé
            if new_ca:
                self.tg.enqueue(f"🔗 <b>CA publié pour ${esc(row['ticker'] or '?')}</b> par @{esc(t.get('handle'))}\n"
                                f"<code>{ca}</code>\nRecherche du dev et des satellites… (la fiche ⬆️ se met à jour)",
                                topic="agenda", reply_to=row["msg_id"], key=f"ann-ca:{row['id']}:{ca}", kind="agenda")
                self.p._spawn(self.resolve(row["id"]))
            return

        if ca and not await self._fresh_ca(ca):
            # Tweet qui partage le CA d'un coin déjà lancé (shill) : ce n'est pas un lancement à venir
            log.info("Ignoré : $%s (%s…) est déjà lancé", ticker, ca[:6])
            return
        ann_id = self.db.insert_announcement(
            ticker=ticker, handle=t.get("handle"), sources=json.dumps([src]), tweet_url=t["url"],
            tweet_text=(t.get("text") or "")[:1000], launch_ts=info.launch_ts, launch_txt=info.launch_txt,
            platform=info.platform, ca=ca, flags=json.dumps(info.scam), status="annoncé",
            details=json.dumps({**({"ca_proof": "annonce"} if ca else {}),
                                **({"launch_alts": info.launch_alts} if info.launch_alts else {}),
                                **({"tweet_time": t["time"]} if t.get("time") else {})}))
        self._dirty = True
        if t.get("verified"):
            self.db.update_announcement(ann_id, account=json.dumps({"handle": t.get("handle"), "verified": True}))
        log.info("Annonce : $%s par @%s (%s)", ticker, t.get("handle"), info.launch_txt or "heure ?")
        await self._maybe_post(ann_id)

    STATUS_ICON = {"annoncé": "⏳", "contrat prêt, pas de pool": "📜", "sur pump.fun": "🟣",
                   "trading ouvert": "🟢", "CA invalide ?": "❌"}

    async def _load_profile(self, handle: str, max_age_s: int = 86400) -> dict | None:
        """Profil X (cache SQLite) : abonnés, date de création, changements de nom, badge, bio, liens."""
        cached = self.db.x_account(handle)
        if cached and time.time() - cached[1] < max_age_s:
            return json.loads(cached[0])
        if not self.xw:
            return json.loads(cached[0]) if cached else None
        prof = await self.xw.profile(handle)
        if not prof:
            return json.loads(cached[0]) if cached else None
        if self.jev.enabled:
            ai = await self.jev.classify_account(prof)
            if ai:
                prof["ai_role"], prof["ai_p"] = ai["role"], round(ai["p_role"], 2)
        self.db.set_x_account(handle, json.dumps(prof))
        return prof

    async def _profile(self, ann_id: int, handle: str | None) -> None:
        """Profil du compte officiel + note de fiabilité (la certification bleue s'achète : elle ne compte pas)."""
        if not handle or not self.xw or f"{ann_id}:{handle}" in self._profiling:
            return
        self._profiling.add(f"{ann_id}:{handle}")
        try:
            prof = await self._load_profile(handle)
            row = self.db.announcement(ann_id)
            if not prof or not row:
                return
            prof["handle"] = handle
            flags = set(json.loads(row["flags"] or "[]"))
            trust = xlinks.account_trust(prof, row["ticker"], row["name"], row["ca"])
            flags |= {f"@{handle} : {f}" for f in trust.flags}
            if prof.get("ai_role") in ("caller", "celebrite", "bot") and (prof.get("ai_p") or 0) >= 0.7:
                flags.add(f"@{handle} ressemble à un compte « {ROLE_LABELS[prof['ai_role']]} » "
                          f"(IA Jev {prof['ai_p']:.0%}), pas au compte du projet")
            old = json.loads(row["account"] or "{}")
            if (old.get("handle") or "").lower() == handle.lower():
                prof["verified"] = bool(prof.get("verified") or old.get("verified"))
            self.db.update_announcement(ann_id, account=json.dumps(prof), flags=json.dumps(sorted(flags)))
            self._dirty = True
            await self.update_card(ann_id)
        finally:
            self._profiling.discard(f"{ann_id}:{handle}")

    def _official(self, row, sources: list | None = None) -> tuple[str | None, list[str]]:
        """Compte officiel probable du coin annoncé, avec les raisons.

        Le 1er compte qui parle d'un ticker est souvent un caller : on préfère le compte qui porte le nom
        du token, qui parle à la 1re personne, que les autres citent, ou que désignent les métadonnées.
        """
        sources = sources if sources is not None else json.loads(row["sources"] or "[]")
        det = json.loads(row["details"] or "{}") if "details" in row.keys() else {}
        texts: dict[str, str] = {}
        if row["handle"]:
            texts[row["handle"]] = row["tweet_text"] or ""
        for src in sources:
            if src.get("handle"):
                texts.setdefault(src["handle"], src.get("text") or "")
        p_off = {src["handle"]: src["p_off"] for src in sources if src.get("handle") and src.get("p_off") is not None}
        best, best_score, best_why = row["handle"], -1, []
        for h, txt in texts.items():
            mentions = sum(1 for o, ot in texts.items() if o != h and f"@{h.lower()}" in (ot or "").lower())
            score, why = xlinks.official_score(h, txt, row["ticker"], row["name"], mentions, det.get("meta_handle"))
            if h in p_off:
                score += round(3 * p_off[h])
                if p_off[h] >= 0.7:
                    why.append(f"IA : compte du projet ({p_off[h]:.0%})")
            if score > best_score:
                best, best_score, best_why = h, score, why
        return best, best_why

    @staticmethod
    def _lookalike_flags(official: str | None, sources: list) -> set[str]:
        return {f"@{src['handle']} imite le compte officiel @{official}" for src in sources
                if xlinks.lookalike(src.get("handle"), official)}

    # ------------------------------------------------------------------ liaison CA -> dev
    async def resolve(self, ann_id: int) -> None:
        if ann_id in self._resolving:
            return
        self._resolving.add(ann_id)
        try:
            row = self.db.announcement(ann_id)
            if not row or not row["ca"]:
                return
            info = await token_info(self.p.rpc, self.p.http, row["ca"])
            if info.supply_raw is None:
                self.db.update_announcement(ann_id, status="CA invalide ?")
                return
            group = f"${row['ticker'] or info.symbol or row['ca'][:4]}"
            report = await devs.resolve(self.p, info, row["dev"], group)
            status = "trading ouvert" if info.has_pool and not (info.pool_dex or "").startswith("pump.fun") \
                else "sur pump.fun" if info.on_pump_curve else "contrat prêt, pas de pool"
            flags = set(json.loads(row["flags"] or "[]")) | set(devs.rug_flags(self.p, report)) | set(info.flags)
            official, _why = self._official(row)
            link = xlinks.parse_x_url(info.twitter)
            annonceurs = {(src.get("handle") or "").lower() for src in json.loads(row["sources"] or "[]")}
            if link and link.kind == "profil" and official and link.handle.lower() != official.lower() \
                    and link.handle.lower() not in annonceurs:
                flags.add(f"le token renvoie vers @{link.handle}"
                          + (", qui IMITE" if xlinks.lookalike(link.handle, official) else ", pas vers")
                          + f" @{official}")
            elif link and link.kind == "tweet":
                flags.add("le token renvoie vers un tweet, pas vers un compte : lien copiable par n'importe qui")
            market = A.market_line(info)
            details = json.loads(row["details"] or "{}")
            details.update({"dev_how": report.how, "funding": devs.chain_text(self.p, report),
                            "dev_history": A.dev_line(info) or "", "market": re.sub("<[^>]+>", "", market),
                            "meta_handle": x_handle(info.twitter)})
            self.db.update_announcement(ann_id, dev=report.dev, name=info.name, flags=json.dumps(sorted(flags)),
                                        satellites=json.dumps(devs.satellites_json(report)), status=status,
                                        details=json.dumps(details))
            if not info.has_pool:
                # On surveille le contrat : le 1er pool déclenchera 🟢 TRADING OUVERT
                self.p.mints.add(info.mint)
                await self.p.watch(info.mint, f"MINT_{group}"[:40], group, "contrat annoncé sur X", 1, None)
            text, markup = devs.card(self.p, report, info, A.ticker(info))
            self.tg.enqueue(text, markup, key=f"devcard:{ann_id}:{report.dev}", kind="devs", topic="devs")
            await self.update_card(ann_id)
            self.tg.enqueue(
                f"🧬 <b>${esc(row['ticker'] or info.symbol)} : dev trouvé</b> <code>{report.dev or '?'}</code> "
                f"+ {len(report.satellites)} satellite(s) · la fiche ⬆️ est à jour",
                topic="agenda", reply_to=row["msg_id"], key=f"ann-dev:{ann_id}:{report.dev}", kind="agenda")
            self._dirty = True
        except Exception:
            log.exception("Liaison impossible pour l'annonce %s", ann_id)
        finally:
            self._resolving.discard(ann_id)


# ---------------------------------------------------------------------------
# Ajout manuel : python -m radar.agenda ajouter ASH "18:00 UTC" --ca <CA> --x AshbornCoin --plateforme Raydium
def main() -> int:
    import argparse

    from . import config as cfgmod
    from .analysis.xparse import parse_launch_time
    from .db import DB

    p = argparse.ArgumentParser(description="Agenda des lancements")
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("ajouter", help="ajoute un coin à l'agenda (le radar publie sa fiche dans les 20 s)")
    a.add_argument("ticker")
    a.add_argument("heure", nargs="?", help='ex. "18:00 UTC", "20h CET", "2pm EST"')
    a.add_argument("--ca")
    a.add_argument("--x", dest="handle", help="compte X qui annonce (sans @)")
    a.add_argument("--plateforme")
    a.add_argument("--tweet", help="lien du tweet d'annonce")
    sub.add_parser("liste", help="affiche l'agenda")
    args = p.parse_args()
    cfgmod.setup_logging("agenda")
    cfg = cfgmod.load()
    db = DB(cfg.db_path)
    if args.cmd == "liste":
        for r in db.announcements_since(int(time.time()) - MATCH_WINDOW_S):
            when = f"{paris(r['launch_ts'])} Paris" if r["launch_ts"] else "heure ?"
            print(f"#{r['id']} ${r['ticker']} · {when} · @{r['handle']} · {r['status']} · CA {r['ca'] or '-'} · dev {r['dev'] or '-'}")
        return 0
    ticker = norm_ticker(args.ticker)
    ts, txt = parse_launch_time(args.heure or "", datetime.now(timezone.utc))
    if args.heure and not ts:
        print(f"❌ Heure non comprise : « {args.heure} » (exemples : \"18:00 UTC\", \"20:00 CET\", \"2pm EST\")")
        return 1
    if db.find_announcement(ticker, args.ca, int(time.time()) - MATCH_WINDOW_S):
        print(f"${ticker} est déjà dans l'agenda.")
        return 0
    handle = (args.handle or "").lstrip("@") or None
    url = args.tweet or (f"https://x.com/{handle}" if handle else f"https://x.com/search?q=%24{ticker}&f=live")
    ann_id = db.insert_announcement(
        ticker=ticker, handle=handle or "?", sources="[]", tweet_url=url, tweet_text="(ajouté à la main)",
        launch_ts=ts, launch_txt=txt, platform=args.plateforme, ca=args.ca, flags="[]", status="annoncé")
    when = datetime.fromtimestamp(ts, PARIS).strftime("%d/%m %H:%M") + " (Paris)" if ts else "heure non précisée"
    print(f"✅ ${ticker} ajouté (#{ann_id}) : lancement {when}. Le radar publie sa fiche dans les 20 s.")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
