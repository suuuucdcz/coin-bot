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
from urllib.parse import quote
from zoneinfo import ZoneInfo

import aiohttp

from . import alerts as A
from . import devs
from .analysis import xlinks
from .analysis.enrich import token_info, x_handle
from .analysis.jev import ROLE_LABELS, Jev
from .analysis.llm import HANDLE_RE, LocalLLM, fetch_images, time_in_text
from .analysis.xparse import parse_tweet
from .telegram import buttons, esc

log = logging.getLogger("agenda")

PARIS = ZoneInfo("Europe/Paris")
MATCH_WINDOW_S = 36 * 3600          # une annonce reste « active » 36 h
PROBABLE_WINDOW_S = 20 * 60         # même ticker créé à ± 20 min de l'heure annoncée = probable
FRESH_CA_S = 3 * 3600               # un CA déjà tradé depuis plus de 3 h = coin déjà lancé (ignoré)
COMMON_FUNDER_STRONG = 3            # un wallet qui a financé ≥ 3 acheteurs du faux = opération du dev
# Vérification d'un candidat : le compte officiel (X, bio, site de sa bio) affiche-t-il CE contrat ?
VERIFY_DELAYS_S = (30, 120, 300, 900, 1800)
PROFILE_FRESH_S = 1800
READ_PER_BATCH = 12          # tweets lus par l'IA locale par page X (la carte graphique est partagée)
PLAN_EVERY_S = 600           # le chef d'orchestre choisit les prochaines recherches X toutes les 10 min


def norm_ticker(s: str | None) -> str:
    return "".join(ch for ch in (s or "").upper() if ch.isalnum())


def paris(ts: int) -> str:
    return datetime.fromtimestamp(ts, PARIS).strftime("%H:%M")


def utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M UTC")


def countdown(ts: int) -> str:
    d = ts - int(time.time())
    if d <= 0:
        return f"il y a {A.age(-d)}"
    return f"dans {A.age(d)}"


class Agenda:
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
        self.llm = LocalLLM(getattr(cfg, "llm_url", "http://127.0.0.1:11434"), getattr(cfg, "llm_model", "gemma4:e4b"),
                            pipeline.http, getattr(cfg, "llm_enabled", False))
        self._last_plan = 0.0
        for r in self.db.announcements_since(int(time.time()) - MATCH_WINDOW_S):
            for c in json.loads(r["details"] or "{}").get("dev_candidates", []):
                self._dev_cands[c["address"]] = r["id"]

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

    async def on_watched_create(self, creator: str, mint: str) -> None:
        """Création vue par Helius (toutes plateformes) par un dev probable -> relier à l'annonce."""
        ann_id = self._dev_cands.get(creator)
        ann = self.db.announcement(ann_id) if ann_id else None
        if ann and not ann["ca"]:
            info = await token_info(self.p.rpc, self.p.http, mint, creator, with_dev_history=False)
            if not self._same_ticker(ann, info.symbol):
                log.info("$%s : le dev probable a créé $%s (autre ticker), pas relié", ann["ticker"], info.symbol)
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
    def _worth_reading(t: dict, info) -> bool:
        """Tweets qui méritent l'IA : annonce probable, ticker, CA, ou image (l'heure est souvent dessus)."""
        return bool(info.is_candidate or info.tickers or info.cas or t.get("images"))

    @staticmethod
    def _merge_reading(info, lu: dict, tweet_time: datetime, texte: str = "") -> bool:
        """Complète la lecture par règles avec celle de l'IA. False = tweet à ignorer (bruit sûr)."""
        from .analysis.xparse import AMBIGUOUS_ZONES, PLATFORMS, _at, explicit_date, zone_tz
        if lu["type"] == "autre" and lu["confiance"] >= 0.85 and not info.cas and not info.launch_ts:
            return False
        if lu["type"] == "arnaque" and lu["confiance"] >= 0.75:
            info.scam.append(f"IA locale : arnaque probable ({lu['confiance']:.0%}) — {lu['raison']}")
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
            if lus >= READ_PER_BATCH and self._worth_reading(t, info) and await self.llm.available():
                # Quota de lecture atteint : relu au prochain passage plutôt que jugé sur les seules règles
                # (vu en vrai : une recherche renvoyait 35 tweets, les 23 derniers entraient sans relecture)
                self.db.unsee_tweet(url)
                continue
            if lus < READ_PER_BATCH and self._worth_reading(t, info) and await self.llm.available():
                lus += 1
                images = await fetch_images(self.p.http, t.get("images") or []) if t.get("images") else []
                lu = await self.llm.read_tweet(t.get("text", ""), t.get("handle"), images)
                if lu:
                    t["ai_local"] = lu
                    if not self._merge_reading(info, lu, dt or datetime.now(timezone.utc), t.get("text", "")):
                        log.info("Ignoré (IA locale : %s à %.0f %%) : %s", lu["type"], 100 * lu["confiance"], url)
                        continue
                    if self._promo_only(info, lu):
                        log.info("Ignoré (IA locale : promo d'un tiers sans CA ni heure) : %s", url)
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

    STATUS_ICON = {"annoncé": "⏳", "contrat prêt, pas de pool": "📜", "sur pump.fun": "🟣",
                   "trading ouvert": "🟢", "CA invalide ?": "❌"}

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

    def _token_buttons(self, mint: str, ann) -> dict:
        return buttons(("pump.fun", f"https://pump.fun/coin/{mint}"), ("Solscan", f"https://solscan.io/token/{mint}"),
                       ("DexScreener", f"https://dexscreener.com/solana/{mint}"),
                       ("Annonce", ann["tweet_url"]), per_row=2)

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

    async def _refresh(self) -> None:
        try:
            await self._refresh_fakes()
        except Exception:
            log.exception("Compartiment faux coins non mis à jour")
        await self._post_pending()
        if time.time() - self._last_plan > PLAN_EVERY_S:
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


# ---------------------------------------------------------------------------
# Ajout manuel : python -m radar.agenda ajouter ASH "18:00 UTC" --ca <CA> --x AshbornCoin --plateforme Raydium
def main() -> int:
    import argparse
    import sys

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
