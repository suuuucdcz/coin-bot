"""🚀 Lancements qui décollent : retrouver le wallet NEUF d'un dev connu à partir de son token.

Les bons devs relancent depuis un wallet neuf, pour ne pas être suivis. Le radar ne voit ces wallets à l'avance que si
leur bank est surveillé. Ici, il part dans l'autre sens :
  0. dès la création : si le créateur est déjà connu de la base (même hors watchlist), alerte immédiate ;
  1. sinon, chaque nouveau token pump.fun (flux PumpPortal, gratuit) est mis de côté ;
  2. entre 3 et 10 min après sa création, DexScreener (gratuit, par lots de 30) dit s'il décolle vraiment ;
  3. pour ceux qui décollent seulement (quelques %), l'argent du créateur est remonté : d'abord par la toile
     (radar/toile.py, déjà fait à la création avec des RPC publics, 0 crédit), sinon par le traceur anti-leurre
     (les petits relais sont suivis sur 3 niveaux ; ~3 à 8 crédits Helius, plafonné par heure) ;
  4. si l'argent vient d'un wallet que le radar connaît (dev ou bank propre, même sorti de la watchlist) :
     « 🔴 NOUVEAU WALLET D'UN DEV CONNU » ; s'il vient d'un réseau à rugs : ⛔ dans 🏴‍☠️ Arnaques.
Le créateur entre alors dans la watchlist (ses ventes, ses fundings, son prochain token seront vus).
Limite : un wallet financé directement par un exchange reste anonyme (veille X et smart money prennent le relais).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict, deque

from . import alerts as A
from .alerte import Alert
from .analysis.tracer import Tracer
from .confiance import is_dev_role, is_service, is_upstream_role
from .smart import SMART_GROUP
from .sources import dexscreener
from .telegram import esc

log = logging.getLogger("lancements")

CHECK_AFTER_S = 180          # regardé 3 min après la création…
CHECK_UNTIL_S = 600          # … et jusqu'à 10 min (DexScreener met parfois une minute à l'indexer)
TRACTION_MC = 15_000         # ≈ 3 fois la market cap de départ d'un token pump.fun
TRACTION_TXNS = 40           # achats + ventes
TRACES_PER_HOUR = 30         # plafond de remontées (quota Helius)
MAX_HOPS = 3
RELAY_MAX_TX = 5             # un financeur avec moins de 6 tx = relais : on remonte encore
PENDING_MAX = 20_000
BON = ("référence", "prouvé", "lié")


class LaunchWatch:
    def __init__(self, pipeline):
        self.p = pipeline
        self.pending: OrderedDict[str, dict] = OrderedDict()
        self._traces: deque[float] = deque()
        self.stats = {"vus": 0, "decollent": 0, "remontes": 0, "trouves": 0}

    # --- 1. flux des nouveaux tokens ------------------------------------------------------------------
    def on_new_token(self, msg: dict) -> None:
        creator, mint = msg.get("traderPublicKey"), msg.get("mint")
        if not creator or not mint or creator in self.p.watched:
            return   # un créateur déjà suivi est traité en direct par le pipeline
        self.stats["vus"] += 1
        t = {"mint": mint, "creator": creator, "ts": time.time(), "symbol": msg.get("symbol"), "name": msg.get("name")}
        connu = self.connu(creator)
        if connu is not None:
            # Créateur déjà connu de la base (sorti de la watchlist, ou jamais suivi mais relié par un lien) :
            # alerte dès la création, sans attendre de voir s'il décolle. Recherche en base : aucun crédit.
            genre, w = connu
            self.stats["trouves"] += 1
            self.p._spawn(self.alerter({**t, "mc": None, "txns": None, "genre": genre, "wallet": w["address"],
                                        "label": w["label"], "grp": w["grp"], "role": w["role"],
                                        "depth": w["depth"], "chaine": []}))
            return
        self.pending[mint] = t
        while len(self.pending) > PENDING_MAX:
            self.pending.popitem(last=False)

    # --- 2. qui décolle ? -----------------------------------------------------------------------------
    async def check_once(self, now: float | None = None) -> list[dict]:
        now = now or time.time()
        for mint in [m for m, t in self.pending.items() if now - t["ts"] > CHECK_UNTIL_S]:
            del self.pending[mint]   # trop tard (ou jamais indexé)
        prets = [t for t in self.pending.values() if now - t["ts"] >= CHECK_AFTER_S]
        if not prets or self.p.http is None:
            return []
        marches = await dexscreener.markets(self.p.http, [t["mint"] for t in prets])
        decollent = []
        for t in prets:
            m = marches.get(t["mint"])
            if not m:
                continue   # pas encore indexé : on réessaie au prochain tour
            del self.pending[t["mint"]]
            if (m.get("mc") or 0) >= TRACTION_MC and (m.get("txns24h") or 0) >= TRACTION_TXNS:
                decollent.append({**t, "mc": m["mc"], "txns": m["txns24h"]})
        self.stats["decollent"] += len(decollent)
        trouves = []
        for t in sorted(decollent, key=lambda t: t["mc"], reverse=True):
            gratuit = self.p.toile is not None and self.p.toile.chaine(t["creator"]) is not None
            while self._traces and now - self._traces[0] > 3600:
                self._traces.popleft()
            if not gratuit and len(self._traces) >= TRACES_PER_HOUR:
                log.info("Remontées : plafond horaire atteint (%d), %s non remonté", TRACES_PER_HOUR, t["mint"][:6])
                continue
            res = await self.remonter(t)
            if res:
                trouves.append(res)
        return trouves

    # --- 3. d'où vient l'argent du créateur ? ------------------------------------------------------------
    def connu(self, adresse: str) -> tuple[str, object] | None:
        """(« bon » | « rug », ligne du wallet connu) si le radar connaît déjà ce wallet, même hors watchlist.

        Jamais pour un exchange : vu en vrai, un wallet chaud de Binance avait financé le dev d'un faux coin ; tous ses
        clients passaient pour le même réseau à rugs.
        """
        db = self.p.db
        if self.p.is_service_address(adresse):
            return None
        w = db.wallet(adresse)
        if w is not None:
            grp = w["grp"] or ""
            if grp in self.p.bad_groups():
                return "rug", w
            if grp == SMART_GROUP or self.p.trust(adresse) in BON:
                return "bon", w
        for r in db.conn.execute("SELECT w.* FROM links l JOIN wallets w ON w.address = l.dst WHERE l.src=? LIMIT 20",
                                 (adresse,)):
            # Il a déjà financé un dev connu : même opérateur
            if (r["grp"] or "") in self.p.bad_groups():
                return "rug", r
            if self.p.trust(r["address"]) in BON and (is_dev_role(r["role"]) or is_upstream_role(r["role"])):
                return "bon", r
        return None

    async def remonter(self, t: dict) -> dict | None:
        self.stats["remontes"] += 1
        creator = t["creator"]
        connu = self.connu(creator)   # dev déjà connu (sorti de la watchlist) : aucun crédit dépensé
        chaine: list[dict] = []
        toile = self.p.toile.chaine(creator) if self.p.toile is not None else None
        if connu is None and toile is not None:
            # Déjà remonté par la toile (RPC publics, même règle anti-leurre) : aucun crédit Helius
            relie = await self.p.toile.relier(creator)
            if relie is not None:
                connu, chaine = relie
        elif connu is None:
            self._traces.append(time.time())
            tracer = Tracer(self.p.rpc, self.p.cfg.hot_wallet_tx_threshold,
                            {a: lab for a, lab in self.p.labels.items() if is_service(None, lab)})
            cur = creator
            for _ in range(MAX_HOPS):
                try:
                    funding, _nb, _raison = await tracer.first_funding(cur)
                except Exception as e:
                    log.debug("Remontée de %s impossible : %s", cur[:6], e)
                    break
                if not funding:
                    break
                src = funding["source"]
                chaine.append({"src": src, "sol": funding["amount"], "leurre": funding.get("leurre")})
                if is_service(self.p.db.wallet(src), self.p.db.get_label(src)):
                    break   # financé par un exchange : anonyme
                connu = self.connu(src)
                if connu is not None:
                    break
                # Relais (quelques tx) : on remonte encore ; sinon c'est un wallet inconnu, on s'arrête
                if len(await self.p.rpc.signatures(src, limit=RELAY_MAX_TX + 1)) > RELAY_MAX_TX:
                    break
                cur = src
        if connu is None:
            return None
        self.stats["trouves"] += 1
        genre, w = connu
        res = {**t, "genre": genre, "wallet": w["address"], "label": w["label"], "grp": w["grp"], "role": w["role"],
               "depth": w["depth"], "chaine": chaine}
        log.info("Lancement %s ($%s) : créateur relié à %s (%s, %s)", t["mint"][:6], t["symbol"], w["label"], w["grp"],
                 genre)
        await self.alerter(res)
        return res

    # --- 4. alerte et suivi du créateur -------------------------------------------------------------------
    async def alerter(self, r: dict) -> None:
        p = self.p
        info = await p._info(r["mint"], r["creator"])
        rug = r["genre"] == "rug"
        via = " ← ".join([f"{A.short(r['creator'])} (créateur)"]
                         + [f"{h['sol']:g} SOL de {esc(p.label(h['src']) or A.short(h['src']))}"
                            + (" 🪤 leurre écarté" if h.get("leurre") else "") for h in r["chaine"]])
        lignes = [f"🔗 {via}" if r["chaine"] else "🔗 le créateur est lui-même un wallet déjà connu",
                  f"👤 Relié à <b>{esc(r['label'] or A.short(r['wallet']))}</b> · groupe {esc(r['grp'] or '?')}"
                  + (f"\n   <i>{esc(r['role'])}</i>" if r["role"] else ""),
                  (f"📈 3 à 10 min après la création : {A.usd(r['mc'])} · {r['txns']} échanges" if r.get("mc")
                   else "⚡ repéré dès la création du token")]
        flags = p.rug_flags(r["wallet"], info.creator)
        titre = "⛔ <b>UN RÉSEAU À RUGS RELANCE" if rug else "🔴 <b>NOUVEAU WALLET D'UN DEV CONNU"
        texte = A.card(f"{titre}</b>", info, flags, lignes, A.token_block(info, []) + p._announcement_line(info))
        confiance = not rug and p.trust(r["wallet"]) in ("référence", "prouvé")
        p.emit(Alert(f"relance:{r['mint']}", "relance", texte, A.token_buttons(info, r["creator"]),
                     top_title="UN DEV CONNU RELANCE" if confiance else "",
                     top_why=f"Le créateur (wallet neuf) est financé par <b>{esc(r['label'] or '')}</b> "
                             f"({esc(r['grp'] or '')}), repéré par la remontée de son argent",
                     info=info, flags=flags, wallet=r["wallet"]))
        # Le créateur est désormais suivi (ventes, retour des profits, prochain token)
        role = f"financé par {r['label'] or r['wallet'][:6]} (relance repérée sur ${r['symbol'] or '?'})"
        await p.watch(r["creator"], f"NEW_{r['creator'][:4]}", r["grp"], role,
                      min((r["depth"] or 0) + 1, p.cfg.trace_max_hops), r["wallet"])

    def status_line(self) -> str:
        s = self.stats
        return (f"🚀 Lancements pump.fun vus : {s['vus']} · qui décollent : {s['decollent']} · remontés : "
                f"{s['remontes']} · reliés à un wallet connu : {s['trouves']}")

    async def loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                await self.check_once()
            except Exception:
                log.exception("Lancements : passage en échec")
