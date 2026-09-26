"""🕸 Réseau d'un dev : tous ses wallets, tous ses projets, et ce qu'ils sont devenus.

Deux niveaux :
  - quick()  : à chaque alerte (quelques secondes). Le créateur, son financeur et les wallets déjà reliés
               à ce financeur, leurs projets pump.fun et leur sort. Sert au verdict : un nouveau wallet
               financé par un opérateur qui vide tous ses tokens en moins d'une minute est bloqué.
  - build()  : à la demande (/reseau). La toile complète : financement en amont hop par hop, wallets
               frères, wallets financés en aval (par le dev ET par son financeur), projets de chacun,
               vitesse à laquelle le dev revend, et un fichier HTML interactif.

Commande de test :
    python -m radar.analysis.network <adresse>   (écrit data/reseaux/<adresse>.html)
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import statistics
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from html import escape

from ..sources import pumpfun
from .classify import token_deltas
from .tracer import Tracer, save_result

log = logging.getLogger("network")

FLASH_S = 60               # ATH moins d'1 min après la création, puis −90 % = vidé d'un coup (achat groupé revendu)
SUCCESS_ATH = 1_000_000    # un projet qui a dépassé 1 M$
RECENT_S = 30 * 60         # trop récent pour juger
QUICK_CACHE_S = 3600
MAX_WALLETS = 40
MAX_PROJECT_WALLETS = 25   # wallets dont on liste les projets pump.fun
SELL_TX = 40               # premières transactions lues pour mesurer la revente du dev
SELL_PROJECTS = 3
RUG_DROP = 0.99            # −99 % depuis un ATH d'au moins 100 k$ = liquidité retirée
CHAIN_HOPS = 3             # remontée du financement du créateur
BIG_FUNDING_SOL = 50       # financement massif d'un wallet de dev (vu en vrai : 100 SOL relayés 2 fois)
SAME_AMOUNT = 0.01         # ±1 % = « même montant » d'un relais à l'autre


@dataclass
class Project:
    mint: str
    symbol: str | None
    creator: str
    created: int
    ath: float | None
    ath_ts: int | None
    mc: float | None
    last_trade: int | None
    complete: bool | None
    verdict: str = ""
    dev_sell_s: int | None = None      # secondes entre la création et la 1re vente du créateur
    dev_sold_pct: float | None = None  # part revendue de ce que le créateur avait acheté


def classify_project(c: dict, now: float | None = None) -> Project:
    now = now or time.time()
    p = Project(c.get("mint") or "", c.get("symbol"), c.get("creator") or "", c.get("created") or 0, c.get("ath"),
                c.get("ath_ts"), c.get("mc"), c.get("last_trade"), c.get("complete"))
    age = now - p.created if p.created else None
    drop = (1 - (p.mc or 0) / p.ath) if p.ath else None
    if age is not None and age < RECENT_S:
        p.verdict = "récent"
    elif drop is not None and drop >= RUG_DROP and (p.ath or 0) >= 100_000:
        # Vu en vrai (cluster Reserve) : 12 à 46 M$ d'ATH puis 2 k$ = liquidité retirée. Un gros ATH suivi
        # d'une chute de −99 % n'est pas un succès, c'est un rug (le « succès » était fabriqué).
        p.verdict = "rug"
    elif (p.ath or 0) >= SUCCESS_ATH or (p.complete and (p.mc or 0) >= 100_000):
        p.verdict = "succès"
    elif p.ath_ts and p.created and p.ath_ts - p.created <= FLASH_S and drop is not None and drop >= 0.5:
        # Un token pump.fun démarre vers 4-5 k$ : un achat groupé revendu le ramène à ce plancher,
        # soit « seulement » −50 à −90 % (vu en vrai : ATH 9 k$ en 3 s, puis 3 k$).
        p.verdict = "vidé"
    elif (drop is not None and drop >= 0.9) or ((p.mc or 0) < 6_000 and age is not None and age > 3600):
        p.verdict = "mort"
    else:
        p.verdict = "vivant"
    return p


@dataclass
class Report:
    seed: str
    wallets: dict[str, dict] = field(default_factory=dict)   # adresse -> {label, role}
    edges: list[dict] = field(default_factory=list)          # {src, dst, kind, sol, ts}
    projects: list[Project] = field(default_factory=list)
    built: int = 0
    deep: bool = False
    stop: str = ""
    funding_chain: list[dict] = field(default_factory=list)   # du créateur vers l'amont : {src, sol, hot}

    def relay_chain(self) -> list[float]:
        """Montants d'une chaîne de relais au même montant (0,1 → 0,1 → CEX ; 100 → 100 → bank), sinon []."""
        montants = [h["sol"] for h in self.funding_chain if h.get("sol")]
        for i in range(len(montants) - 1):
            a, b = montants[i], montants[i + 1]
            if a >= 0.05 and abs(a - b) <= SAME_AMOUNT * max(a, b):
                return [a, b]
        return []

    def big_funding(self) -> float | None:
        vus = [h["sol"] for h in self.funding_chain[:2] if not h.get("hot") and h.get("sol")]
        return max(vus) if vus and max(vus) >= BIG_FUNDING_SOL else None

    @property
    def evaluated(self) -> list[Project]:
        return [p for p in self.projects if p.verdict != "récent"]

    def counts(self) -> dict[str, int]:
        out = {k: 0 for k in ("succès", "vivant", "mort", "vidé", "rug", "récent")}
        for p in self.projects:
            out[p.verdict] = out.get(p.verdict, 0) + 1
        return out

    def relaunched(self) -> list[str]:
        """Tickers lancés au moins 2 fois (un vrai projet ne relance pas son nom ; une arnaque en série, si)."""
        vus: dict[str, set[str]] = {}
        for p in self.projects:
            k = "".join(ch for ch in (p.symbol or "").upper() if ch.isalnum())
            if k:
                vus.setdefault(k, set()).add(p.mint)   # tokens distincts
        return [f"${k} ×{len(m)}" for k, m in vus.items() if len(m) >= 2]

    def dev_sell_median(self) -> int | None:
        vals = [p.dev_sell_s for p in self.projects if p.dev_sell_s is not None]
        return int(statistics.median(vals)) if vals else None

    def verdict(self) -> tuple[str, str | None]:
        """(verdict lisible, drapeau rouge éventuel)."""
        ev = self.evaluated
        n, c = len(ev), self.counts()
        mauvais = c["vidé"] + c["rug"]
        if n >= 1 and mauvais / n >= 0.5:   # même un seul projet, s'il a été vidé ou rug
            return ("⛔ réseau à rugs en série",
                    f"réseau à rugs : {mauvais}/{n} projets du dev et de ses wallets rug (−99 %) ou vidés en moins d'1 min")
        relances = self.relaunched()
        if relances:
            return ("⛔ relance plusieurs fois le même nom",
                    "réseau à rugs : relance le même nom plusieurs fois (" + ", ".join(relances) + ") : arnaque en série")
        vente = self.dev_sell_median()
        if vente is not None and vente <= 120 and len([p for p in self.projects if p.dev_sell_s is not None]) >= 2:
            return ("⛔ le dev revend en quelques secondes",
                    f"réseau à rugs : le dev revend en moyenne {vente} s après la création")
        relais, massif = self.relay_chain(), self.big_funding()
        if relais and massif:
            return ("⛔ schéma des faux fonds souverains",
                    f"réseau à rugs : dev financé {massif:g} SOL via une chaîne de relais au même montant "
                    "(schéma du cluster Reserve)")
        if relais:
            return ("🟠 financement brouillé",
                    f"financement brouillé : chaîne de relais au même montant ({relais[0]:g} SOL → {relais[1]:g} SOL)")
        if n >= 5 and c["succès"] == 0 and (c["mort"] + c["vidé"]) / n >= 0.9:
            return ("🟠 réseau sans aucun succès", f"réseau sans aucun succès : {n} projets, tous morts")
        if c["succès"]:
            return (f"✅ {c['succès']} succès dans le réseau", None)
        if n == 0:
            return ("🆕 aucun projet passé connu", None)
        return ("🟡 historique mitigé", None)

    def summary_line(self) -> str:
        c = self.counts()
        aths = [p.ath for p in self.projects if p.ath]
        bits = [f"{len(self.wallets)} wallet(s)", f"{len(self.projects)} projet(s)"]
        if self.projects:
            bits.append(f"✅ {c['succès']} · 💀 {c['mort']} · 🪤 {c['rug']} rugs · ⚡ {c['vidé']} vidés < 1 min")
        if aths:
            bits.append(f"ATH max {_usd(max(aths))}")
        return "🕸 Réseau du dev : " + " · ".join(bits)

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d)

    @classmethod
    def from_json(cls, s: str) -> "Report":
        d = json.loads(s)
        projets = []
        for p in d.get("projects", []):
            # Le verdict est recalculé : les règles évoluent (ex. « rug » ajouté après avoir vu le cluster Reserve)
            q = classify_project(p)
            q.dev_sell_s, q.dev_sold_pct = p.get("dev_sell_s"), p.get("dev_sold_pct")
            projets.append(q)
        d["projects"] = projets
        return cls(**d)


def _usd(v: float | None) -> str:
    if v is None:
        return "?"
    return f"{v / 1e6:.1f} M$" if v >= 1e6 else f"{v / 1e3:.1f} k$" if v >= 1e3 else f"{v:.0f} $"


def _short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}" if len(a) > 10 else a


# --- collecte -----------------------------------------------------------------------------------
_quick_cache: dict[str, tuple[float, Report]] = {}


def _known_services(pipeline) -> dict[str, str]:
    return {a: lab for a, lab in pipeline.labels.items() if "hot wallet" in lab.lower() or "exchange" in lab.lower()}


def _add_wallet(rep: Report, pipeline, addr: str, role: str) -> None:
    if addr and addr not in rep.wallets and len(rep.wallets) < MAX_WALLETS:
        rep.wallets[addr] = {"label": pipeline.label(addr) or _short(addr), "role": role}


async def _projects(http, wallets: list[str]) -> list[Project]:
    sem = asyncio.Semaphore(4)

    async def un(w: str) -> list[Project]:
        async with sem:
            coins = await pumpfun.coins_by_creator(http, w)
        return [classify_project({**c, "creator": c.get("creator") or w}) for c in coins or [] if c.get("mint")]

    vus: dict[str, Project] = {}
    for lot in await asyncio.gather(*(un(w) for w in wallets), return_exceptions=True):
        if isinstance(lot, list):
            for p in lot:
                vus.setdefault(p.mint, p)
    return sorted(vus.values(), key=lambda p: -p.created)


async def quick(pipeline, creator: str) -> Report:
    """Version rapide pour les alertes : créateur + financeur + wallets déjà reliés au financeur."""
    hit = _quick_cache.get(creator)
    if hit and time.time() - hit[0] < QUICK_CACHE_S:
        return hit[1]
    saved = pipeline.db.get_network(creator)
    if saved and time.time() - saved[1] < QUICK_CACHE_S * 6:
        rep = Report.from_json(saved[0])
        _quick_cache[creator] = (time.time(), rep)
        return rep
    rep = Report(creator, built=int(time.time()))
    _add_wallet(rep, pipeline, creator, "créateur")
    tracer = Tracer(pipeline.rpc, pipeline.cfg.hot_wallet_tx_threshold, _known_services(pipeline))
    # Remontée du financement sur quelques hops : révèle les chaînes de relais et les gros financements
    cur, premier, vus = creator, None, {creator}
    for _ in range(CHAIN_HOPS):
        funding, _nb, _r = await tracer.first_funding(cur)
        if not funding or funding["source"] in vus:
            break
        src = funding["source"]
        vus.add(src)
        hot, info = await tracer.hot_check(src, funding["signature"])
        rep.funding_chain.append({"src": src, "sol": funding["amount"], "hot": hot})
        rep.edges.append({"src": src, "dst": cur, "kind": "financement", "sol": funding["amount"], "ts": funding["ts"]})
        if hot:
            rep.wallets[src] = {"label": pipeline.label(src) or f"exchange ({info})", "role": "exchange"}
            break
        _add_wallet(rep, pipeline, src, "financeur" if premier is None else "financeur en amont")
        premier = premier or src
        cur = src
    if premier:
        for r in pipeline.db.conn.execute(
                "SELECT dst, kind, amount, ts FROM links WHERE src=? AND dst!=? ORDER BY ts DESC LIMIT 15",
                (premier, creator)):
            _add_wallet(rep, pipeline, r["dst"], "frère" if r["kind"] == "frère" else "financé par le même wallet")
            rep.edges.append({"src": premier, "dst": r["dst"], "kind": r["kind"], "sol": r["amount"], "ts": r["ts"]})
    humains = [a for a, w in rep.wallets.items() if w["role"] != "exchange"][:MAX_PROJECT_WALLETS]
    rep.projects = await _projects(pipeline.http, humains)
    _quick_cache[creator] = (time.time(), rep)
    return rep


async def dev_sell_speed(rpc, mint: str, creator: str, created: int) -> tuple[int | None, float | None]:
    """(secondes avant la 1re vente du créateur, % revendu de ce qu'il a acheté) sur les 1res transactions."""
    sigs = await rpc.signatures(mint, limit=1000)
    before = sigs[-1]["signature"] if len(sigs) == 1000 else None
    for _ in range(2):  # remonte vers les plus anciennes
        if not before:
            break
        page = await rpc.signatures(mint, before=before, limit=1000)
        if not page:
            break
        sigs = page
        before = page[-1]["signature"] if len(page) == 1000 else None
    premieres = [s for s in reversed(sigs) if s.get("err") is None][:SELL_TX]
    achete = vendu = 0
    premiere_vente = None
    for s in premieres:
        tx = await rpc.transaction(s["signature"])
        if not tx:
            continue
        d = token_deltas(tx).get((creator, mint))
        if not d:
            continue
        avant, apres, _dec = d
        if apres > avant:
            achete += apres - avant
        elif apres < avant:
            vendu += avant - apres
            if premiere_vente is None:
                premiere_vente = max(0, (tx.get("blockTime") or created) - created)
    return premiere_vente, (round(100 * vendu / achete, 0) if achete else None)


async def build(pipeline, seed: str) -> Report:
    """Toile complète (à la demande) : amont, frères, aval, projets, revente du dev."""
    rep = Report(seed, built=int(time.time()), deep=True)
    _add_wallet(rep, pipeline, seed, "départ")
    rpc, db = pipeline.rpc, pipeline.db
    known = _known_services(pipeline)
    tracer = Tracer(rpc, pipeline.cfg.hot_wallet_tx_threshold, known)

    # 1. En amont : financement hop par hop + wallets frères (même source, même minute)
    res = await tracer.trace(seed, 2, with_siblings=True)
    save_result(res, db, False, pipeline.cfg.trace_max_hops)
    rep.stop = res.stop_reason
    rep.funding_chain = [{"src": h.source, "sol": h.amount, "hot": h.source_hot} for h in res.hops]
    banks: list[str] = []
    for h in res.hops:
        if h.source_hot:
            rep.wallets[h.source] = {"label": pipeline.label(h.source) or f"exchange ({h.source_hot_info})",
                                     "role": "exchange"}
        else:
            _add_wallet(rep, pipeline, h.source, "relais" if h.is_relay else "financeur")
            banks.append(h.source)
        rep.edges.append({"src": h.source, "dst": h.address, "kind": "relais" if h.is_relay else "financement",
                          "sol": h.amount, "ts": h.ts})
        for s in h.siblings[:15]:
            _add_wallet(rep, pipeline, s.address, "frère" if s.same_amount else "financé par le même wallet")
            if s.address in rep.wallets:
                rep.edges.append({"src": h.source, "dst": s.address, "kind": "frère" if s.same_amount else "sortie",
                                  "sol": s.amount, "ts": s.ts})

    # 2. En aval : wallets frais financés par le dev, puis par son premier financeur (l'opérateur)
    from ..devs import funded_wallets
    for origine, role in [(seed, "financé par le dev")] + [(b, "financé par le même wallet") for b in banks[:1]]:
        try:
            for addr, sol in await funded_wallets(rpc, origine, max_tx=150, days=30):
                _add_wallet(rep, pipeline, addr, role)
                if addr in rep.wallets:
                    rep.edges.append({"src": origine, "dst": addr, "kind": "financement", "sol": sol, "ts": 0})
        except Exception as e:
            log.debug("wallets financés par %s : %s", origine[:6], e)

    # 3. Ce que le radar sait déjà : wallets qu'il a vus financés par un wallet de la toile (ex. les petits
    #    acheteurs d'un faux coin, même mis en veille), puis les liens des traçages précédents
    for a in list(rep.wallets):
        for r in db.conn.execute("SELECT address, role FROM wallets WHERE parent=? LIMIT 30", (a,)):
            _add_wallet(rep, pipeline, r["address"], "financé par le dev" if a == seed else "financé par le même wallet")
            if r["address"] in rep.wallets:
                rep.edges.append({"src": a, "dst": r["address"], "kind": "financement", "sol": 0, "ts": 0})
    for a in list(rep.wallets):
        for r in db.conn.execute("SELECT src, dst, kind, amount, ts FROM links WHERE src=? OR dst=? LIMIT 20", (a, a)):
            if r["src"] in rep.wallets and r["dst"] in rep.wallets:
                rep.edges.append({"src": r["src"], "dst": r["dst"], "kind": r["kind"], "sol": r["amount"], "ts": r["ts"]})

    # 4. Projets de chaque wallet + vitesse de revente du créateur sur les plus récents
    humains = [a for a, w in rep.wallets.items() if w["role"] != "exchange"][:MAX_PROJECT_WALLETS]
    rep.projects = await _projects(pipeline.http, humains)
    for p in [p for p in rep.projects if p.verdict != "récent"][:SELL_PROJECTS]:
        try:
            p.dev_sell_s, p.dev_sold_pct = await dev_sell_speed(rpc, p.mint, p.creator, p.created)
        except Exception as e:
            log.debug("revente du dev sur %s : %s", p.mint[:6], e)
    # dédoublonnage des liens
    vus, uniques = set(), []
    for e in rep.edges:
        k = (e["src"], e["dst"], e["kind"])
        if k not in vus:
            vus.add(k)
            uniques.append(e)
    rep.edges = uniques
    db.put_network(seed, rep.to_json())
    _quick_cache[seed] = (time.time(), rep)
    return rep


def quick_verdict(rep: Report) -> str | None:
    return rep.verdict()[1]


def annotate(info, rep: Report) -> None:
    """Ajoute la ligne « réseau » et l'éventuel drapeau rouge à la fiche d'un token."""
    info.network = rep.summary_line() + f" · {rep.verdict()[0]}"
    flag = rep.verdict()[1]
    if flag and flag not in info.flags:
        info.flags.append(flag)


# --- rendu Telegram ------------------------------------------------------------------------------
VERDICT_ICON = {"succès": "✅", "vivant": "🟢", "mort": "💀", "vidé": "⚡", "rug": "🪤", "récent": "🆕"}


def telegram_text(rep: Report, label: str) -> str:
    c = rep.counts()
    verdict, flag = rep.verdict()
    aths = [p.ath for p in rep.projects if p.ath]
    roles: dict[str, int] = {}
    for w in rep.wallets.values():
        roles[w["role"]] = roles.get(w["role"], 0) + 1
    lines = [f"🕸 <b>RÉSEAU DE {escape(label)}</b>", f"<b>{escape(verdict)}</b>", "───────────────",
             f"👛 {len(rep.wallets)} wallets : " + ", ".join(f"{n} {escape(r)}" for r, n in
                                                          sorted(roles.items(), key=lambda x: -x[1])),
             f"🧬 {len(rep.projects)} projets : ✅ {c['succès']} succès · 🟢 {c['vivant']} vivants · 💀 {c['mort']} morts "
             f"· 🪤 {c['rug']} rugs (−99 %) · ⚡ {c['vidé']} vidés en moins d'1 min"
             + (f" · 🆕 {c['récent']} récents" if c["récent"] else "")]
    if aths:
        lines.append(f"🏆 ATH max du réseau : {_usd(max(aths))}")
    vente = rep.dev_sell_median()
    if vente is not None:
        lines.append(f"💸 Le dev revend en moyenne <b>{vente} s</b> après la création "
                     f"({len([p for p in rep.projects if p.dev_sell_s is not None])} projets analysés)")
    if rep.funding_chain:
        lines.append("💰 Financement : dev ⟵ " + " ⟵ ".join(
            f"{h['sol']:g} SOL ⟵ " + escape(rep.wallets.get(h["src"], {}).get("label", _short(h["src"])))
            for h in rep.funding_chain))
    exch = [w["label"] for w in rep.wallets.values() if w["role"] == "exchange"]
    if exch:
        lines.append(f"🏦 Argent venu de : {escape(', '.join(exch[:3]))}")
    if flag:
        lines.append(f"🚩 {escape(flag)}")
    if rep.projects:
        lines.append("───────────────")
        lines.append("<b>Derniers projets</b>")
        for p in rep.projects[:8]:
            quand = datetime.fromtimestamp(p.created).strftime("%d/%m") if p.created else "?"
            extra = f" · dev a revendu à {p.dev_sell_s} s" if p.dev_sell_s is not None else ""
            lines.append(f"{VERDICT_ICON.get(p.verdict, '•')} ${escape(p.symbol or '?')} · {quand} · ATH {_usd(p.ath)}"
                         f" → {escape(p.verdict)}{extra}")
    lines.append("<i>Toile interactive complète dans le fichier ci-dessous.</i>" if rep.deep else "")
    return "\n".join(x for x in lines if x)


# --- toile HTML (autonome, sans dépendance) -------------------------------------------------------
COLORS = {"départ": "#e0a100", "créateur": "#e0a100", "financeur": "#8a4dd6", "financeur en amont": "#6d3bb3",
          "relais": "#b48ee8",
          "frère": "#2f7de1", "financé par le même wallet": "#5aa9f0", "financé par le dev": "#13a3a3",
          "exchange": "#8a8f98"}
PROJECT_COLORS = {"succès": "#1f9d55", "vivant": "#63b37a", "mort": "#5b5f66", "vidé": "#d64545", "rug": "#a01b1b",
                  "récent": "#e08a1f"}


def _layout(nodes: list[str], edges: list[tuple[str, str]], w: float = 1000, h: float = 700) -> dict[str, tuple[float, float]]:
    """Placement « ressorts » (Fruchterman-Reingold) : les éléments liés se rapprochent."""
    rnd = random.Random(42)
    pos = {n: [rnd.uniform(0, w), rnd.uniform(0, h)] for n in nodes}
    if len(nodes) < 2:
        return {n: (w / 2, h / 2) for n in nodes}
    k = math.sqrt(w * h / len(nodes)) * 0.75
    temp = w / 8
    for _ in range(250):
        disp = {n: [0.0, 0.0] for n in nodes}
        for i, a in enumerate(nodes):
            for b in nodes[i + 1:]:
                dx, dy = pos[a][0] - pos[b][0], pos[a][1] - pos[b][1]
                d = max(math.hypot(dx, dy), 0.01)
                f = k * k / d
                disp[a][0] += dx / d * f
                disp[a][1] += dy / d * f
                disp[b][0] -= dx / d * f
                disp[b][1] -= dy / d * f
        for a, b in edges:
            if a not in pos or b not in pos:
                continue
            dx, dy = pos[a][0] - pos[b][0], pos[a][1] - pos[b][1]
            d = max(math.hypot(dx, dy), 0.01)
            f = d * d / k
            disp[a][0] -= dx / d * f
            disp[a][1] -= dy / d * f
            disp[b][0] += dx / d * f
            disp[b][1] += dy / d * f
        for n in nodes:
            dx, dy = disp[n]
            d = max(math.hypot(dx, dy), 0.01)
            pos[n][0] = min(w - 30, max(30, pos[n][0] + dx / d * min(d, temp)))
            pos[n][1] = min(h - 30, max(30, pos[n][1] + dy / d * min(d, temp)))
        temp *= 0.97
    return {n: (p[0], p[1]) for n, p in pos.items()}


def to_html(rep: Report, label: str) -> str:
    projets = rep.projects[:40]
    liens = [(e["src"], e["dst"]) for e in rep.edges] + [(p.creator, p.mint) for p in projets]
    noeuds = list(rep.wallets) + [p.mint for p in projets if p.mint not in rep.wallets]
    pos = _layout(noeuds, liens)
    svg = []
    for e in rep.edges:
        if e["src"] in pos and e["dst"] in pos:
            (x1, y1), (x2, y2) = pos[e["src"]], pos[e["dst"]]
            dash = ' stroke-dasharray="5 4"' if e["kind"] in ("frère", "sortie") else ""
            svg.append(f'<line x1="{x1:.0f}" y1="{y1:.0f}" x2="{x2:.0f}" y2="{y2:.0f}" class="edge"{dash} '
                       f'marker-end="url(#fleche)"><title>{escape(e["kind"])} · {e["sol"] or 0:g} SOL</title></line>')
            if e.get("sol"):
                svg.append(f'<text x="{(x1 + x2) / 2:.0f}" y="{(y1 + y2) / 2 - 3:.0f}" class="sol">{e["sol"]:g} SOL</text>')
    for p in projets:
        if p.creator in pos and p.mint in pos:
            (x1, y1), (x2, y2) = pos[p.creator], pos[p.mint]
            svg.append(f'<line x1="{x1:.0f}" y1="{y1:.0f}" x2="{x2:.0f}" y2="{y2:.0f}" class="cree"/>')
    for a, w in rep.wallets.items():
        x, y = pos[a]
        r = 13 if a == rep.seed else 9
        svg.append(f'<a href="https://solscan.io/account/{a}" target="_blank"><circle cx="{x:.0f}" cy="{y:.0f}" r="{r}" '
                   f'fill="{COLORS.get(w["role"], "#5aa9f0")}" class="node"><title>{escape(w["label"])} · '
                   f'{escape(w["role"])}\n{a}</title></circle></a>'
                   f'<text x="{x:.0f}" y="{y + r + 12:.0f}" class="lab">{escape(w["label"][:14])}</text>')
    for p in projets:
        if p.mint not in pos:
            continue
        x, y = pos[p.mint]
        svg.append(f'<a href="https://pump.fun/coin/{p.mint}" target="_blank"><rect x="{x - 8:.0f}" y="{y - 8:.0f}" '
                   f'width="16" height="16" rx="3" fill="{PROJECT_COLORS.get(p.verdict, "#999")}" class="node">'
                   f'<title>${escape(p.symbol or "?")} · {escape(p.verdict)} · ATH {_usd(p.ath)}</title></rect></a>'
                   f'<text x="{x:.0f}" y="{y + 20:.0f}" class="lab">${escape((p.symbol or "?")[:10])}</text>')
    lignes_projets = "".join(
        f"<tr><td><span class='pastille' style='background:{PROJECT_COLORS.get(p.verdict, '#999')}'></span>"
        f"{escape(p.verdict)}</td><td><a href='https://pump.fun/coin/{p.mint}' target='_blank'>${escape(p.symbol or '?')}</a>"
        f"</td><td>{datetime.fromtimestamp(p.created).strftime('%d/%m/%y %H:%M') if p.created else '?'}</td>"
        f"<td>{_usd(p.ath)}</td><td>{_usd(p.mc)}</td><td>{f'{p.ath_ts - p.created} s' if p.ath_ts and p.created else '?'}</td>"
        f"<td>{f'{p.dev_sell_s} s' if p.dev_sell_s is not None else '—'}</td>"
        f"<td><a href='https://solscan.io/account/{p.creator}' target='_blank'>{escape(rep.wallets.get(p.creator, {}).get('label', _short(p.creator)))}</a></td></tr>"
        for p in rep.projects)
    lignes_wallets = "".join(
        f"<tr><td><span class='pastille' style='background:{COLORS.get(w['role'], '#5aa9f0')}'></span>{escape(w['role'])}"
        f"</td><td>{escape(w['label'])}</td><td><a href='https://solscan.io/account/{a}' target='_blank'><code>{a}</code></a></td></tr>"
        for a, w in rep.wallets.items())
    verdict, flag = rep.verdict()
    c = rep.counts()
    legende = "".join(f"<span><i class='pastille' style='background:{col}'></i>{escape(r)}</span>"
                      for r, col in list(COLORS.items())[1:]) + "".join(
        f"<span><i class='pastille carre' style='background:{col}'></i>projet {escape(v)}</span>"
        for v, col in PROJECT_COLORS.items())
    return f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Réseau {escape(label)}</title>
<style>
:root{{--bg:#fafafa;--fg:#1c1f24;--muted:#6b7280;--card:#fff;--line:#c7ccd4;--edge:#9aa3af}}
@media (prefers-color-scheme:dark){{:root{{--bg:#111316;--fg:#e8eaed;--muted:#9aa0a6;--card:#1a1d21;--line:#2c3036;--edge:#5f6670}}}}
body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}}
main{{max-width:1100px;margin:0 auto;padding:16px}} h1{{font-size:20px;margin:0 0 4px}}
.verdict{{font-weight:600;margin:4px 0 12px}} .stats{{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:12px}}
.stat{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:8px 12px}}
.stat b{{display:block;font-size:20px}} svg{{width:100%;height:auto;background:var(--card);border:1px solid var(--line);border-radius:8px}}
.edge{{stroke:var(--edge);stroke-width:1.4}} .cree{{stroke:var(--edge);stroke-width:1;opacity:.5}}
.sol{{fill:var(--muted);font-size:10px;text-anchor:middle}} .lab{{fill:var(--fg);font-size:10px;text-anchor:middle}}
.node{{stroke:var(--card);stroke-width:2}} .legende{{display:flex;flex-wrap:wrap;gap:10px;margin:10px 0;color:var(--muted);font-size:13px}}
.pastille{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px}} .carre{{border-radius:2px}}
.table{{overflow-x:auto}} table{{border-collapse:collapse;width:100%;margin:8px 0 20px;font-size:13px}}
td,th{{border-bottom:1px solid var(--line);padding:6px 8px;text-align:left;white-space:nowrap}} a{{color:#2f7de1}}
code{{font-size:12px}} .flag{{color:#d64545}}
</style></head><body><main>
<h1>🕸 Réseau de {escape(label)}</h1>
<div class="verdict">{escape(verdict)}</div>{f'<div class="flag">🚩 {escape(flag)}</div>' if flag else ''}
<div class="stats"><div class="stat"><b>{len(rep.wallets)}</b>wallets</div><div class="stat"><b>{len(rep.projects)}</b>projets</div>
<div class="stat"><b>{c['succès']}</b>succès</div><div class="stat"><b>{c['mort']}</b>morts</div>
<div class="stat"><b>{c['rug']}</b>rugs (−99 %)</div><div class="stat"><b>{c['vidé']}</b>vidés &lt; 1 min</div>
<div class="stat"><b>{rep.dev_sell_median() if rep.dev_sell_median() is not None else '—'}</b>s avant revente du dev</div></div>
<svg viewBox="0 0 1000 700" role="img" aria-label="Toile du réseau">
<defs><marker id="fleche" viewBox="0 0 10 10" refX="16" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
<path d="M0,0 L10,5 L0,10 z" fill="var(--edge)"/></marker></defs>{''.join(svg)}</svg>
<div class="legende">{legende}</div>
<p style="color:var(--muted);font-size:13px">Flèche = argent envoyé (SOL) · pointillés = wallets frères (même source, même minute) ·
carré = projet pump.fun. Clique sur un élément pour l'ouvrir. Arrêt de la remontée : {escape(rep.stop or '—')}.</p>
<h2>Projets</h2><div class="table"><table><tr><th>Sort</th><th>Token</th><th>Créé</th><th>ATH</th><th>MC</th>
<th>ATH après</th><th>Revente dev</th><th>Créateur</th></tr>{lignes_projets}</table></div>
<h2>Wallets</h2><div class="table"><table><tr><th>Rôle</th><th>Nom</th><th>Adresse</th></tr>{lignes_wallets}</table></div>
<p style="color:var(--muted);font-size:12px">Généré le {datetime.now().strftime('%d/%m/%Y %H:%M')} par Memecoin Radar ·
alerte uniquement, pas un conseil.</p></main></body></html>"""


# --- test en ligne de commande ------------------------------------------------------------------
def main() -> int:
    import sys
    from pathlib import Path

    import aiohttp

    from .. import config as cfgmod
    from ..db import DB
    from ..pipeline import Pipeline
    from ..sources.helius import SolanaRPC

    if len(sys.argv) < 2:
        print("Usage : python -m radar.analysis.network <adresse>")
        return 1
    seed = sys.argv[1]
    cfgmod.setup_logging("network")
    cfg = cfgmod.load()

    async def go() -> int:
        db = DB(cfg.db_path)
        async with SolanaRPC(cfg.rpc_url) as rpc, aiohttp.ClientSession() as http:
            p = Pipeline(cfg, db, rpc, http, None, None, dry_run=True)
            rep = await build(p, seed)
            label = p.label(seed) or _short(seed)
        out = Path(cfg.db_path).parent / "reseaux" / f"{seed}.html"
        out.parent.mkdir(exist_ok=True)
        out.write_text(to_html(rep, label), encoding="utf-8")
        import re
        print(re.sub("<[^>]+>", "", telegram_text(rep, label)))
        print(f"\nToile : {out}")
        db.close()
        return 0

    return asyncio.run(go())


if __name__ == "__main__":
    raise SystemExit(main())
