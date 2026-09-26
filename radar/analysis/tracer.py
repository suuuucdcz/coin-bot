"""Remontée du funding d'une adresse, hop par hop, + wallets frères (cluster).

Utilisation :
    python -m radar.analysis.tracer <adresse> [--hops N] [--ajouter] [--json]

Méthode (voir CLAUDE.md) :
- plus ancienne transaction réussie de l'adresse qui lui apporte du SOL ;
- la source = le compte dont le solde SOL baisse le plus (pas seulement les « system transfer ») ;
- si la source fait plus de HOT_WALLET_TX_THRESHOLD tx en quelques minutes -> exchange/service, on s'arrête ;
- les relais (wallets à ≤ 3 tx) sont suivis jusqu'au bout et ne comptent pas dans la profondeur ;
- frères = autres sorties de la source dans la même fenêtre de temps (même montant = très suspect) ;
- les transferts ≤ 0,002 SOL (address poisoning) sont ignorés.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import aiohttp

from .. import config as cfgmod
from ..db import DB
from ..sources.helius import RpcError, SolanaRPC, sol_deltas
from ..sources.pumpfun import coins_by_creator

log = logging.getLogger("tracer")

DUST_SOL = 0.002             # en dessous : bruit / address poisoning
MIN_SIBLING_SOL = 0.005      # au-dessus du loyer d'un compte de token (~0,00204 SOL)
RELAY_MAX_TX = 5             # un wallet avec si peu de tx = relais probable
SIBLING_WINDOW_S = 120       # ± 2 min autour du funding
SAME_AMOUNT_TOLERANCE = 0.05 # ± 5 % = « même montant »
# N tx en moins de 4 h = hot wallet / service. Vu en pratique : is6M… (source du seed
# « Reserve ») = 1 000 tx en 2 h 30, avec des montants variés vers des wallets sans lien.
HOT_WINDOW_S = 4 * 3600
MAX_TOTAL_STEPS = 25         # garde-fou (relais compris)
MAX_SIBLING_TX = 60
# Au-delà de 15 destinataires en ±2 min dont la majorité avec des montants différents : c'est un service
# (échangeur type FixedFloat / ChangeNOW, bridge, bot de paiement), pas un opérateur. Ses « frères » sont
# de simples clients sans lien entre eux : on s'arrête là.
SERVICE_MIN_RECIPIENTS = 15
SERVICE_MAX_SAME_RATIO = 0.5


@dataclass
class Sibling:
    address: str
    amount: float
    signature: str
    ts: int
    same_amount: bool


@dataclass
class Hop:
    address: str                 # l'adresse financée
    tx_count: int | None         # nb de tx de l'adresse (None = trop pour être compté)
    is_relay: bool
    source: str
    amount: float                # SOL reçus
    signature: str
    ts: int
    source_hot: bool = False
    source_hot_info: str = ""
    siblings: list[Sibling] = field(default_factory=list)


@dataclass
class TraceResult:
    start: str
    hops: list[Hop]
    stop_reason: str
    dev_tokens: dict[str, list[dict] | None]


# ---------------------------------------------------------------------------
def look_alike(a: str, b: str) -> bool:
    return a != b and a[:4] == b[:4] and a[-4:] == b[-4:]


class Tracer:
    def __init__(self, rpc: SolanaRPC, hot_threshold: int = 1000, known: dict[str, str] | None = None):
        self.rpc = rpc
        self.hot_threshold = hot_threshold
        # Exchanges connus (data/labels_connus.csv) : reconnus sans appel RPC
        self._hot_cache: dict[str, tuple[bool, str]] = {a: (True, lab) for a, lab in (known or {}).items()}

    async def first_funding(self, address: str) -> tuple[dict | None, int | None, str]:
        """Trouve la première transaction qui a apporté du SOL à `address`.

        Renvoie (funding, nb_tx, raison_si_échec). funding = {source, amount, signature, ts}.
        """
        sigs, truncated = await self.rpc.all_signatures(address, max_pages=10)
        if not sigs:
            return None, 0, "aucune transaction trouvée (adresse vide, ou historique indisponible sur ce RPC)"
        if truncated:
            return None, None, "plus de 10 000 tx : adresse très active (service / exchange ?), remontée arrêtée"
        ok = [s for s in reversed(sigs) if s.get("err") is None]  # de la plus ancienne à la plus récente
        for s in ok[:8]:
            tx = await self.rpc.transaction(s["signature"])
            if not tx:
                continue
            deltas = sol_deltas(tx)
            recu = deltas.get(address, 0.0)
            if recu <= DUST_SOL:
                continue  # frais payés, dust, poisoning… on passe à la suivante
            perdants = [(d, k) for k, d in deltas.items() if k != address and d < 0 and not look_alike(k, address)]
            if not perdants:
                continue
            _, source = min(perdants)
            return ({"source": source, "amount": round(recu, 6), "signature": s["signature"],
                     "ts": tx.get("blockTime") or s.get("blockTime") or 0}, len(sigs), "")
        return None, len(sigs), "aucune entrée de SOL identifiable dans les premières transactions"

    async def hot_check(self, source: str, before_sig: str) -> tuple[bool, str]:
        """Exchange / service ? = ≥ seuil tx juste avant le funding, en moins de 30 min."""
        if source in self._hot_cache:
            return self._hot_cache[source]
        page = await self.rpc.signatures(source, before=before_sig, limit=min(1000, self.hot_threshold))
        res = (False, "")
        if len(page) >= self.hot_threshold:
            times = [p["blockTime"] for p in page if p.get("blockTime")]
            span = (max(times) - min(times)) if times else 0
            if span <= HOT_WINDOW_S:
                duree = f"{span // 3600} h {span % 3600 // 60:02d}" if span >= 3600 else f"{max(1, span // 60)} min"
                res = (True, f"{len(page)} tx en {duree}")
        self._hot_cache[source] = res
        return res

    async def siblings(self, source: str, funding: dict, exclude: str) -> list[Sibling]:
        """Autres wallets financés par `source` dans la même fenêtre de temps."""
        ts, sig = funding["ts"], funding["signature"]
        cands: list[dict] = [{"signature": sig, "blockTime": ts}]
        # Plus anciennes que le funding
        older = await self.rpc.signatures(source, before=sig, limit=100)
        cands += [s for s in older if (s.get("blockTime") or 0) >= ts - SIBLING_WINDOW_S]
        # Plus récentes : on pagine depuis le haut jusqu'au funding (limité à 3 pages)
        newer: list[dict] = []
        before = None
        for _ in range(3):
            page = await self.rpc.signatures(source, before=before, until=sig, limit=1000)
            newer += page
            if len(page) < 1000:
                break
            before = page[-1]["signature"]
        else:
            log.info("Source %s trop active : frères postérieurs au funding non vérifiés", source[:6])
        cands += [s for s in newer if (s.get("blockTime") or 0) <= ts + SIBLING_WINDOW_S]

        vus: dict[str, Sibling] = {}
        for s in cands[:MAX_SIBLING_TX]:
            if s.get("err") is not None:
                continue
            tx = await self.rpc.transaction(s["signature"])
            if not tx:
                continue
            deltas = sol_deltas(tx)
            if deltas.get(source, 0) >= 0:
                continue  # la source n'a rien envoyé dans cette tx
            for addr, d in deltas.items():
                if addr in (source, exclude) or d < MIN_SIBLING_SOL or addr in vus:
                    continue
                same = abs(d - funding["amount"]) <= SAME_AMOUNT_TOLERANCE * funding["amount"]
                vus[addr] = Sibling(addr, round(d, 6), s["signature"], tx.get("blockTime") or 0, same)
        return sorted(vus.values(), key=lambda x: (not x.same_amount, x.ts))

    async def trace(self, start: str, max_hops: int = 3, with_siblings: bool = True) -> TraceResult:
        hops: list[Hop] = []
        visited = {start}
        current = start
        real_hops = 0
        stop = ""
        for _ in range(MAX_TOTAL_STEPS):
            funding, nb_tx, raison = await self.first_funding(current)
            if not funding:
                stop = raison
                break
            src = funding["source"]
            is_relay = nb_tx is not None and nb_tx <= RELAY_MAX_TX
            hot, info = await self.hot_check(src, funding["signature"])
            hop = Hop(current, nb_tx, is_relay, src, funding["amount"], funding["signature"], funding["ts"], hot, info)
            if with_siblings and not hot:
                hop.siblings = await self.siblings(src, funding, current)
                same = sum(s.same_amount for s in hop.siblings)
                if len(hop.siblings) >= SERVICE_MIN_RECIPIENTS and same / len(hop.siblings) < SERVICE_MAX_SAME_RATIO:
                    hot, info = True, f"paie {len(hop.siblings)} wallets différents en 4 min (service)"
                    hop.source_hot, hop.source_hot_info, hop.siblings = True, info, []
                    self._hot_cache[src] = (True, info)
            hops.append(hop)
            log.info("%s <- %s (%.4f SOL)%s", current[:6], src[:6], funding["amount"], " [relais]" if is_relay else "")
            if hot:
                stop = f"source = hot wallet d'exchange / service ({info})"
                break
            if src in visited:
                stop = "boucle détectée (adresse déjà vue)"
                break
            visited.add(src)
            if not is_relay:
                real_hops += 1
            if real_hops >= max_hops:
                stop = f"profondeur max atteinte ({max_hops} hops hors relais)"
                break
            current = src
        else:
            stop = "garde-fou : trop d'étapes"
        return TraceResult(start, hops, stop, {})


# ---------------------------------------------------------------------------
def short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}" if len(a) > 10 else a


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%d/%m/%Y %H:%M:%S UTC") if ts else "?"


def fmt_usd(v: float | None) -> str:
    if v is None:
        return "?"
    return f"{v / 1_000_000:.1f} M$" if v >= 1_000_000 else f"{v / 1000:.1f} k$"


def print_result(res: TraceResult, db: DB) -> None:
    def tag(a: str) -> str:
        w = db.wallet(a)
        lab = db.get_label(a)
        bits = [f"[{w['label']}]" if w else "", f"({lab})" if lab else ""]
        return " ".join(b for b in bits if b)

    print()
    print(f"🔎 Traçage de {res.start} {tag(res.start)}")
    print("─" * 78)
    for i, h in enumerate(res.hops, 1):
        kind = "relais" if h.is_relay else "funding"
        nb = f"{h.tx_count} tx" if h.tx_count is not None else "10 000+ tx"
        print(f"{i}. {short(h.address)} ({nb}) a reçu {h.amount:g} SOL de {h.source} {tag(h.source)}")
        print(f"   {kind} · {fmt_ts(h.ts)} · tx {h.signature[:16]}…")
        if h.source_hot:
            print(f"   ⛔ {short(h.source)} = exchange / service : {h.source_hot_info}")
        same = [s for s in h.siblings if s.same_amount]
        other = [s for s in h.siblings if not s.same_amount]
        if same:
            print(f"   👥 Wallets frères (même montant, ±{SIBLING_WINDOW_S // 60} min) : {len(same)}")
            for s in same:
                print(f"      • {s.address}  {s.amount:g} SOL  {fmt_ts(s.ts)} {tag(s.address)}")
        if other:
            print(f"   ↳ autres sorties de la source dans la fenêtre : {len(other)}")
            for s in other[:10]:
                print(f"      · {s.address}  {s.amount:g} SOL {tag(s.address)}")
            if len(other) > 10:
                print(f"      … et {len(other) - 10} autres")
    print("─" * 78)
    print(f"Arrêt : {res.stop_reason}")
    if res.dev_tokens:
        print()
        print("🪙 Anciens tokens pump.fun")
        for addr, coins in res.dev_tokens.items():
            if coins is None:
                print(f"   {short(addr)} : API pump.fun indisponible")
            elif not coins:
                print(f"   {short(addr)} : aucun")
            else:
                aths = [c["ath"] for c in coins if c["ath"] is not None]
                rugs = sum(c["rug"] for c in coins)
                print(f"   {short(addr)} {tag(addr)} : {len(coins)} token(s), ATH max {fmt_usd(max(aths) if aths else None)}"
                      + (f", {rugs} rug(s) probable(s) 🚩" if rugs else ""))
                for c in coins[:8]:
                    chute = f" ({-c['drop'] * 100:.1f} %)" if c["drop"] is not None and c["drop"] > 0.5 else ""
                    print(f"      ${c['symbol']} ({c['name']}) · créé {fmt_ts(c['created'])[:10]} · "
                          f"ATH {fmt_usd(c['ath'])} → MC {fmt_usd(c['mc'])}{chute}{' 🚩 rug' if c['rug'] else ''}")
                    print(f"        {c['mint']}")
    print()


def save_result(res: TraceResult, db: DB, add_to_watchlist: bool, max_depth: int) -> int:
    """Enregistre les liens et étiquettes ; ajoute éventuellement les wallets à la watchlist."""
    start_w = db.wallet(res.start)
    grp = (start_w["grp"] if start_w else None) or f"trace-{res.start[:6]}"
    ajoutes = 0
    for depth, h in enumerate(res.hops, 1):
        db.add_link(h.source, h.address, "relais" if h.is_relay else "funding", h.amount, h.signature, h.ts)
        if h.source_hot and not db.get_label(h.source):
            db.set_label(h.source, f"hot wallet / service ({h.source_hot_info})")
        for s in h.siblings:
            db.add_link(h.source, s.address, "frère" if s.same_amount else "sortie", s.amount, s.signature, s.ts)
        if not add_to_watchlist or depth > max_depth:
            continue
        if not h.source_hot:
            ajoutes += db.add_wallet(h.source, f"SRC_{h.source[:4]}", grp, "source (traçage)", depth, h.address)
        for s in h.siblings:
            if s.same_amount:
                ajoutes += db.add_wallet(s.address, f"SIB_{s.address[:4]}", grp, "wallet frère (traçage)", depth, h.source)
    return ajoutes


async def run(address: str, hops: int | None, add: bool, as_json: bool, siblings: bool) -> int:
    cfg = cfgmod.load()
    db = DB(cfg.db_path)
    db.import_watchlist(cfg.watchlist_path)
    db.import_labels(cfg.labels_path)
    if not cfg.helius_api_key:
        log.warning("Pas de HELIUS_API_KEY : RPC de secours (%s), historique possiblement incomplet", cfg.rpc_url)
    max_hops = hops or cfg.trace_max_hops
    try:
        async with SolanaRPC(cfg.rpc_url) as rpc, aiohttp.ClientSession() as http:
            known = {a: l for a, l in db.labels().items() if "hot wallet" in l.lower() or "exchange" in l.lower()}
            res = await Tracer(rpc, cfg.hot_wallet_tx_threshold, known).trace(address, max_hops, siblings)
            # Historique pump.fun de l'adresse de départ et des sources (hors hot wallets)
            for a in [address] + [h.source for h in res.hops if not h.source_hot]:
                if a not in res.dev_tokens:
                    res.dev_tokens[a] = await coins_by_creator(http, a)
    except RpcError as e:
        print("❌ Erreur RPC :", e)
        return 1
    n = save_result(res, db, add, cfg.trace_max_hops)
    if as_json:
        print(json.dumps(asdict(res), ensure_ascii=False, indent=2))
    else:
        print_result(res, db)
        if add:
            print(f"➕ {n} wallet(s) ajouté(s) à la watchlist (base SQLite).")
    db.close()
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Remonte le funding d'une adresse Solana et liste les wallets frères.")
    p.add_argument("address")
    p.add_argument("--hops", type=int, help="profondeur max hors relais (défaut : TRACE_MAX_HOPS)")
    p.add_argument("--ajouter", action="store_true", help="ajoute sources et frères à la watchlist")
    p.add_argument("--json", action="store_true", help="sortie JSON brute")
    p.add_argument("--sans-freres", action="store_true", help="plus rapide : ne cherche pas les wallets frères")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()
    cfgmod.setup_logging("tracer", logging.DEBUG if a.verbose else logging.INFO)
    return asyncio.run(run(a.address, a.hops, a.ajouter, a.json, not a.sans_freres))


if __name__ == "__main__":
    sys.exit(main())
