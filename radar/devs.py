"""Identification du dev d'un token et de ses satellites, + fiche Telegram « 🧬 Devs & satellites ».

Satellites = wallets liés au dev :
- financeurs (remontée du funding, hors exchanges) et relais ;
- frères (même source, même montant, même minute) ;
- gros détenteurs de la supply (≥ 1 %, hors pools / programmes) ;
- wallets que le dev a lui-même financés (vus en temps réel).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from . import alerts as A
from .analysis.enrich import TokenInfo
from .analysis.tracer import TraceResult, Tracer, save_result
from .sources.helius import SYSTEM_PROGRAM, SolanaRPC, sol_deltas
from .telegram import buttons, esc

log = logging.getLogger("devs")

HOLDER_MIN_PCT = 1.0


@dataclass
class Satellite:
    address: str
    role: str


@dataclass
class DevReport:
    dev: str | None
    how: str                                   # comment le dev a été trouvé
    satellites: list[Satellite] = field(default_factory=list)
    trace: TraceResult | None = None
    added: int = 0


async def find_deployer(rpc: SolanaRPC, mint: str) -> str | None:
    """Payeur de la toute première transaction du mint (= celui qui l'a créé)."""
    sigs, truncated = await rpc.all_signatures(mint, max_pages=5)
    if truncated or not sigs:
        return None
    for s in reversed(sigs):
        if s.get("err") is None:
            tx = await rpc.transaction(s["signature"])
            if tx:
                k = tx["transaction"]["message"]["accountKeys"][0]
                return k["pubkey"] if isinstance(k, dict) else k
    return None


async def big_holders(rpc: SolanaRPC, mint: str, supply_raw: int | None) -> list[tuple[str, float]]:
    """Propriétaires des plus gros comptes du token (hors pools / programmes)."""
    if not supply_raw:
        return []
    try:
        accounts = await rpc.token_largest_accounts(mint)
    except Exception as e:
        log.debug("getTokenLargestAccounts : %s", e)
        return []
    accounts = [a for a in accounts if 100 * int(a["amount"]) / supply_raw >= HOLDER_MIN_PCT][:10]
    if not accounts:
        return []
    res = await rpc.call("getMultipleAccounts", [[a["address"] for a in accounts], {"encoding": "jsonParsed"}])
    owners = []
    for a, v in zip(accounts, (res or {}).get("value", [])):
        try:
            owners.append((v["data"]["parsed"]["info"]["owner"], 100 * int(a["amount"]) / supply_raw))
        except (TypeError, KeyError):
            continue
    if not owners:
        return []
    # Un propriétaire qui n'est pas un wallet normal (compte d'un programme) = pool / bonding curve
    res = await rpc.call("getMultipleAccounts", [[o for o, _ in owners], {"encoding": "base64"}])
    out = []
    for (o, pct), v in zip(owners, (res or {}).get("value", [])):
        if v is None or v.get("owner") == SYSTEM_PROGRAM:
            out.append((o, pct))
    return out


async def funders(rpc: SolanaRPC, address: str, max_tx: int = 50, min_sol: float = 0.05) -> list[tuple[str, float]]:
    """Tous les wallets qui ont envoyé du SOL à `address` (seulement pour un petit wallet).

    Vu en pratique ($ASH) : le wallet LP est d'abord financé par le créateur du mint (0,015 SOL),
    puis par le wallet principal du dev HiQd… (0,5 SOL). Le 1er funding ne suffit donc pas.
    """
    sigs = await rpc.signatures(address, limit=max_tx + 1)
    if len(sigs) > max_tx:
        return []
    out: dict[str, float] = {}
    for s in sigs:
        if s.get("err") is not None:
            continue
        tx = await rpc.transaction(s["signature"])
        if not tx:
            continue
        d = sol_deltas(tx)
        if d.get(address, 0) < min_sol:
            continue
        losers = [(v, k) for k, v in d.items() if k != address and v < 0]
        if losers:
            src = min(losers)[1]
            out[src] = out.get(src, 0.0) + d[address]
    return sorted(out.items(), key=lambda x: -x[1])


async def funded_wallets(rpc: SolanaRPC, dev: str, max_tx: int = 150, days: int = 14,
                         min_sol: float = 0.05) -> list[tuple[str, float]]:
    """Wallets FRAIS (< 30 tx) que le dev a financés récemment : futurs snipers / wallets de bundle."""
    from .analysis.classify import top_level_programs, token_deltas
    from .sources.helius import IGNORED_MINTS, SIMPLE_TRANSFER_PROGRAMS, TOKEN_PROGRAMS
    sigs = await rpc.signatures(dev, limit=max_tx)
    since = time.time() - days * 86400
    out: dict[str, float] = {}
    for s in sigs:
        if s.get("err") is not None or (s.get("blockTime") or 0) < since:
            continue
        tx = await rpc.transaction(s["signature"])
        if not tx:
            continue
        real_tokens = any(m not in IGNORED_MINTS and a != b for (_o, m), (a, b, _d) in token_deltas(tx).items())
        if real_tokens or not top_level_programs(tx) <= SIMPLE_TRANSFER_PROGRAMS | TOKEN_PROGRAMS:
            continue
        d = sol_deltas(tx)
        if d.get(dev, 0) > -min_sol:
            continue
        for addr, v in d.items():
            if addr != dev and v >= min_sol:
                out[addr] = out.get(addr, 0.0) + v
    fresh = []
    for addr, amount in sorted(out.items(), key=lambda x: -x[1])[:25]:
        if len(await rpc.signatures(addr, limit=30)) < 30:
            fresh.append((addr, round(amount, 4)))
    return fresh


async def early_buyers(rpc: SolanaRPC, mint: str, creator: str | None, n: int = 25) -> list[str]:
    """Acheteurs des toutes premières transactions d'un token (bloc de création = bundle)."""
    from .analysis.classify import token_deltas
    page = await rpc.signatures(mint, limit=1000)
    before = page[-1]["signature"] if len(page) == 1000 else None
    for _ in range(4):  # remonte vers les plus anciennes (limité)
        if not before:
            break
        nxt = await rpc.signatures(mint, before=before, limit=1000)
        if not nxt:
            break
        page = nxt
        before = nxt[-1]["signature"] if len(nxt) == 1000 else None
    first = [s for s in reversed(page) if s.get("err") is None][:n]
    buyers: list[str] = []
    for s in first:
        tx = await rpc.transaction(s["signature"])
        if not tx:
            continue
        for (owner, m), (a, b, _d) in token_deltas(tx).items():
            if m == mint and b > a and owner not in (creator, mint) and owner not in buyers:
                buyers.append(owner)
    return buyers


async def is_generic_bot(rpc: SolanaRPC, address: str) -> bool:
    """≥ 300 tx en moins de 48 h = bot qui trade tout (sniper générique, market maker)."""
    sigs = await rpc.signatures(address, limit=300)
    if len(sigs) < 300:
        return False
    times = [s["blockTime"] for s in sigs if s.get("blockTime")]
    return bool(times) and max(times) - min(times) < 48 * 3600


async def expand(pipeline, dev: str, http=None) -> list[Satellite]:
    """Cartographie complète autour d'un dev : funding, frères, wallets financés, bundlers récurrents."""
    from .sources.pumpfun import coins_by_creator
    rpc = pipeline.rpc
    known = {a: l for a, l in pipeline.labels.items() if "hot wallet" in l.lower()}
    sats: dict[str, str] = {}
    res = await Tracer(rpc, pipeline.cfg.hot_wallet_tx_threshold, known).trace(dev, 2)
    save_result(res, pipeline.db, False, pipeline.cfg.trace_max_hops)
    relays = {h.address for h in res.hops if h.is_relay}
    for h in res.hops:
        if not h.source_hot:
            sats.setdefault(h.source, "relais" if h.source in relays else "financeur du dev")
        for s in h.siblings:
            if s.same_amount:
                sats.setdefault(s.address, f"frère ({s.amount:g} SOL de la même source)")
    for addr, amount in await funded_wallets(rpc, dev):
        sats.setdefault(addr, f"wallet frais financé par le dev ({amount:g} SOL)")
    # Acheteurs précoces récurrents sur les anciens coins du dev = ses snipers / bundlers
    coins = (await coins_by_creator(pipeline.http, dev)) or []
    counts: dict[str, int] = {}
    for c in coins[:5]:
        try:
            for b in await early_buyers(rpc, c["mint"], dev):
                counts[b] = counts.get(b, 0) + 1
        except Exception as e:
            log.debug("early buyers %s : %s", c["mint"][:6], e)
    for b, n in sorted(counts.items(), key=lambda x: -x[1])[:30]:
        if not (n >= 2 or (len(coins) == 1 and len(sats) < 30)):
            continue
        if await is_generic_bot(rpc, b):
            continue  # sniper qui achète tous les lancements : pas un satellite du dev
        sats.setdefault(b, f"acheteur précoce sur {n} coin(s) du dev (sniper / bundle)")
    return [Satellite(a, r) for a, r in sats.items() if a != dev and a not in known][:40]


async def resolve(pipeline, info: TokenInfo | None, dev: str | None, group: str) -> DevReport:
    """Trouve le dev (si besoin) et ses satellites, puis les ajoute à la watchlist (groupe = ticker)."""
    rpc = pipeline.rpc
    how = "créateur du token"
    if not dev and info:
        dev = info.creator
        if not dev:
            dev = await find_deployer(rpc, info.mint)
            how = "payeur de la 1re transaction du contrat"
    report = DevReport(dev, how)
    if not dev:
        report.how = "introuvable (contrat trop actif ou pas encore créé)"
        return report
    # Un dev déjà connu garde son groupe (ex. reserve-cluster) : ses satellites en héritent
    group = pipeline.group(dev) or group

    sats: dict[str, str] = {}
    known = {a: l for a, l in pipeline.labels.items() if "hot wallet" in l.lower()}
    tracer = Tracer(rpc, pipeline.cfg.hot_wallet_tx_threshold, known)
    if info:
        holders = [(o, pct) for o, pct in await big_holders(rpc, info.mint, info.supply_raw) if o != dev]
        # Un sniper générique détient souvent un gros % juste après le lancement : ce n'est pas un satellite
        # du dev, et il ferait sonner « le cluster entre » sur chaque token qu'il achète.
        holders = [(o, pct) for o, pct in holders if not await is_generic_bot(rpc, o)]
        for owner, pct in holders:
            sats[owner] = f"détient {pct:.1f} % de la supply"
        # Qui a financé les gros détenteurs ? (cas $ASH : HiQd… a financé le wallet LP)
        for owner, pct in holders[:5]:
            for src, amount in await funders(rpc, owner):
                if src == dev or src in sats or src in known:
                    continue
                sats[src] = f"a financé le détenteur de {pct:.0f} % ({amount:g} SOL)"
    # Autres financeurs du dev lui-même (s'il a peu de transactions)
    for src, amount in await funders(rpc, dev):
        if src not in sats and src not in known:
            sats[src] = f"a financé le dev ({amount:g} SOL)"
    res = await tracer.trace(dev, pipeline.cfg.trace_max_hops)
    report.trace = res
    relays = {h.address for h in res.hops if h.is_relay}
    for h in res.hops:
        if h.address != dev:
            sats.setdefault(h.address, "relais" if h.is_relay else "wallet intermédiaire")
        if not h.source_hot:
            sats.setdefault(h.source, "relais" if h.source in relays else "financeur")
        for s in h.siblings:
            if s.same_amount:
                sats.setdefault(s.address, f"frère ({s.amount:g} SOL de la même source)")
    for row in pipeline.db.conn.execute("SELECT dst, amount FROM links WHERE src=? AND kind='funding'", (dev,)):
        sats.setdefault(row["dst"], f"financé par le dev ({row['amount']:g} SOL)")
    report.satellites = [Satellite(a, r) for a, r in sats.items()]

    # Tout le monde entre dans la watchlist
    save_result(res, pipeline.db, False, pipeline.cfg.trace_max_hops)
    if await pipeline.watch(dev, f"DEV_{group}"[:40], group, "dev (auto)", 1, None):
        report.added += 1
    for s in report.satellites:
        if await pipeline.watch(s.address, f"SAT_{s.address[:4]}", group, s.role, 1, dev):
            report.added += 1
    return report


def chain_text(pipeline, report: DevReport) -> str:
    """« dev ← 0.48 SOL ← ASH_FUNDER (relais) ← 0.40 SOL ← MEXC » (texte brut)."""
    if not report.trace or not report.trace.hops:
        return ""
    hops = report.trace.hops
    chain = ["dev"]
    for i, h in enumerate(hops):
        src = pipeline.label(h.source) or A.short(h.source)
        if i + 1 < len(hops) and hops[i + 1].is_relay:
            src += " (relais)"
        chain.append(f"{h.amount:g} SOL ← {src}")
    return " ← ".join(chain)


FUNDING_ROLES = ("financeur", "relais", "frère", "a financé", "financé par", "intermédiaire")


def rug_flags(pipeline, report: DevReport) -> list[str]:
    """Le dev est-il lié à un cluster de rugs connu ? Seulement par des liens d'ARGENT (financement, relais,
    frères) : un simple détenteur ou acheteur précoce commun ne suffit pas."""
    if not report.dev:
        return []
    lies = [s.address for s in report.satellites if any(k in s.role.lower() for k in FUNDING_ROLES)]
    return pipeline.rug_flags(report.dev, *lies)


def card(pipeline, report: DevReport, info: TokenInfo | None, title: str) -> tuple[str, dict]:
    """Fiche du dev pour le sujet 🧬."""
    lines = [f"🧬 <b>FICHE DEV — {title}</b>"]
    if not report.dev:
        lines.append(f"Dev : {esc(report.how)}")
        return "\n".join(lines), buttons()
    lab = pipeline.label(report.dev)
    lines.append(f"Dev : <code>{report.dev}</code>" + (f" [{esc(lab)}]" if lab else "") + f"\n<i>({esc(report.how)})</i>")
    if report.trace and report.trace.hops:
        lines.append("Financement : " + esc(chain_text(pipeline, report)))
        lines.append(f"<i>Arrêt : {esc(report.trace.stop_reason)}</i>")
    if info:
        d = A.dev_line(info)
        if d:
            lines.append(d)
    if report.satellites:
        lines.append(f"\n<b>Satellites ({len(report.satellites)})</b> :")
        for s in report.satellites[:25]:
            lab = pipeline.label(s.address)
            lines.append(f"• <code>{s.address}</code> — {esc(s.role)}" + (f" [{esc(lab)}]" if lab else ""))
        if len(report.satellites) > 25:
            lines.append(f"… et {len(report.satellites) - 25} autres")
    else:
        lines.append("Satellites : aucun trouvé pour l'instant")
    lines.append(f"\n➕ {report.added} wallet(s) ajouté(s) à la surveillance temps réel")
    for f in rug_flags(pipeline, report):
        lines.append(f"🚩 {esc(f)}")
    links = [("Dev sur Solscan", f"https://solscan.io/account/{report.dev}")]
    if info:
        links.append(("Token", f"https://solscan.io/token/{info.mint}"))
    return "\n".join(lines), buttons(*links, per_row=2)


def satellites_json(report: DevReport) -> list[dict]:
    return [{"address": s.address, "role": s.role} for s in report.satellites]
