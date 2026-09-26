"""🎭 Faux coins : le dev lance un coin au ticker annoncé, fait une bougie (~50 k$ de MC),
~50 petits wallets entrent, puis rug. Le VRAI coin arrive ensuite.

Le faux coin est une mine d'or : ses petits wallets ont presque toujours été financés par le
même wallet (celui du dev). On remonte leur financement -> wallet du dev -> on attend le vrai coin.
"""
from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field

from .analysis.classify import token_deltas
from .analysis.tracer import Tracer
from .sources import pumpfun
from .sources.helius import sol_deltas

log = logging.getLogger("fakes")

FAKE_MIN_ATH = 15_000        # bougie d'au moins 15 k$ de MC
FAKE_MAX_ATH = 5_000_000     # un « faux coin » fait une petite bougie : au-delà, ATH douteux ou vrai succès
FAKE_MIN_DROP = 0.80         # puis chute d'au moins 80 %
FAKE_MIN_BUYERS = 12         # beaucoup de wallets…
FAKE_MAX_MEDIAN_BUY = 1.0    # … avec de petites sommes (SOL)
MAX_TX = 150
MAX_BUYERS_TRACED = 40
COMMON_FUNDER_MIN = 3        # un wallet qui a financé ≥ 3 acheteurs = wallet du dev


@dataclass
class FakeReport:
    mint: str
    symbol: str | None = None
    creator: str | None = None
    twitter: str | None = None         # lien X dans les métadonnées du faux coin
    ath: float | None = None
    mc: float | None = None
    drop: float | None = None
    buyers: dict[str, float] = field(default_factory=dict)   # wallet -> SOL dépensés
    median_buy: float | None = None
    creator_sold: bool = False
    is_fake: bool = False
    verdict: str = ""
    funders: list[tuple[str, int]] = field(default_factory=list)   # (wallet, nb d'acheteurs financés)
    funded_by: dict[str, str] = field(default_factory=dict)        # acheteur -> financeur


async def analyze(pipeline, mint: str, trace_funders: bool = True) -> FakeReport:
    rpc, http = pipeline.rpc, pipeline.http
    rep = FakeReport(mint)
    coin = await pumpfun.coin(http, mint)
    if coin:
        rep.symbol, rep.creator, rep.ath, rep.mc = coin["symbol"], coin.get("creator"), coin["ath"], coin["mc"]
        rep.twitter = coin.get("twitter")
    # Transactions du token (un faux coin en a peu : on les lit toutes, jusqu'à MAX_TX)
    sigs = await rpc.signatures(mint, limit=1000)
    first = [s for s in reversed(sigs) if s.get("err") is None][:MAX_TX]
    for i, s in enumerate(first):
        tx = await rpc.transaction(s["signature"])
        if not tx:
            continue
        if i == 0 and not rep.creator:
            k = tx["transaction"]["message"]["accountKeys"][0]
            rep.creator = k["pubkey"] if isinstance(k, dict) else k
        sd = sol_deltas(tx)
        for (owner, m), (a, b, _d) in token_deltas(tx).items():
            if m != mint or owner == mint:
                continue
            if b > a and sd.get(owner, 0) < 0 and owner != rep.creator:
                rep.buyers[owner] = rep.buyers.get(owner, 0.0) + (-sd[owner])
            elif b < a and owner == rep.creator and sd.get(owner, 0) > 0:
                rep.creator_sold = True
    if not rep.ath and rep.creator:
        for c in await pumpfun.coins_by_creator(http, rep.creator) or []:
            if c["mint"] == mint:
                rep.ath, rep.mc, rep.symbol = c["ath"], c["mc"], rep.symbol or c["symbol"]
    if rep.ath and rep.mc is not None and rep.ath > 0:
        rep.drop = 1 - rep.mc / rep.ath
    buys = list(rep.buyers.values())
    rep.median_buy = statistics.median(buys) if buys else None

    rep.is_fake = bool(rep.ath and FAKE_MIN_ATH <= rep.ath <= FAKE_MAX_ATH and rep.drop is not None
                       and rep.drop >= FAKE_MIN_DROP
                       and len(buys) >= FAKE_MIN_BUYERS and rep.median_buy is not None
                       and rep.median_buy <= FAKE_MAX_MEDIAN_BUY)
    rug_fast = bool(rep.drop is not None and rep.drop >= 0.9 and rep.creator_sold)
    if rep.is_fake:
        rep.verdict = (f"bougie à {rep.ath / 1000:.0f} k$ puis −{rep.drop * 100:.0f} %, {len(buys)} petits wallets "
                       f"(médiane {rep.median_buy:.2f} SOL)")
    elif rug_fast:
        rep.is_fake = True
        rep.verdict = f"rug rapide : le créateur a vendu, −{rep.drop * 100:.0f} % depuis l'ATH"

    # Remonter le financement de 40 acheteurs coûte des centaines d'appels Helius : seulement pour un vrai faux coin
    if rep.is_fake and trace_funders:
        await _common_funders(pipeline, rep)
    return rep


async def _common_funders(pipeline, rep: FakeReport) -> None:
    """Qui a financé les acheteurs (et le créateur) ? Un financeur commun = le wallet du dev."""
    known = {a: l for a, l in pipeline.labels.items() if "hot wallet" in l.lower()}
    tracer = Tracer(pipeline.rpc, pipeline.cfg.hot_wallet_tx_threshold, known)
    targets = sorted(rep.buyers, key=lambda a: rep.buyers[a])[:MAX_BUYERS_TRACED]
    if rep.creator:
        targets.append(rep.creator)
    counts: dict[str, int] = {}
    for w in targets:
        try:
            f, _n, _r = await tracer.first_funding(w)
        except Exception:
            continue
        if not f:
            continue
        src = f["source"]
        hot, _i = await tracer.hot_check(src, f["signature"])
        if hot:
            continue
        rep.funded_by[w] = src
        counts[src] = counts.get(src, 0) + 1
    rep.funders = sorted(((a, n) for a, n in counts.items() if n >= 2 or a == rep.funded_by.get(rep.creator or "")),
                         key=lambda x: -x[1])
