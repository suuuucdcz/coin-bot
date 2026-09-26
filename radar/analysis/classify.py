"""Détection des événements dans une transaction (partie « lecture pure », sans appel réseau).

Événements produits (vérifiés ensuite par radar/pipeline.py) :
    create     : un wallet suivi crée un token (initializeMint signé par lui)
    buy        : il reçoit des tokens en dépensant du SOL
    sell       : il rend des tokens contre du SOL
    lp_add     : il dépose tokens + SOL dans un programme d'AMM (= ouverture du trading)
    supply_in  : il reçoit des tokens sans rien payer (préparation de lancement)
    transfer   : il envoie du SOL par simple transfert (funding ou retour de profits)
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..sources.helius import (AMM_PROGRAMS, IGNORED_MINTS, PUMP_FUN, SIMPLE_TRANSFER_PROGRAMS,
                              TOKEN_PROGRAMS, WSOL, account_keys, sol_deltas)

MIN_TRADE_SOL = 0.005     # en dessous : frais / loyer de compte, pas un achat
MIN_TRANSFER_SOL = 0.01   # en dessous : dust / address poisoning
MIN_TRADE_USD = 1.0       # paiement en USDC / USDT
STABLES = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}


@dataclass
class Event:
    kind: str
    wallet: str                  # wallet suivi concerné
    signature: str
    ts: int
    mint: str | None = None
    sol: float = 0.0             # SOL dépensés / reçus / envoyés (valeur positive)
    tokens_raw: int = 0          # variation de tokens (valeur absolue, unités brutes)
    pre_tokens_raw: int = 0      # solde de tokens avant la tx
    decimals: int = 0
    other: str | None = None     # destinataire d'un transfert
    dex: str | None = None
    extra: dict = field(default_factory=dict)


def _all_instructions(tx: dict) -> list[dict]:
    ixs = list(tx["transaction"]["message"].get("instructions", []))
    for inner in (tx.get("meta") or {}).get("innerInstructions") or []:
        ixs += inner.get("instructions", [])
    return ixs


def programs(tx: dict) -> set[str]:
    return {ix.get("programId") for ix in _all_instructions(tx) if ix.get("programId")}


def top_level_programs(tx: dict) -> set[str]:
    return {ix.get("programId") for ix in tx["transaction"]["message"].get("instructions", [])}


def created_mints(tx: dict) -> list[str]:
    """Mints de TOKENS créés dans la tx (les NFT à 0 décimale sont exclus : positions de liquidité
    Raydium CLMM / Orca, NFT… un wallet qui ajoute de la liquidité ne « crée pas un token »)."""
    out = []
    for ix in _all_instructions(tx):
        p = ix.get("parsed")
        if isinstance(p, dict) and p.get("type") in ("initializeMint", "initializeMint2"):
            info = p.get("info", {})
            m = info.get("mint")
            if m and m not in out and info.get("decimals", 6) != 0:
                out.append(m)
    return out


def signers(tx: dict) -> set[str]:
    keys = tx["transaction"]["message"]["accountKeys"]
    return {k["pubkey"] for k in keys if isinstance(k, dict) and k.get("signer")}


def token_deltas(tx: dict) -> dict[tuple[str, str], tuple[int, int, int]]:
    """{(propriétaire, mint): (avant, après, décimales)} en unités brutes."""
    meta = tx.get("meta") or {}
    acc: dict[tuple[str, str], list[int]] = {}
    for side, lst in ((0, meta.get("preTokenBalances") or []), (1, meta.get("postTokenBalances") or [])):
        for b in lst:
            owner, mint = b.get("owner"), b.get("mint")
            if not owner or not mint:
                continue
            ui = b.get("uiTokenAmount") or {}
            v = acc.setdefault((owner, mint), [0, 0, ui.get("decimals", 0)])
            v[side] += int(ui.get("amount") or 0)
    return {k: (v[0], v[1], v[2]) for k, v in acc.items()}


def analyze(tx: dict, watched: set[str]) -> list[Event]:
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return []
    keys = account_keys(tx)
    sig = tx["transaction"]["signatures"][0]
    ts = tx.get("blockTime") or 0
    fee = (meta.get("fee") or 0) / 1e9
    payer = keys[0] if keys else None
    deltas = sol_deltas(tx)
    tdeltas = token_deltas(tx)
    progs = programs(tx)
    dex = next((AMM_PROGRAMS[p] for p in progs if p in AMM_PROGRAMS), None)
    if PUMP_FUN in progs and not dex:
        dex = "pump.fun"
    sgn = signers(tx)
    mints_created = created_mints(tx)
    real_token_moves = any(m not in IGNORED_MINTS and pre != post for (_o, m), (pre, post, _d) in tdeltas.items())
    plain_sol_move = (top_level_programs(tx) <= SIMPLE_TRANSFER_PROGRAMS | TOKEN_PROGRAMS
                      and not real_token_moves and not mints_created)

    involved = ({k for k in keys if k in watched} | {o for (o, _m) in tdeltas if o in watched})
    events: list[Event] = []
    for w in involved:
        # SOL net hors frais de réseau (si le wallet paie les frais), wSOL compris : un achat payé avec un
        # compte wSOL déjà rempli, ou une vente encaissée en wSOL, ne fait pas bouger le SOL natif.
        sol_net = deltas.get(w, 0.0) + (fee if w == payer else 0.0)
        pre_w, post_w, _dw = tdeltas.get((w, WSOL), (0, 0, 9))
        sol_eff = sol_net + (post_w - pre_w) / 1e9
        usd = sum((b - a) / 10 ** d for (o, m), (a, b, d) in tdeltas.items() if o == w and m in STABLES)
        paid = sol_eff < -MIN_TRADE_SOL or usd < -MIN_TRADE_USD
        got_paid = sol_eff > MIN_TRADE_SOL or usd > MIN_TRADE_USD
        mine: list[Event] = []

        # --- création de token ---
        if w in sgn:
            for m in mints_created:
                pre, post, dec = tdeltas.get((w, m), (0, 0, 0))
                mine.append(Event("create", w, sig, ts, m, sol=max(0.0, -sol_eff), tokens_raw=max(0, post - pre),
                                  decimals=dec, dex=dex))

        # --- mouvements de tokens ---
        for (owner, m), (pre, post, dec) in tdeltas.items():
            if owner != w or m in IGNORED_MINTS or m in mints_created:
                continue
            d = post - pre
            if d == 0 or dec == 0:
                continue  # 0 décimale = NFT / position de liquidité, pas un memecoin
            common = dict(mint=m, tokens_raw=abs(d), pre_tokens_raw=pre, decimals=dec, dex=dex,
                          extra={"usd": round(-usd, 2)} if usd < -MIN_TRADE_USD else {})
            if d > 0 and paid:
                mine.append(Event("buy", w, sig, ts, sol=max(0.0, -sol_eff), **common))
            elif d > 0:
                mine.append(Event("supply_in", w, sig, ts, **common))
            elif got_paid:
                common["extra"] = {"usd": round(usd, 2)} if usd > MIN_TRADE_USD else {}
                mine.append(Event("sell", w, sig, ts, sol=max(0.0, sol_eff), **common))
            elif dex and dex != "pump.fun" and paid:
                # Tokens ET SOL/wSOL/USDC déposés ensemble dans un AMM = ajout de liquidité.
                # (Une vente encaissée en wSOL/USDC n'est plus prise pour un ajout de liquidité.)
                mine.append(Event("lp_add", w, sig, ts, sol=max(0.0, -sol_eff), **common))
            elif not dex:
                # Tokens envoyés à un autre wallet sans rien encaisser : déplacement de supply (souvent avant
                # une vente : vers un exchange ou des wallets relais). Vu en vrai : 2,6 milliards de $PAID déplacés.
                dest = max(((o, b - a) for (o, mm), (a, b, _d2) in tdeltas.items() if mm == m and o != w and b > a),
                           key=lambda x: x[1], default=(None, 0))[0]
                if dest:
                    common["extra"] = {}
                    mine.append(Event("supply_out", w, sig, ts, other=dest, **common))

        # Ajout de liquidité : les jetons LP reçus dans la même tx ne sont pas un « achat »
        if any(e.kind == "lp_add" for e in mine):
            mine = [e for e in mine if e.kind not in ("buy", "supply_in")]
        events += mine

        # --- transfert de SOL envoyé par le wallet suivi ---
        # Vu en pratique (cluster Reserve) : le SOL passe par un compte wSOL temporaire
        # (programme Token) pour masquer le transfert. On l'accepte tant qu'aucun vrai token ne bouge.
        if sol_eff < -MIN_TRANSFER_SOL and plain_sol_move:
            for dst, d in deltas.items():
                if dst != w and d >= MIN_TRANSFER_SOL:
                    events.append(Event("transfer", w, sig, ts, sol=round(d, 6), other=dst))
    return events


# ---------------------------------------------------------------------------
# Test en ligne de commande : python -m radar.analysis.classify <signature> [--envoyer]
def main() -> int:
    import argparse
    import asyncio
    import logging
    import sys

    from .. import config as cfgmod
    from ..pipeline import run_signature_cli

    p = argparse.ArgumentParser(description="Analyse une transaction comme si elle arrivait en temps réel.")
    p.add_argument("signature")
    p.add_argument("--envoyer", action="store_true", help="envoie vraiment les alertes sur Telegram")
    a = p.parse_args()
    cfgmod.setup_logging("classify", logging.INFO)
    return asyncio.run(run_signature_cli(a.signature, a.envoyer))


if __name__ == "__main__":
    import sys
    sys.exit(main())
