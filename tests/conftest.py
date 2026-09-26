"""Outils communs aux tests : fabrication de transactions Solana « jsonParsed » minimales."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

LAMPORTS = 1_000_000_000
SYSTEM = "11111111111111111111111111111111"
PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

WATCHED = "HiQdmuwcQuzhL5KJxqMh7WGzMcdjjLfsGSe1pcLM6YnM"
NEW_WALLET = "33yhak3xPxpcRB9XbhbhSYgqsZa1rBmF7k55VydUXEHp"
MINT = "DU5L11pfQ1EyDWvBhM5sp2piGeHEYDy9sdTvDkfcKrNs"
CURVE = "Bqxuzhw484izLTXS6wWS8PvuTWZEcHFQ9VdwDgKX3fhH"


def make_tx(accounts: list[tuple[str, bool, float, float]], programs: list[str], fee: int = 5000,
            tokens: list[tuple[str, str, int, int]] | None = None, parsed: list[dict] | None = None) -> dict:
    """accounts : (adresse, signataire, SOL avant, SOL après) ; le 1er compte paie les frais.

    tokens : (propriétaire, mint, unités brutes avant, après) ; parsed : instructions analysées en plus.
    """
    pre_tok, post_tok = [], []
    for i, (owner, mint, avant, apres) in enumerate(tokens or []):
        for lst, v in ((pre_tok, avant), (post_tok, apres)):
            lst.append({"accountIndex": 10 + i, "mint": mint, "owner": owner,
                        "uiTokenAmount": {"amount": str(v), "decimals": 6}})
    return {
        "blockTime": 1_790_000_000,
        "transaction": {
            "signatures": ["sig" + "1" * 60],
            "message": {
                "accountKeys": [{"pubkey": a, "signer": s} for a, s, _b, _c in accounts],
                "instructions": [{"programId": p} for p in programs] + (parsed or []),
            },
        },
        "meta": {
            "err": None,
            "fee": fee,
            "preBalances": [int(b * LAMPORTS) for _a, _s, b, _c in accounts],
            "postBalances": [int(c * LAMPORTS) for _a, _s, _b, c in accounts],
            "preTokenBalances": pre_tok,
            "postTokenBalances": post_tok,
            "innerInstructions": [],
        },
    }
