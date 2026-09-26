"""Détection des événements on-chain (radar/analysis/classify.py) sur des transactions fabriquées."""
from conftest import CURVE, MINT, NEW_WALLET, PUMP_FUN, SYSTEM, TOKEN, WATCHED, make_tx

from radar.analysis.classify import analyze


def kinds(events):
    return sorted(e.kind for e in events)


def test_funding_d_un_nouveau_wallet():
    tx = make_tx([(WATCHED, True, 10, 8.999995), (NEW_WALLET, False, 0, 1), (SYSTEM, False, 1, 1)], [SYSTEM])
    ev = analyze(tx, {WATCHED})
    assert kinds(ev) == ["transfer"]
    assert ev[0].other == NEW_WALLET and abs(ev[0].sol - 1.0) < 1e-6


def test_dust_ignore():
    """Transfert quasi nul (address poisoning) : aucun événement."""
    tx = make_tx([(WATCHED, True, 10, 9.998995), (NEW_WALLET, False, 0, 0.001)], [SYSTEM])
    assert analyze(tx, {WATCHED}) == []


def test_achat_pump_fun():
    tx = make_tx([(WATCHED, True, 10, 8.49), (CURVE, False, 30, 31.5)], [PUMP_FUN],
                 tokens=[(WATCHED, MINT, 0, 35_000_000_000_000)])
    ev = analyze(tx, {WATCHED})
    assert kinds(ev) == ["buy"]
    assert ev[0].mint == MINT and 1.4 < ev[0].sol < 1.6 and ev[0].dex == "pump.fun"


def test_vente():
    tx = make_tx([(WATCHED, True, 1, 3), (CURVE, False, 30, 28)], [PUMP_FUN],
                 tokens=[(WATCHED, MINT, 50_000, 0)])
    ev = analyze(tx, {WATCHED})
    assert kinds(ev) == ["sell"] and ev[0].pre_tokens_raw == 50_000


def test_supply_recue_gratuitement():
    tx = make_tx([(NEW_WALLET, True, 1, 0.999995), (WATCHED, False, 1, 1)], [TOKEN],
                 tokens=[(WATCHED, MINT, 0, 2_000_000)])
    assert kinds(analyze(tx, {WATCHED})) == ["supply_in"]


def test_creation_de_token():
    init = {"programId": TOKEN, "parsed": {"type": "initializeMint2", "info": {"mint": MINT}}}
    tx = make_tx([(WATCHED, True, 5, 3.97), (MINT, True, 0, 0.0015)], [PUMP_FUN],
                 tokens=[(WATCHED, MINT, 0, 100_000)], parsed=[init])
    ev = analyze(tx, {WATCHED})
    assert kinds(ev) == ["create"] and ev[0].mint == MINT


def test_transaction_echouee_ignoree():
    tx = make_tx([(WATCHED, True, 10, 8.999995), (NEW_WALLET, False, 0, 1)], [SYSTEM])
    tx["meta"]["err"] = {"InstructionError": [0, "Custom"]}
    assert analyze(tx, {WATCHED}) == []


def test_wallet_non_suivi_ignore():
    tx = make_tx([(WATCHED, True, 10, 8.999995), (NEW_WALLET, False, 0, 1)], [SYSTEM])
    assert analyze(tx, {"autre"}) == []


# --- biais corrigés -------------------------------------------------------------------------------
RAYDIUM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LP_MINT = "Bqxuzhw484izLTXS6wWS8PvuTWZEcHFQ9VdwDgKX3fhH"


def test_vente_encaissee_en_wsol_n_est_pas_un_ajout_de_liquidite():
    tx = make_tx([(WATCHED, True, 1, 0.999995)], [RAYDIUM],
                 tokens=[(WATCHED, MINT, 50_000, 0), (WATCHED, WSOL, 0, 2_000_000_000)])
    ev = analyze(tx, {WATCHED})
    assert kinds(ev) == ["sell"] and abs(ev[0].sol - 2) < 1e-6


def test_achat_paye_en_usdc():
    tx = make_tx([(WATCHED, True, 1, 0.999995)], [RAYDIUM],
                 tokens=[(WATCHED, MINT, 0, 50_000), (WATCHED, USDC, 500_000_000, 300_000_000)])
    ev = analyze(tx, {WATCHED})
    assert kinds(ev) == ["buy"] and ev[0].extra["usd"] == 200


def test_ajout_de_liquidite_sans_faux_achat_du_jeton_lp():
    tx = make_tx([(WATCHED, True, 100, 10), (CURVE, False, 0, 90)], [RAYDIUM],
                 tokens=[(WATCHED, MINT, 6_000_000, 0), (WATCHED, LP_MINT, 0, 77_000)])
    assert kinds(analyze(tx, {WATCHED})) == ["lp_add"]


def test_position_nft_n_est_pas_une_creation_de_token():
    nft = {"programId": TOKEN, "parsed": {"type": "initializeMint2", "info": {"mint": LP_MINT, "decimals": 0}}}
    tx = make_tx([(WATCHED, True, 10, 4), (CURVE, False, 0, 6)], [RAYDIUM],
                 tokens=[(WATCHED, MINT, 1_000, 0), (WATCHED, LP_MINT, 0, 1)], parsed=[nft])
    assert "create" not in kinds(analyze(tx, {WATCHED}))


def test_emballer_du_sol_n_est_pas_un_funding():
    wsol_compte = "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD"
    tx = make_tx([(WATCHED, True, 10, 8.997955), (wsol_compte, False, 0, 1.002)], [TOKEN],
                 tokens=[(WATCHED, WSOL, 0, 1_000_000_000)])
    assert analyze(tx, {WATCHED}) == []
