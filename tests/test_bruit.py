"""Bruit et quota : jeton LP, usine à tokens, wallets très actifs, journal des décisions complet."""
import asyncio
import time

from conftest import CURVE, MINT, TOKEN, WATCHED, make_tx
from test_pipeline import setup  # noqa: F401  (fixture partagée)

from radar.analysis.classify import Event, analyze
from radar.analysis.enrich import TokenInfo, _dev_flags
from radar.sources.helius import COMPUTE_BUDGET, SYSTEM_PROGRAM, notable_logs

CPMM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
LAUNCHLAB = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
LP_MINT = "FDWWYygjSa5MUvSHxLyodMZ7rkdzRM5XsFtSZLSkBidh"
POOL_AUTH = "GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL"
USINE = "newvxgk5iKU9nDDFzVuJvjNChES5iRs7bZNAE3U1uor"


def init_mint(mint, auth, decimals=9):
    return {"programId": TOKEN, "parsed": {"type": "initializeMint2",
                                           "info": {"mint": mint, "decimals": decimals, "mintAuthority": auth}}}


# --- jeton LP ---------------------------------------------------------------------------------------
def test_jeton_lp_d_un_pool_n_est_pas_une_creation():
    # Vu en vrai : la création du pool Raydium CPMM de $ASH donnait « 🔴 DEV CRÉE UN TOKEN » (FDWWYy)
    tx = make_tx([(WATCHED, True, 100, 10), (CURVE, False, 0, 90)], [CPMM],
                 tokens=[(WATCHED, MINT, 6_000_000, 0), (WATCHED, LP_MINT, 0, 77_000)],
                 parsed=[init_mint(LP_MINT, POOL_AUTH)])
    assert sorted(e.kind for e in analyze(tx, {WATCHED})) == ["lp_add"]


def test_token_cree_par_le_dev_dans_la_tx_du_pool_reste_une_creation():
    tx = make_tx([(WATCHED, True, 100, 10), (CURVE, False, 0, 90)], [CPMM],
                 tokens=[(WATCHED, MINT, 0, 1_000)],
                 parsed=[init_mint(MINT, WATCHED, 6), init_mint(LP_MINT, POOL_AUTH)])
    ev = [e for e in analyze(tx, {WATCHED}) if e.kind == "create"]
    assert [e.mint for e in ev] == [MINT]


def test_launchpad_cree_toujours_de_vrais_tokens():
    tx = make_tx([(WATCHED, True, 5, 3.9), (MINT, True, 0, 0.0015)], [LAUNCHLAB],
                 tokens=[(WATCHED, MINT, 0, 100_000)], parsed=[init_mint(MINT, "autorite_launchlab", 6)])
    assert "create" in [e.kind for e in analyze(tx, {WATCHED})]


# --- usine à tokens ----------------------------------------------------------------------------------
def test_usine_a_tokens_reperee_dans_l_historique_pump_fun():
    now = int(time.time())
    info = TokenInfo(MINT, dev_coins=[{"mint": f"m{i}", "rug": False, "ath": 60_000, "created": now - 3600 * i}
                                      for i in (1, 2)])
    _dev_flags(info)
    assert any("3 tokens créés en 24 h" in f for f in info.flags)
    info = TokenInfo(MINT, dev_coins=[{"mint": "m1", "rug": False, "ath": 60_000, "created": now - 3 * 86400}])
    _dev_flags(info)
    assert not any("24 h" in f for f in info.flags)


def _cree(p, wallet, mint):
    async def go():
        a = await p.process(Event("create", wallet, "sig" + mint, int(time.time()), mint, extra={"symbol": "CAT"}))
        if a:
            p.emit(a)
        await asyncio.sleep(0)
        return a
    return asyncio.run(go())


def test_usine_a_tokens_signalee_puis_retiree(setup):  # noqa: F811
    p, tg, db = setup
    db.add_wallet(USINE, "DEVPROB_STARTUP", "$STARTUP", "dev probable", 1, None)
    p.reload_watchlist()
    for i in range(1, 5):
        _cree(p, USINE, f"CATmint{i}")
    derniere = [t for k, _t, t in tg.sent if k == "create:CATmint3"][0]
    assert "lanceur en série : 3 tokens créés en 24 h" in derniere
    # 5e création en 24 h : plateforme / usine -> plus suivie, plus d'alerte de création
    assert _cree(p, USINE, "CATmint5") is None
    assert USINE not in p.watched and not db.wallet(USINE)["active"]
    assert any(k == f"factory:{USINE}" and topic == "system" for k, topic, _t in tg.sent)
    assert not any(k == "create:CATmint5" for k, _t, _x in tg.sent)
    assert "usine à tokens" in p.decisions_line()


def test_watchlist_de_depart_jamais_retiree(setup):  # noqa: F811
    p, tg, db = setup
    for i in range(1, 7):
        _cree(p, WATCHED, f"DEPmint{i}")
    assert WATCHED in p.watched and db.wallet(WATCHED)["active"]


# --- wallets très actifs -----------------------------------------------------------------------------
PUMPSWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
SWAP = [f"Program {COMPUTE_BUDGET} invoke [1]", f"Program {COMPUTE_BUDGET} success",
        f"Program {PUMPSWAP} invoke [1]", "Program log: Instruction: Buy",
        f"Program {TOKEN} invoke [2]", "Program log: Instruction: InitializeAccount3",
        "Program log: Instruction: TransferChecked", f"Program {PUMPSWAP} success"]
FRAIS = [f"Program {PUMPSWAP} invoke [1]", "Program log: Instruction: CollectCreatorFee", f"Program {PUMPSWAP} success"]
CREATION = ["Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]", "Program log: Instruction: Create",
            f"Program {TOKEN} invoke [2]", "Program log: Instruction: InitializeMint2"]
VIREMENT = [f"Program {SYSTEM_PROGRAM} invoke [1]", f"Program {SYSTEM_PROGRAM} success"]


def test_tri_des_transactions_d_un_wallet_tres_actif():
    assert not notable_logs(SWAP) and not notable_logs(FRAIS)          # trading, frais : pas téléchargés
    assert notable_logs(CREATION) and notable_logs(CREATION, strict=True)
    assert notable_logs(["Program log: Instruction: CreatePool"], strict=True)
    assert notable_logs(["Program log: initialize2: InitializeInstruction2 { nonce: 254 }"], strict=True)
    assert notable_logs(VIREMENT) and not notable_logs(VIREMENT, strict=True)
    assert notable_logs([]) and notable_logs(SWAP + ["Log truncated"])  # logs incomplets : on ne devine pas


# --- journal des décisions ----------------------------------------------------------------------------
def test_chaque_evenement_a_une_raison(setup):  # noqa: F811
    p, _tg, _db = setup

    async def rien(ev):
        return None
    p._on_sell = rien
    asyncio.run(p.process(Event("sell", WATCHED, "s", int(time.time()), MINT)))
    assert "sell sans signal" in p.decisions_line()


def test_refus_a_ne_pas_rater_explique(setup):  # noqa: F811
    p, tg, _db = setup
    _cree(p, WATCHED, "RUGmint1")   # dev du cluster Reserve : jamais « à ne pas rater », et on dit pourquoi
    assert "pas « à ne pas rater »" in p.decisions_line()


# --- fundings ------------------------------------------------------------------------------------------
class FakeRPC:
    async def signatures(self, address, limit=10, **_k):
        return []   # wallet neuf


def test_distributeur_ne_remplit_pas_la_watchlist(setup):  # noqa: F811
    p, tg, db = setup
    p.rpc = FakeRPC()
    for i in range(14):
        asyncio.run(p.process(Event("transfer", WATCHED, f"f{i}", int(time.time()), other=f"NEUF{i:02d}" + "x" * 30,
                                    sol=1.0)))
    ajoutes = db.conn.execute("SELECT COUNT(*) FROM wallets WHERE parent=?", (WATCHED,)).fetchone()[0]
    assert ajoutes == 12 and "financeur en série" in p.decisions_line()


def test_sniper_refinance_reste_retire(setup):  # noqa: F811
    p, tg, db = setup
    sniper = "SNIPxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    db.add_wallet(sniper, "SAT_SNIP", "g", "satellite", 1, WATCHED)
    db.deactivate([sniper])
    db.put(f"sniper:{sniper}", int(time.time()))
    a = asyncio.run(p.process(Event("transfer", WATCHED, "w1", int(time.time()), other=sniper, sol=5.0)))
    assert a is None and not db.wallet(sniper)["active"] and sniper not in p.watched


def test_vieux_token_deja_connu_sans_appel_reseau(setup, monkeypatch):  # noqa: F811
    p, tg, db = setup
    db.upsert_token(MINT, "OLD", "Vieux", "créateur", int(time.time()) - 3 * 86400)

    async def interdit(*a, **k):
        raise AssertionError("analyse réseau inutile")
    monkeypatch.setattr(p, "_info", interdit)
    a = asyncio.run(p.process(Event("buy", WATCHED, "b1", int(time.time()), MINT, sol=0.5, tokens_raw=10)))
    assert a is None and "achat d'un token ancien" in p.decisions_line()


def test_wallet_neuf_du_bank_d_un_dev_a_succes_est_de_confiance(setup):  # noqa: F811
    p, _tg, db = setup
    dev, bank, neuf, relais = ("DEVx" + "a" * 36, "BANKx" + "b" * 35, "NEUFx" + "c" * 35, "RELx" + "d" * 36)
    db.add_wallet(dev, "DEV_WEPE", "découverte", "dev (découverte : $WEPE, MC 11.5 M$ vérifiée DexScreener)", 0, None)
    db.add_wallet(bank, "BANK_WEPE", "découverte", "bank probable (a financé le dev de $WEPE)", 1, dev)
    db.add_wallet(neuf, "NEW_neuf", "découverte", "financé par BANK_WEPE", 2, bank)
    db.add_wallet(relais, "NEW_rel", "découverte", "financé par NEW_neuf", 3, neuf)
    assert [p.trust(a) for a in (dev, bank, neuf, relais)] == ["prouvé", "lié", "lié", "faible"]


def test_vente_d_un_token_non_suivi_sans_appel_reseau(setup, monkeypatch):  # noqa: F811
    p, tg, db = setup
    appels = []
    vrai_info = p._info

    async def espion(*a, **k):
        appels.append(a)
        return await vrai_info(*a, **k)
    monkeypatch.setattr(p, "_info", espion)
    vente = Event("sell", WATCHED, "v1", int(time.time()), MINT, sol=3.0, tokens_raw=10**14, pre_tokens_raw=15 * 10**13)
    assert asyncio.run(p.process(vente)) is None and appels == []
    db.upsert_token(MINT, "ASH", "Ashborn", "autre", int(time.time()) - 600)   # token suivi : 15 % vendus = alerte
    a = asyncio.run(p.process(Event("sell", WATCHED, "v2", int(time.time()), MINT, sol=3.0, tokens_raw=10**14,
                                    pre_tokens_raw=15 * 10**13)))
    assert a is not None and "RÉSERVE VEND" in a.text
