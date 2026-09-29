"""🚀 Lancements qui décollent : du token au wallet neuf d'un dev connu (même hors watchlist)."""
import asyncio
import time

from conftest import make_tx
from test_top import setup  # noqa: F401  (fixture partagée)

from radar import config as cfgmod
from radar import lancements as L
from radar import pipeline as pl

CREATEUR = "C94Xcreateurccccccccccccccccccccccccccccc"
RELAIS = "77zDrelaisrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
BANK = "BANKweperrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
DEV_WEPE = "DEVwepeddddddddddddddddddddddddddddddddd"


class FakeRPC:
    """Créateur : 0,01 SOL d'un leurre, puis 5 SOL d'un relais (2 tx) lui-même financé par le bank."""
    def __init__(self, bank=BANK):
        self.txs = {
            "c1": make_tx([("LEURRExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", True, 1, 0.989995), (CREATEUR, False, 0, 0.01)], []),
            "c2": make_tx([(RELAIS, True, 5.1, 0.099995), (CREATEUR, False, 0.01, 5.01)], []),
            "r1": make_tx([(bank, True, 100, 94.899995), (RELAIS, False, 0, 5.1)], []),
        }
        self.hist = {CREATEUR: [("c2", 1030), ("c1", 1000)], RELAIS: [("c2", 1030), ("r1", 1010)]}

    async def all_signatures(self, address, max_pages=10):
        return [{"signature": s, "blockTime": t, "err": None} for s, t in self.hist.get(address, [])], False

    async def transaction(self, sig):
        return self.txs.get(sig)

    async def signatures(self, address, limit=10, **_k):
        return [{"signature": s} for s, _t in self.hist.get(address, [])][:limit]


def _veille(db, tg, monkeypatch, marches, rpc=None):
    p = pl.Pipeline(cfgmod.load(), db, rpc=rpc or FakeRPC(), http=object(), tg=tg)

    async def markets(http, mints):
        return {m: v for m, v in marches.items() if m in mints}
    monkeypatch.setattr(L.dexscreener, "markets", markets)
    return p, L.LaunchWatch(p)


def _token(w, mint, age_s, creator=CREATEUR):
    w.on_new_token({"mint": mint, "traderPublicKey": creator, "symbol": "CAT", "name": "Cat"})
    w.pending[mint]["ts"] = time.time() - age_s


def test_tri_des_lancements_avant_toute_remontee(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    db.add_wallet("SUIVIxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "DEV_SUIVI", "g", "dev", 0, None)
    p, w = _veille(db, tg, monkeypatch, {"CALME": {"mc": 6_000, "txns24h": 12}, "JEUNE": {"mc": 90_000, "txns24h": 300}})
    p.reload_watchlist()
    w.on_new_token({"mint": "X", "traderPublicKey": "SUIVIxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"})
    assert "X" not in w.pending                          # créateur déjà suivi : le pipeline s'en occupe
    _token(w, "CALME", 200)
    _token(w, "JEUNE", 60)                               # moins de 3 min : pas encore regardé
    _token(w, "PASINDEXE", 200)                          # pas encore sur DexScreener : réessayé
    _token(w, "VIEUX", 900)                              # plus de 10 min : abandonné
    assert asyncio.run(w.check_once()) == []
    assert set(w.pending) == {"JEUNE", "PASINDEXE"} and w.stats["decollent"] == 0


def test_wallet_neuf_finance_par_le_bank_d_un_dev_connu(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    db.add_wallet(DEV_WEPE, "DEV_WEPE", "découverte", "dev (découverte : $WEPE, MC 11.5 M$ vérifiée DexScreener)", 0, None)
    db.add_wallet(BANK, "BANK_WEPE", "découverte", "bank probable (a financé le dev de $WEPE)", 1, DEV_WEPE)
    db.deactivate([BANK])                                # sorti de la watchlist : connu quand même
    p, w = _veille(db, tg, monkeypatch, {"MINTa": {"mc": 60_000, "txns24h": 180}})
    _token(w, "MINTa", 200)
    trouves = asyncio.run(w.check_once())
    assert len(trouves) == 1 and trouves[0]["label"] == "BANK_WEPE" and trouves[0]["genre"] == "bon"
    assert [h["src"] for h in trouves[0]["chaine"]] == [RELAIS, BANK]      # leurre écarté, relais remonté
    texte = [t for t in tg.sent if "NOUVEAU WALLET D'UN DEV CONNU" in t][0]
    assert "BANK_WEPE" in texte and "leurre" not in texte.split("←")[0]
    assert db.wallet(CREATEUR)["active"] and db.wallet(CREATEUR)["grp"] == "découverte"   # créateur suivi


def test_reseau_a_rugs_qui_relance_part_dans_arnaques(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    db.add_wallet(BANK, "BANK_WOTF_GVXP", "reserve-suspect", "financeur lié au cluster reserve-suspect", 1, None)
    p, w = _veille(db, tg, monkeypatch, {"MINTb": {"mc": 110_000, "txns24h": 400}})
    _token(w, "MINTb", 200)
    trouves = asyncio.run(w.check_once())
    assert trouves and trouves[0]["genre"] == "rug"
    assert any("RÉSEAU À RUGS RELANCE" in t for t in tg.sent) and tg.top == []


def test_plafond_horaire_des_remontees(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    monkeypatch.setattr(L, "TRACES_PER_HOUR", 2)
    marches = {f"M{i}": {"mc": 60_000 + i, "txns24h": 60} for i in range(5)}
    p, w = _veille(db, tg, monkeypatch, marches)
    for i in range(5):
        _token(w, f"M{i}", 200, creator=f"INCONNU{i}" + "i" * 33)
    asyncio.run(w.check_once())
    assert w.stats["decollent"] == 5 and w.stats["remontes"] == 2


def test_createur_deja_connu_alerte_des_la_creation(setup, monkeypatch):  # noqa: F811
    # Un dev connu, sorti de la watchlist, relance avec le même wallet : alerte immédiate, sans attendre 3 min
    db, tg = setup
    db.add_wallet(DEV_WEPE, "DEV_WEPE", "découverte", "dev (découverte : $WEPE, MC 11.5 M$ vérifiée DexScreener)", 0, None)
    db.deactivate([DEV_WEPE])
    p, w = _veille(db, tg, monkeypatch, {})

    async def go():
        w.on_new_token({"mint": "MINTnouveau", "traderPublicKey": DEV_WEPE, "symbol": "NEW", "name": "New"})
        for _ in range(40):
            await asyncio.sleep(0)
    asyncio.run(go())
    assert "MINTnouveau" not in w.pending and w.stats["trouves"] == 1
    assert any("NOUVEAU WALLET D'UN DEV CONNU" in t and "dès la création" in t for t in tg.sent)


def test_remontee_helius_reservee_aux_vrais_decollages(setup, monkeypatch):  # noqa: F811
    # Quota Helius (29/09) : 674 remontées pour 1 seul lien trouvé ; en dessous de 50 k$, plus de remontée par Helius
    db, tg = setup
    p, w = _veille(db, tg, monkeypatch, {"PETIT": {"mc": 25_000, "txns24h": 90}})
    _token(w, "PETIT", 200, creator="INCONNUpetitiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
    asyncio.run(w.check_once())
    assert w.stats["decollent"] == 1 and w.stats["remontes"] == 0


def test_rafale_d_un_meme_operateur_regroupee(setup, monkeypatch):  # noqa: F811
    # Vu en vrai (29/09) : un opérateur de faux coins a lancé 75 faux $BOB en 1 h 20, une alerte par token
    db, tg = setup
    db.add_wallet(BANK, "FAUX_BOB_3fsb", "faux-coins", "organisateur de faux coins", 1, None)
    p, w = _veille(db, tg, monkeypatch, {})
    base = {"genre": "rug", "wallet": BANK, "label": "FAUX_BOB_3fsb", "grp": "faux-coins", "role": "", "depth": 1,
            "chaine": [], "mc": None, "txns": None, "symbol": "BOB"}

    async def go():
        for i in range(7):
            await w.alerter({**base, "mint": f"BOB{i}" + "x" * 36, "creator": f"CRE{i}" + "c" * 36})
    asyncio.run(go())
    assert sum("UN RÉSEAU À RUGS RELANCE" in s for s in tg.sent) == 3 and w.stats["regroupes"] == 4
    w.bilan_regroupes(time.time() + 20 * 60)
    bilan = [s for s in tg.sent if "lancements de plus" in s]
    assert len(bilan) == 1 and "4 lancements de plus" in bilan[0] and "$BOB ×4" in bilan[0]
    assert not db.wallet("CRE6" + "c" * 36)          # les créateurs d'une rafale ne remplissent pas la watchlist
