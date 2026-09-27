"""🧠 Smart money : gros détenteurs de plusieurs vrais succès ; alerte quand plusieurs entrent ensemble."""
import asyncio
import time

from conftest import MINT
from test_top import setup  # noqa: F401  (fixture partagée)

from radar import config as cfgmod
from radar import pipeline as pl
from radar import smart as S
from radar.analysis.classify import Event

SMART = [f"SMART{i}" + "s" * 34 for i in range(3)]
POOL, CEX, RUG = "POOL" + "p" * 36, "CEXX" + "c" * 36, "RUGG" + "r" * 36
DEVS = [f"DEV{i}" + "d" * 36 for i in range(3)]


class FakeRPC:
    """Même trio de gros détenteurs sur chaque succès, plus le dev du token, un pool, un exchange et un rugger."""
    def __init__(self):
        self.mint = None

    async def mint_info(self, mint):
        self.mint = mint
        return {"supply": "1000000"}

    async def token_largest_accounts(self, mint):
        dev = DEVS[int(mint[-1])]
        self.owners = SMART + [dev, POOL, CEX, RUG]
        return [{"address": f"ata{i}", "amount": str(50_000 - i * 1000)} for i in range(len(self.owners))]

    async def call(self, method, params):
        adresses = params[0]
        if params[1]["encoding"] == "jsonParsed":
            return {"value": [{"data": {"parsed": {"info": {"owner": o}}}} for o in self.owners]}
        return {"value": [{"owner": "pAMMprogramme" if a == POOL else S.SYSTEM} for a in adresses]}


def test_gros_detenteurs_de_plusieurs_succes_deviennent_smart_money(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    db.set_label(CEX, "hot wallet Binance")
    db.add_wallet(RUG, "DEV_RUG", "reserve-cluster", "dev", 0, None)
    for i, d in enumerate(DEVS):   # devs jugés propres par la découverte
        db.add_wallet(d, f"DEV_W{i}", "découverte", f"dev (découverte : $W{i})", 0, None)
    p = pl.Pipeline(cfgmod.load(), db, rpc=FakeRPC(), http=None, tg=tg)
    succes = [{"mint": f"WIN{'w' * 36}{i}", "creator": DEVS[i], "symbol": f"W{i}"} for i in range(3)]

    async def candidats(http, min_ath):
        return succes
    monkeypatch.setattr(S.discovery, "candidates", candidats)
    nouveaux = asyncio.run(S.SmartMoney(p).scan_once())
    assert sorted(nouveaux) == sorted(SMART)                       # ni le dev, ni le pool, ni l'exchange, ni le rugger
    w = db.wallet(SMART[0])
    assert w["grp"] == S.SMART_GROUP and w["role"].startswith("smart money : gros détenteur de 3 succès")
    assert all(db.wallet(d)["grp"] == "découverte" for d in DEVS) and db.wallet(CEX) is None
    assert any("smart money ajouté" in t for t in tg.sent)
    assert asyncio.run(S.SmartMoney(p).scan_once()) == []          # succès déjà vus : rien de plus


def _smart_pipeline(db, tg):
    for a in SMART:
        db.add_wallet(a, f"SMART_{a[:4]}", S.SMART_GROUP, "smart money : gros détenteur de 3 succès", 1, None)
    return pl.Pipeline(cfgmod.load(), db, rpc=None, http=None, tg=tg)


def _achats(p, wallets, mint=MINT):
    async def go():
        for i, a in enumerate(wallets):
            await p.process(Event("buy", a, f"b{i}{a[:5]}", int(time.time()), mint, sol=1.0, tokens_raw=10**12))
        for _ in range(60):
            await asyncio.sleep(0)
    asyncio.run(go())


def test_trois_smart_money_ensemble_a_ne_pas_rater(setup, monkeypatch):  # noqa: F811
    from radar.analysis.enrich import TokenInfo
    db, tg = setup

    async def info_complete(rpc, http, mint, creator_hint=None, dev=True):
        return TokenInfo(mint, name="Cat", symbol="CAT", creator=DEVS[0], created_ts=int(time.time()) - 120,
                         supply_raw=10**15, dev_coins=[], mc_usd=30_000, top10_pct=15.0, dev_pct=2.0)
    monkeypatch.setattr(pl, "token_info", info_complete)
    p = _smart_pipeline(db, tg)
    _achats(p, SMART[:2])
    assert any("SMART MONEY ENTRE (2 wallets)" in t for t in tg.sent) and tg.top == []   # 2 : alerte, pas ‼️
    _achats(p, SMART[2:])
    assert len(tg.top) == 1 and "SMART MONEY ENTRE (3 wallets)" in tg.top[0][0]


def test_smart_money_jamais_reclasse_sniper_ni_ferme(setup):  # noqa: F811
    db, tg = setup
    p = _smart_pipeline(db, tg)
    for i in range(7):                                             # 7 tokens différents en quelques minutes
        _achats(p, SMART[:1], mint=f"TOK{i}" + "t" * 37)
    assert SMART[0] in p.watched and not p.is_sniper(SMART[0]) and not p.is_farm(S.SMART_GROUP)


def test_parts_identiques_ecartees_bundle():
    # Vu en vrai sur $AMERICA : 14 gros détenteurs à 0,98 % chacun = un seul opérateur
    gros = [(f"B{i}", 0.98) for i in range(14)] + [("VRAI", 2.4), ("AUTRE", 1.1)]
    assert S.sans_bundles(gros) == [("VRAI", 2.4), ("AUTRE", 1.1)]


def test_succes_d_un_dev_non_verifie_ignore(setup, monkeypatch):  # noqa: F811
    # Vu en vrai : $VSOF et $AOR (cluster Reserve) passaient pour des « succès »
    db, tg = setup
    db.add_wallet(DEVS[0], "DEV_VSOF", "reserve-suspect", "dev (découverte : $VSOF)", 1, None)
    p = pl.Pipeline(cfgmod.load(), db, rpc=FakeRPC(), http=None, tg=tg)

    async def candidats(http, min_ath):
        return [{"mint": f"WIN{'w' * 36}0", "creator": DEVS[0], "symbol": "VSOF"},
                {"mint": f"WIN{'w' * 36}1", "creator": DEVS[1], "symbol": "INCONNU"}]
    monkeypatch.setattr(S.discovery, "candidates", candidats)
    asyncio.run(S.SmartMoney(p).scan_once())
    assert db.conn.execute("SELECT COUNT(*) FROM smart_hits").fetchone()[0] == 0
