"""🎯 Cibles : un dev suivi de près dans sa section — tout raconté, nouveaux wallets suivis, prochain coin repéré."""
import asyncio
import time

import pytest
from conftest import PUMP_FUN, SYSTEM, TOKEN, make_tx

from radar import cible as C
from radar import config as cfgmod
from radar import pipeline as pl
from radar.db import DB

DEV = "BZ7ndevdddddddddddddddddddddddddddddddddd"
W1 = "W1pumpinuwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"
W2 = "W2pumpinuvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
NEUF = "NEUFaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
VIEUX = "VIEUXbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
BINANCE = "BINAcccccccccccccccccccccccccccccccccccccc"
MINT = "NEWcoinmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmpump"
CURVE = "CURVEkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"


class FakeTG:
    """Comme le vrai : une clé déjà envoyée ne repart pas."""
    def __init__(self):
        self.sent, self.top, self.topics, self.pins, self.cles = [], [], {}, [], set()

    def enqueue(self, text, markup=None, key=None, kind="", topic=None, on_sent=None, **_kw):
        if key and key in self.cles:
            return False
        self.cles.add(key)
        self.sent.append((topic, kind, text))
        if on_sent:
            on_sent(len(self.sent))
        return True

    def enqueue_top(self, text, markup=None, key=None, **_kw):
        if key and key in self.cles:
            return False
        self.cles.add(key)
        self.top.append(text)
        return True

    def add_topic(self, key, emoji, title, color):
        self.topics[key] = title

    async def ensure_topic(self, key):
        return None

    async def pin(self, mid):
        self.pins.append(mid)


class FakeRPC:
    def __init__(self, historiques):
        self.historiques = historiques

    async def signatures(self, address, limit=10, **_k):
        return [{"signature": f"s{i}"} for i in range(self.historiques.get(address, 50))][:limit]


@pytest.fixture
def cible(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    tg = FakeTG()
    db.cible_add("PUMPINU", DEV, "Dev de $PUMPINU")
    db.add_wallet(DEV, "DEV_PUMPINU_BZ7n", "cible:PUMPINU", "dev (cible PUMPINU)", 1, None)
    for w in (W1, W2):
        db.add_wallet(w, f"PUMPINU_W_{w[:4]}", "cible:PUMPINU", "wallet du dev", 2, DEV)
    db.set_label(BINANCE, "Binance (hot wallet)")
    p = pl.Pipeline(cfgmod.load(), db, rpc=FakeRPC({NEUF: 1, VIEUX: 40}), http=object(), tg=tg)
    c = C.Cibles(p, tg)
    p.cibles = c

    async def coin(http, mint):
        return {"symbol": "NEW", "name": "New Coin", "created": int(time.time()) - 120, "creator": "CREAinconnu" + "i" * 33}
    monkeypatch.setattr(C.pumpfun, "coin", coin)

    async def prep():
        for nom in c.recharger():
            await c.preparer(nom)
    asyncio.run(prep())
    yield db, tg, p, c
    db.close()


def _textes(tg, topic="cible_pumpinu"):
    return [t for (tp, _k, t) in tg.sent if tp == topic]


def test_section_et_fiche_epinglee(cible):
    db, tg, p, c = cible
    assert tg.topics == {"cible_pumpinu": "Cible PUMPINU"} and "CIBLE PUMPINU" in _textes(tg)[0] and tg.pins
    assert c.status_line() == "🎯 Cibles : PUMPINU (3 wallets)"


def test_wallet_neuf_finance_rejoint_la_cible(cible):
    db, tg, p, c = cible
    tx = make_tx([(DEV, True, 2.0, 1.909995), (NEUF, False, 0, 0.0905)], [SYSTEM])
    asyncio.run(c.on_tx("sig1", tx))
    texte = _textes(tg)[-1]
    assert "finance un wallet NEUF" in texte and NEUF in texte
    assert c.cible_de(NEUF) == "PUMPINU" and NEUF in p.watched and db.wallet(NEUF)["grp"] == "cible:PUMPINU"
    # vers un vieux wallet (40 tx) : raconté, pas ajouté ; vers un exchange : signalé
    asyncio.run(c.on_tx("sig2", make_tx([(DEV, True, 1.9, 1.399995), (VIEUX, False, 3, 3.5)], [SYSTEM])))
    asyncio.run(c.on_tx("sig3", make_tx([(DEV, True, 1.4, 0.399995), (BINANCE, False, 900, 901)], [SYSTEM])))
    assert c.cible_de(VIEUX) is None and "wallet existant" in _textes(tg)[-2] and "vers un exchange" in _textes(tg)[-1]


def test_sol_recu_et_transfert_interne(cible):
    db, tg, p, c = cible
    asyncio.run(c.on_tx("sig4", make_tx([(BINANCE, True, 900, 897.999995), (DEV, False, 0.1, 2.1)], [SYSTEM])))
    assert "reçoit</b> 2.0000 SOL de <b>Binance (hot wallet)</b>" in _textes(tg)[-1]
    asyncio.run(c.on_tx("sig5", make_tx([(DEV, True, 2.1, 1.599995), (W1, False, 0, 0.5)], [SYSTEM])))
    assert "entre ses wallets" in _textes(tg)[-1]


def test_creation_par_un_wallet_de_la_cible(cible):
    db, tg, p, c = cible
    tx = make_tx([(W1, True, 1.0, 0.97), (MINT, True, 0, 0.0015)], [PUMP_FUN, TOKEN],
                 tokens=[(W1, MINT, 0, 30_000_000_000_000)],
                 parsed=[{"parsed": {"type": "initializeMint2", "info": {"mint": MINT, "decimals": 6,
                                                                         "mintAuthority": W1}}}])
    asyncio.run(c.on_tx("sig6", tx))
    alertes = [t for t in _textes(tg) if "NOUVEAU COIN" in t]
    assert len(alertes) == 1 and MINT in alertes[0] and "$NEW" in alertes[0]
    assert len(tg.top) == 1 and len(tg.pins) == 2                      # copie dans ‼️ + épinglée (et la fiche)

    async def flux():   # le même coin vu ensuite par le flux pump.fun : une seule alerte (clé commune)
        c.on_new_token({"mint": MINT, "traderPublicKey": W1, "symbol": "NEW", "name": "New"})
        for _ in range(5):
            await asyncio.sleep(0)
    asyncio.run(flux())
    assert len([t for t in _textes(tg) if "NOUVEAU COIN" in t]) == 1 and len(tg.top) == 1


def test_achat_groupe_revele_le_coin_d_un_wallet_inconnu(cible):
    db, tg, p, c = cible

    def achat(w, sig):
        return c.on_tx(sig, make_tx([(w, True, 1.0, 0.7), (CURVE, False, 50, 50.3)], [PUMP_FUN],
                                    tokens=[(w, MINT, 0, 12_000_000_000_000)]))
    asyncio.run(achat(W1, "sig7"))
    assert not [t for t in _textes(tg) if "NOUVEAU COIN" in t] and "achète" in _textes(tg)[-1]
    asyncio.run(achat(W2, "sig8"))
    alerte = [t for t in _textes(tg) if "NOUVEAU COIN" in t]
    assert len(alerte) == 1 and "2 wallets de la cible l'achètent" in alerte[0] and tg.top
    assert c.cible_de("CREAinconnu" + "i" * 33) == "PUMPINU"            # son créateur rejoint la cible


def test_flux_pumpfun_et_toile_en_secours(cible):
    db, tg, p, c = cible

    async def go():
        c.on_new_token({"mint": MINT, "traderPublicKey": W2, "symbol": "NEW", "name": "New"})
        for _ in range(5):
            await asyncio.sleep(0)
        t = {"mint": "AUTREmint" + "m" * 35, "creator": "INCONNUcreateur" + "c" * 29, "symbol": "OTH"}
        return await c.lien_toile(t, [{"src": "RELAIS" + "r" * 38, "sol": 1.0}, {"src": NEUF, "sol": 0.5},
                                      {"src": W1, "sol": 0.09}])
    assert asyncio.run(go())
    alertes = [t for t in _textes(tg) if "NOUVEAU COIN" in t]
    assert len(alertes) == 2 and "flux pump.fun" in alertes[0] and "repéré par la toile" in alertes[1]
    assert c.cible_de("INCONNUcreateur" + "c" * 29) == "PUMPINU"


def test_jamais_filtree_ni_retiree(cible):
    db, tg, p, c = cible
    from radar.analysis.classify import Event
    assert asyncio.run(p._sniper_check(Event("buy", W1, "s", int(time.time()), MINT))) is False
    assert not p._farm_check("cible:PUMPINU", MINT)
    db.conn.execute("UPDATE wallets SET added_at = 0")
    db.conn.commit()
    assert W1 not in db.stale_wallets(1)                                # jamais purgée pour inactivité
