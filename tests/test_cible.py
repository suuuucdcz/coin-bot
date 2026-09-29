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
    def __init__(self, historiques, txs=None):
        self.historiques = historiques
        self.txs = txs or {}                     # adresse -> [(signature, transaction)], de la plus récente à la plus ancienne

    async def signatures(self, address, limit=10, **_k):
        if address in self.txs:
            return [{"signature": s, "err": None} for s, _tx in self.txs[address]][:limit]
        return [{"signature": f"s{i}"} for i in range(self.historiques.get(address, 50))][:limit]

    async def transaction(self, sig):
        return next((tx for lst in self.txs.values() for s, tx in lst if s == sig), None)

    transaction_retry = transaction


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
    assert c.status_line() == "🎯 Cibles : PUMPINU (3 wallets, dont 0 discrets)"


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
    assert c.cible_de(VIEUX) is None and "wallet existant" in _textes(tg)[-2] and "Binance" in _textes(tg)[-1]


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


# --- bundle discret, armement, mélangeur (vus sur $PUMPINU le 19/09) ------------------------------------------------
D1 = "D1bundleaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
D2 = "D2bundlebbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
D3 = "D3bundlecccccccccccccccccccccccccccccccccc"


def _bundle(db, c):
    for d in (D1, D2, D3):
        db.add_wallet(d, f"PUMPINU_W_{d[:4]}", "cible:PUMPINU", "bundle de l'opérateur · suivi discret", 2, DEV)
    c.recharger()


def _achat(w, sol=1.0, mint=MINT):
    return make_tx([(w, True, 5.0, 5.0 - sol), (CURVE, False, 50, 50 + sol)], [PUMP_FUN],
                   tokens=[(w, mint, 0, 12_000_000_000_000)])


def _vente(w, sol=1.2, mint=MINT):
    return make_tx([(w, True, 1.0, 1.0 + sol), (CURVE, False, 50, 50 - sol)], [PUMP_FUN],
                   tokens=[(w, mint, 12_000_000_000_000, 0)])


def test_bundle_discret_silencieux_mais_revele_le_coin(cible, monkeypatch):
    db, tg, p, c = cible
    _bundle(db, c)

    async def tout_neuf(http, mint):
        return {"symbol": "NEW", "name": "New Coin", "created": int(time.time()) - 30, "creator": "CREA" + "i" * 40}
    monkeypatch.setattr(C.pumpfun, "coin", tout_neuf)
    avant = len(tg.sent)
    asyncio.run(c.on_tx("b1", _achat(D1)))
    assert len(tg.sent) == avant                                         # un achat du bundle : rien de raconté
    asyncio.run(c.on_tx("b2", _achat(D2)))
    alerte = [t for t in _textes(tg) if "NOUVEAU COIN" in t]
    assert len(alerte) == 1 and "bundle au lancement" in alerte[0] and tg.top
    asyncio.run(c.on_tx("b3", _vente(D1)))
    assert len([t for t in _textes(tg) if "vend" in t]) == 0             # ses ventes : dans le bilan, pas une par une
    b = c._bundle[("PUMPINU", MINT)]
    assert b["achats"] == 2 and b["ventes"] == 1 and b["wallets"] == {D1, D2}
    assert c.status_line() == "🎯 Cibles : PUMPINU (7 wallets, dont 3 discrets)"      # + son créateur


def test_bundle_hors_des_alertes_ordinaires(cible):
    db, tg, p, c = cible
    _bundle(db, c)
    p.watched |= {D1, W1}
    p.rpc = FakeRPC({}, {D1: [("b4", _achat(D1))], W1: [("b5", _achat(W1))]})
    vus = []

    async def process(ev):
        vus.append(ev.wallet)
    p.process = process
    asyncio.run(p.handle_signature("b4"))
    asyncio.run(p.handle_signature("b5"))
    assert vus == [W1]                                                   # le bundle ne fait pas d'alerte ordinaire


def test_achat_groupe_ignore_un_vieux_coin(cible, monkeypatch):
    db, tg, p, c = cible

    async def vieux(http, mint):
        return {"symbol": "OLD", "created": int(time.time()) - 7200, "creator": "X" * 44}
    monkeypatch.setattr(C.pumpfun, "coin", vieux)
    asyncio.run(c.on_tx("v1", _achat(W1)))
    asyncio.run(c.on_tx("v2", _achat(W2)))
    assert not [t for t in _textes(tg) if "NOUVEAU COIN" in t] and not tg.top


def _virement(src, dst, sol):
    return make_tx([(src, True, 1.0, 1.0 - sol - 0.000005), (dst, False, 0.1, 0.1 + sol)], [SYSTEM])


def test_armement_virements_identiques(cible):
    db, tg, p, c = cible
    sources = [f"MIX{i}" + "m" * 40 for i in range(6)]
    for i, s in enumerate(sources[:4]):
        asyncio.run(c.on_tx(f"a{i}", _virement(s, DEV, 0.0339)))
    asyncio.run(c.on_tx("a9", _virement(sources[5], DEV, 0.5)))        # montant différent : ne compte pas
    assert not [t for t in _textes(tg) if "ARMEMENT EN COURS" in t]
    asyncio.run(c.on_tx("a4", _virement(sources[4], DEV, 0.0340)))
    arme = [t for t in _textes(tg) if "ARMEMENT EN COURS" in t]
    assert len(arme) == 1 and "5 virements identiques" in arme[0] and any("ARMEMENT EN COURS" in t for t in tg.top)
    asyncio.run(c.on_tx("a5", _virement("MIX9" + "m" * 40, DEV, 0.0339)))
    assert len([t for t in _textes(tg) if "ARMEMENT EN COURS" in t]) == 1        # une seule alerte par quart d'heure


def test_rafale_suivie_en_silence_puis_armement(cible):
    """Le 19/09, le lanceur a financé 100+ wallets neufs en 1 min, montants tous différents : 2 racontés, puis la
    rafale est suivie en silence (en tâche de fond) et l'armement part au 8e wallet neuf."""
    db, tg, p, c = cible
    neufs = [f"N{i}neuf" + "n" * 37 for i in range(9)]
    p.rpc = FakeRPC({n: 1 for n in neufs})

    async def go():
        for i, n in enumerate(neufs[:8]):
            sol = 0.05 + 0.013 * i
            await c.on_tx(f"m{i}", make_tx([(DEV, True, 5.0, 5.0 - sol - 0.000005), (n, False, 0, sol)], [SYSTEM]))
        for _ in range(30):
            await asyncio.sleep(0)
        textes = _textes(tg)
        assert sum("finance un wallet NEUF" in t for t in textes) == 2 and sum("en rafale" in t for t in textes) == 1
        assert all(c.cible_de(n) == "PUMPINU" for n in neufs[:8]) and set(neufs[2:8]) <= c.discrets
        assert set(neufs[2:8]) <= db.cible_discrets()                      # mémorisé en base (redémarrage)
        assert set(neufs[:2]) <= p.watched and not set(neufs[2:8]) & p.watched   # relais : membres, sans abonnement
        arme = [t for t in textes if "ARMEMENT EN COURS" in t]
        assert len(arme) == 1 and "8 wallets neufs" in arme[0] and any("ARMEMENT EN COURS" in t for t in tg.top)
        nb = len(tg.sent)
        await c.on_tx("m9", make_tx([(neufs[3], True, 0.1, 0.09), (neufs[8], False, 0, 0.01 - 0.000005)], [SYSTEM]))
        for _ in range(30):
            await asyncio.sleep(0)
        assert len(tg.sent) == nb and neufs[8] in c.discrets               # financé par un discret : discret, sans bruit
    asyncio.run(go())


def test_rattrapage_d_un_wallet_neuf(cible, monkeypatch):
    """Un mélangeur enchaîne plus vite que l'abonnement : les 1res tx du wallet neuf sont relues."""
    db, tg, p, c = cible
    monkeypatch.setattr(C, "RATTRAPAGE_S", (0,))
    crea = make_tx([(NEUF, True, 1.0, 0.97), (MINT, True, 0, 0.0015)], [PUMP_FUN, TOKEN],
                   tokens=[(NEUF, MINT, 0, 30_000_000_000_000)],
                   parsed=[{"parsed": {"type": "initializeMint2", "info": {"mint": MINT, "decimals": 6,
                                                                           "mintAuthority": NEUF}}}])
    p.rpc = FakeRPC({}, {NEUF: [("crea", crea)]})                          # 1 tx : neuf ; celle-ci a été manquée

    async def go():
        await c.on_tx("f1", make_tx([(DEV, True, 2.0, 0.999995), (NEUF, False, 0, 1.0)], [SYSTEM]))
        for _ in range(20):
            await asyncio.sleep(0)
    asyncio.run(go())
    assert [t for t in _textes(tg) if "NOUVEAU COIN" in t and MINT in t]


def test_file_prioritaire():
    from radar.telegram import FileAlertes

    async def go():
        f = FileAlertes()
        f.put_nowait("a")
        f.put_nowait("b")
        f.put_urgent("coin")
        assert f.qsize() == 3 and not f.empty()
        return [await f.get() for _ in range(3)]
    assert asyncio.run(go()) == ["coin", "a", "b"]


def test_armement_par_des_relais_deja_suivis(cible):
    """Le mélangeur suivi fait entrer ses relais dans la cible : leurs virements identiques comptent quand même."""
    db, tg, p, c = cible
    relais = [f"R{i}relais" + "r" * 35 for i in range(5)]
    for r in relais:
        db.add_wallet(r, f"PUMPINU_W_{r[:4]}", "cible:PUMPINU", "wallet neuf · suivi discret", 3, DEV)
    c.recharger()
    for i, r in enumerate(relais):
        asyncio.run(c.on_tx(f"r{i}", _virement(r, DEV, 0.0339)))
    assert [t for t in _textes(tg) if "ARMEMENT EN COURS" in t]


def test_purge_des_relais_vides(cible):
    """Un mélangeur ajoute des centaines de relais : vidés, ils sortent de la surveillance au bout de 2 h."""
    db, tg, p, c = cible
    vide, plein = "VIDE" + "v" * 40, "PLEIN" + "p" * 39
    for a in (vide, plein):
        db.add_wallet(a, f"PUMPINU_W_{a[:4]}", "cible:PUMPINU", "wallet neuf financé par X (0.1 SOL) · suivi discret", 3, DEV)
    db.conn.execute("UPDATE wallets SET added_at = 0 WHERE address IN (?, ?)", (vide, plein))
    db.conn.commit()
    c.recharger()

    class RPC(FakeRPC):
        async def call(self, method, params):
            assert method == "getMultipleAccounts"
            return {"value": [{"lamports": 5_000_000_000} if a == plein else None for a in params[0]]}
    p.rpc = RPC({})
    p.watched |= {vide, plein}
    assert asyncio.run(c.purger()) == 1
    assert c.cible_de(vide) is None and c.cible_de(plein) == "PUMPINU" and vide not in p.watched
    assert db.cible_membres().get(vide) is None and c.cible_de(W1) == "PUMPINU"   # les wallets clés ne sont pas touchés
