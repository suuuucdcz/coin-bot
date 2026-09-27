"""🕸️ La toile : wallets neufs confirmés, relais, réutilisés écartés, succès à 24 h, financeurs promus, alertes."""
import asyncio
import time

from conftest import PUMP_FUN, SYSTEM, make_tx
from test_top import setup  # noqa: F401  (fixture partagée)

from radar import config as cfgmod
from radar import lancements as L
from radar import pipeline as pl
from radar import toile as T
from radar.sources.helius import RpcError

BANK = "BANKtoilebbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
RELAIS = "RELAtoilerrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
LEURRE = "LEURtoilellllllllllllllllllllllllllllllll"
H = 3600


def fund(src, dst, sol, dst_avant=0.0):
    return make_tx([(src, True, 500, 500 - sol - 0.000005), (dst, False, dst_avant, dst_avant + sol)], [SYSTEM])


def create(creator):
    return make_tx([(creator, True, 1.0, 0.97)], [PUMP_FUN])


class FakePublic:
    """publicnode : historique récent seulement (signatures de la plus récente à la plus ancienne)."""
    def __init__(self):
        self.hist: dict[str, list[tuple[str, int]]] = {BANK: [(f"b{i}", 900 - i) for i in range(10)]}
        self.txs: dict[str, dict] = {}
        self.muet = False
        self.calls = 0

    def neuf(self, creator, src=BANK, sol=1.5, avant=0.0, t=1000):
        self.txs[f"f_{creator}"] = fund(src, creator, sol, avant)
        self.txs[f"c_{creator}"] = create(creator)
        self.hist[creator] = [(f"c_{creator}", t + 60), (f"f_{creator}", t)]

    async def signatures(self, address, limit=1000, before=None, until=None):
        self.calls += 1
        if self.muet:
            raise RpcError("HTTP 503")
        return [{"signature": s, "blockTime": b, "err": None} for s, b in self.hist.get(address, [])][:limit]

    async def transaction(self, sig):
        self.calls += 1
        return self.txs.get(sig)

    def recent_failures(self, window_s=600):
        return 0

    async def close(self):
        pass


class FakeArchive(FakePublic):
    """RPC officiel : historique complet. `anciens` = wallets qui ont des tx avant ce que voit publicnode."""
    def __init__(self, anciens=()):
        super().__init__()
        self.anciens = set(anciens)

    async def signatures(self, address, limit=1000, before=None, until=None):
        self.calls += 1
        return [{"signature": "vieille", "blockTime": 1, "err": None}] if address in self.anciens else []


def _toile(db, tg, pub, arc=None, http=None):
    p = pl.Pipeline(cfgmod.load(), db, rpc=None, http=http, tg=tg)
    p.lancements = L.LaunchWatch(p)
    t = T.Toile(p, public=pub, archive=arc or FakeArchive())
    p.toile = t
    return p, t


def _token(t, creator, mint, age_s=0):
    return asyncio.run(t.traiter({"mint": mint, "creator": creator, "symbol": mint[:4], "ts": time.time() - age_s}))


def test_createur_neuf_relie_a_sa_racine_par_un_relais(setup):  # noqa: F811
    db, tg = setup
    pub = FakePublic()
    creator = "C1neufcccccccccccccccccccccccccccccccccccc"
    pub.neuf(creator, src=RELAIS, sol=1.99)
    pub.txs["r_in"] = fund(BANK, RELAIS, 2.0)
    pub.hist[RELAIS] = [(f"f_{creator}", 1000), ("r_in", 990)]         # 2 tx : entrée, sortie
    p, t = _toile(db, tg, pub)
    _token(t, creator, "MINT1")
    w = db.toile_wallet(creator)
    assert (w["statut"], w["source"], w["racine"]) == ("neuf", RELAIS, BANK)
    assert db.toile_wallet(RELAIS)["statut"] == "relais"
    assert [h["src"] for h in t.chaine(creator)] == [RELAIS, BANK]
    assert db.conn.execute("SELECT creator FROM toile_tokens WHERE mint='MINT1'").fetchone()[0] == creator


def test_wallet_vide_puis_refinance_n_est_pas_neuf(setup):  # noqa: F811
    # Vu en vrai : publicnode ne montre que 1,7 jour ; le wallet existait depuis 2025, vidé puis refinancé
    db, tg = setup
    pub = FakePublic()
    creator = "C2reutilisecccccccccccccccccccccccccccccc"
    pub.neuf(creator)
    p, t = _toile(db, tg, pub, FakeArchive(anciens={creator}))
    assert _token(t, creator, "MINT2") is None
    assert db.toile_wallet(creator)["statut"] == "reutilise" and t.chaine(creator) is None
    assert db.conn.execute("SELECT COUNT(*) FROM toile_tokens").fetchone()[0] == 0


def test_wallet_qui_avait_deja_du_sol_ecarte_sans_appel_d_archive(setup):  # noqa: F811
    db, tg = setup
    pub, arc = FakePublic(), FakeArchive()
    creator = "C3anciennnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
    pub.neuf(creator, avant=0.4)
    p, t = _toile(db, tg, pub, arc)
    _token(t, creator, "MINT3")
    assert db.toile_wallet(creator)["statut"] == "reutilise" and arc.calls == 0


def test_wallet_tres_actif_et_leurre(setup):  # noqa: F811
    db, tg = setup
    pub = FakePublic()
    bot = "C4botttttttttttttttttttttttttttttttttttttt"
    pub.hist[bot] = [(f"s{i}", 5000 - i) for i in range(1000)]
    malin = "C5malinnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
    pub.txs["leurre"] = fund(LEURRE, malin, 0.01)
    pub.txs["vrai"] = fund(BANK, malin, 3.0, dst_avant=0.01)
    pub.hist[malin] = [("vrai", 1001), ("leurre", 1000)]
    p, t = _toile(db, tg, pub)
    _token(t, bot, "MINT4")
    _token(t, malin, "MINT5")
    assert db.toile_wallet(bot)["statut"] == "actif"
    w = db.toile_wallet(malin)
    assert (w["source"], w["racine"], w["leurre"]) == (BANK, BANK, LEURRE)


def test_rpc_public_muet_rien_n_est_enregistre(setup):  # noqa: F811
    db, tg = setup
    pub = FakePublic()
    pub.muet = True
    p, t = _toile(db, tg, pub)
    creator = "C6muettttttttttttttttttttttttttttttttttttt"
    assert _token(t, creator, "MINT6") is None
    assert db.toile_wallet(creator) is None and t.stats["erreurs"] == 1   # réessayé à son prochain token


def _createurs(pub, t, n, age_s=25 * H, src=BANK):
    out = []
    for i in range(n):
        c = f"C{i}succes" + "s" * (34 - len(str(i)))
        pub.neuf(c, src=src)
        _token(t, c, f"MINTs{i}", age_s)
        out.append(c)
    return out


def _marches(monkeypatch, valeurs):
    async def markets(http, mints):
        return {m: {"mc": valeurs[m]} for m in mints if m in valeurs}
    monkeypatch.setattr(T.dexscreener, "markets", markets)


def test_financeur_a_succes_promu_puis_alerte_des_la_creation(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    pub = FakePublic()
    p, t = _toile(db, tg, pub, http=object())
    _createurs(pub, t, 4)
    _marches(monkeypatch, {"MINTs0": 120_000, "MINTs1": 64_000, "MINTs2": 5_000})   # MINTs3 : plus indexé = mort
    assert asyncio.run(t.mesurer()) == 4
    assert db.get(f"toile_promu:{BANK}") and p.trust(BANK) == "prouvé" and BANK in p.watched
    assert db.wallet(BANK)["role"].startswith("bank à succès (toile : 2 créateurs neufs sur 4")
    assert any("TOILE : nouveau financeur à succès" in s and "$MINT" in s for s in tg.sent)
    # Son prochain wallet neuf crée un token : relié dès la création, sans attendre qu'il décolle
    nouveau = "C9nouveaunnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
    pub.neuf(nouveau)
    p.lancements.pending["MINTnew"] = {"mint": "MINTnew"}
    res = _token(t, nouveau, "MINTnew")
    assert res and res["genre"] == "bon" and res["label"] == f"TOILE_{BANK[:4]}"
    assert "MINTnew" not in p.lancements.pending
    assert any("NOUVEAU WALLET D'UN DEV CONNU" in s and "repéré dès la création" in s for s in tg.sent)


def test_pas_de_promotion_sous_25_pour_cent_ni_pour_un_service(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    pub = FakePublic()
    p, t = _toile(db, tg, pub, http=object())
    _createurs(pub, t, 10)
    _marches(monkeypatch, {"MINTs0": 90_000, "MINTs1": 70_000})                      # 2 sur 10 = 20 %
    asyncio.run(t.mesurer())
    assert not db.get(f"toile_promu:{BANK}") and BANK not in p.watched
    # Exchange : 1 000 tx en moins de 4 h, même avec de bons clients
    db2_bank = "EXCHtoileeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    pub.hist[db2_bank] = [(f"x{i}", 10_000 - i) for i in range(1000)]
    clients = [c for c in _createurs(pub, t, 12, src=db2_bank)][10:]
    _marches(monkeypatch, {"MINTs10": 90_000, "MINTs11": 70_000})
    asyncio.run(t.mesurer())
    assert db.toile_wallet(clients[0])["racine"] == db2_bank
    assert db.get(f"toile_service:{db2_bank}") == "1" and db2_bank not in p.watched


def test_dexscreener_muet_on_reessaie(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    pub = FakePublic()
    p, t = _toile(db, tg, pub, http=object())
    _createurs(pub, t, 1)
    _marches(monkeypatch, {})
    assert asyncio.run(t.mesurer()) == 0
    assert db.conn.execute("SELECT mc_24h FROM toile_tokens").fetchone()[0] is None     # pas marqué « mort »
    assert asyncio.run(t.mesurer(now=time.time() + 3 * 86400)) == 1                    # abandon après 3 jours


def test_lancement_qui_decolle_remonte_par_la_toile_sans_helius(setup):  # noqa: F811
    db, tg = setup
    pub = FakePublic()
    db.add_wallet(BANK, "BANK_WOTF", "reserve-suspect", "financeur lié au cluster reserve-suspect", 1, None)
    p, t = _toile(db, tg, pub)   # rpc=None : le moindre appel Helius échouerait
    creator = "C7decolleccccccccccccccccccccccccccccccccc"
    pub.neuf(creator, src=RELAIS)
    pub.txs["r_in"] = fund(BANK, RELAIS, 1.6)
    pub.hist[RELAIS] = [(f"f_{creator}", 1000), ("r_in", 990)]
    db.toile_put(creator, "neuf", RELAIS, 1.5, 1000, BANK)
    db.toile_put(RELAIS, "relais", BANK, 1.6, 990, BANK)
    p.lancements._traces.extend([time.time()] * L.TRACES_PER_HOUR)   # plafond Helius atteint : sans effet ici
    res = asyncio.run(p.lancements.remonter({"mint": "MINT7", "creator": creator, "symbol": "UP", "mc": 40_000,
                                             "txns": 90, "ts": time.time()}))
    assert res and res["genre"] == "rug" and [h["src"] for h in res["chaine"]] == [RELAIS, BANK]


def test_menage(setup):  # noqa: F811
    db, tg = setup
    now = int(time.time())
    db.toile_put("VIEUXcache", "reutilise")
    db.conn.execute("UPDATE toile_wallets SET vu=? WHERE address='VIEUXcache'", (now - 10 * 86400,))
    db.toile_put("OKneuf", "neuf", BANK, 1.0, now - 70 * 86400, BANK)
    db.toile_put("MORTneuf", "neuf", BANK, 1.0, now - 70 * 86400, BANK)
    db.conn.execute("UPDATE toile_wallets SET vu=? WHERE address IN ('OKneuf', 'MORTneuf')", (now - 70 * 86400,))
    db.toile_add_token("MINTok", "OKneuf", "OK", now - 70 * 86400)
    db.toile_add_token("MINTmort", "MORTneuf", "RIP", now - 70 * 86400)
    db.toile_set_mc({"MINTok": 200_000, "MINTmort": 3_000})
    db.toile_purge(now - T.CACHE_JOURS * 86400, now - T.GARDE_JOURS * 86400, T.SUCCES_MC)
    restes = {r[0] for r in db.conn.execute("SELECT address FROM toile_wallets")}
    assert restes == {"OKneuf"}   # le succès reste, le reste part
