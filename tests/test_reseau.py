"""🕸 Réseau d'un dev : sort des projets, verdict du réseau, blocage des réseaux à rugs, toile HTML."""
import asyncio
import time
from html.parser import HTMLParser

import pytest
from conftest import MINT, NEW_WALLET, WATCHED

from radar import alerts as A
from radar import config as cfgmod
from radar.analysis import network
from radar.analysis.enrich import TokenInfo
from radar.db import DB
from radar.pipeline import Pipeline

NOW = time.time()
BANK = "H9L2M3NNQEVxz5vShmYG2esJ1pwkPBrJdMNLBnnzhQSF"


def coin(mint, ath, ath_after_s, mc, age_h=5, complete=False, creator=WATCHED):
    created = int(NOW - age_h * 3600)
    return {"mint": mint, "symbol": mint[:4].upper(), "creator": creator, "created": created, "ath": ath,
            "ath_ts": created + ath_after_s, "mc": mc, "complete": complete, "last_trade": created + 300}


def test_sort_des_projets():
    assert network.classify_project(coin("a", 9_000, 3, 3_000)).verdict == "vidé"        # achat groupé revendu
    assert network.classify_project(coin("b", 2_500_000, 3600, 400_000, complete=True)).verdict == "succès"
    assert network.classify_project(coin("c", 40_000, 1800, 2_000)).verdict == "mort"
    assert network.classify_project(coin("d", 40_000, 60, 30_000, age_h=0.2)).verdict == "récent"


def test_verdict_du_reseau():
    rugs = network.Report(WATCHED, projects=[network.classify_project(coin(f"m{i}", 9_000, 5, 2_000)) for i in range(4)])
    texte, drapeau = rugs.verdict()
    assert "rugs en série" in texte and drapeau and "4/4" in drapeau
    bon = network.Report(WATCHED, projects=[network.classify_project(coin("s", 3_000_000, 7200, 900_000, complete=True))])
    assert bon.verdict()[1] is None and "succès" in bon.verdict()[0]
    revend = network.Report(WATCHED, projects=[network.Project("x", "X", WATCHED, int(NOW) - 9000, 50_000, None, 20_000,
                                                                None, False, "mort", dev_sell_s=40)] * 2)
    assert "revend" in revend.verdict()[0]


@pytest.fixture
def pipe(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    p = Pipeline(cfgmod.load(), db, rpc=None, http=None, tg=None)
    network._quick_cache.clear()

    async def funding(self, address):
        return {"source": BANK, "amount": 1.5, "signature": "s", "ts": int(NOW) - 60}, 2, ""

    async def pas_exchange(self, source, sig):
        return False, ""

    monkeypatch.setattr(network.Tracer, "first_funding", funding)
    monkeypatch.setattr(network.Tracer, "hot_check", pas_exchange)
    db.add_link(BANK, NEW_WALLET, "frère", 1.5, "s2", int(NOW) - 60)
    yield p, db, monkeypatch
    db.close()


def test_reseau_rapide_remonte_le_financeur_et_les_freres(pipe):
    p, db, monkeypatch = pipe
    projets = {NEW_WALLET: [coin(f"f{i}", 9_000, 2, 2_500, creator=NEW_WALLET) for i in range(3)],
               BANK: [coin("b1", 12_000, 4, 3_000, creator=BANK)]}

    async def coins_by_creator(http, w):
        return projets.get(w, [])

    monkeypatch.setattr(network.pumpfun, "coins_by_creator", coins_by_creator)
    rep = asyncio.run(network.quick(p, WATCHED))
    assert set(rep.wallets) == {WATCHED, BANK, NEW_WALLET} and len(rep.projects) == 4
    info = TokenInfo(MINT, symbol="NEW", creator=WATCHED)
    network.annotate(info, rep)
    # nouveau wallet « propre », mais son réseau vide tous ses tokens : bloqué pour « à ne pas rater »
    assert "réseau à rugs" in info.flags[0] and not A.is_safe(info.flags)
    assert "Réseau du dev" in info.network


class _Checker(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)


def test_toile_html(pipe):
    p, db, _m = pipe
    rep = network.Report(WATCHED, wallets={WATCHED: {"label": "DEV <A>", "role": "départ"},
                                           BANK: {"label": "BANK", "role": "financeur"},
                                           "MEXC1": {"label": "MEXC", "role": "exchange"}},
                         edges=[{"src": BANK, "dst": WATCHED, "kind": "financement", "sol": 1.5, "ts": 0},
                                {"src": "MEXC1", "dst": BANK, "kind": "financement", "sol": 50, "ts": 0}],
                         projects=[network.classify_project(coin("t1", 9_000, 3, 2_000))], deep=True)
    html = network.to_html(rep, "DEV <A>")
    c = _Checker()
    c.feed(html)
    assert c.tags.count("circle") == 3 and c.tags.count("rect") == 1 and "DEV &lt;A&gt;" in html
    texte = network.telegram_text(rep, "DEV <A>")
    assert "RÉSEAU DE DEV &lt;A&gt;" in texte and "vidés" in texte
    # sauvegarde / relecture (bouton « Suivre tout le réseau »)
    assert network.Report.from_json(rep.to_json()).projects[0].verdict == "vidé"


def test_gros_ath_puis_moins_99_pourcent_est_un_rug_pas_un_succes():
    # Vu en vrai : $NTDA 46,7 M$ d'ATH puis 2,4 k$ (cluster Reserve)
    p = network.classify_project(coin("ntda", 46_690_173, 80 * 3600, 2_412, age_h=150, complete=True))
    assert p.verdict == "rug"
    dev_djt = network.Report(WATCHED, projects=[
        network.classify_project(coin("djt", 20_592_589, 19 * 3600, 20_548_583, age_h=19.3, complete=True)),
        network.classify_project(coin("vsof1", 38_285_762, 70 * 3600, 2_189, age_h=88, complete=True)),
        network.classify_project(coin("vsof2", 13_327_673, 4 * 3600, 2_111, age_h=90, complete=True)),
        network.classify_project(coin("ntda", 46_690_173, 80 * 3600, 2_412, age_h=150, complete=True))])
    texte, drapeau = dev_djt.verdict()
    assert "⛔" in texte and "3/4" in drapeau


def test_relance_du_meme_nom():
    projets = [network.classify_project({**coin(f"v{i}", 60_000, 3600, 30_000), "symbol": "VSOF"}) for i in range(2)]
    projets.append(network.classify_project(coin("ok", 2_000_000, 7200, 900_000, complete=True)))
    texte, drapeau = network.Report(WATCHED, projects=projets).verdict()
    assert "même nom" in texte and "$VSOF ×2" in drapeau


def test_chaine_de_relais_au_meme_montant():
    # Vu en vrai : dev ⟵ 100 SOL ⟵ 100 SOL ⟵ bank 1 006 SOL (XBC, USDF, VSOF, AROS…)
    rep = network.Report(WATCHED, funding_chain=[{"src": "a", "sol": 99.99999, "hot": False},
                                                 {"src": "b", "sol": 100.0, "hot": False},
                                                 {"src": "c", "sol": 1006.06, "hot": False}])
    texte, drapeau = rep.verdict()
    assert "faux fonds souverains" in texte and "réseau à rugs" in drapeau
    # 0,1 ⟵ 0,1 ⟵ Binance : financement brouillé (pas massif) -> 🟠, bloque « à ne pas rater »
    rep = network.Report(WATCHED, funding_chain=[{"src": "a", "sol": 0.1, "hot": False},
                                                 {"src": "b", "sol": 0.100005, "hot": False},
                                                 {"src": "cex", "sol": 2000, "hot": True}])
    texte, drapeau = rep.verdict()
    assert "brouillé" in texte and not A.is_safe([drapeau])
    # financement normal : rien
    assert network.Report(WATCHED, funding_chain=[{"src": "a", "sol": 1.5, "hot": False},
                                                  {"src": "cex", "sol": 40, "hot": True}]).verdict()[1] is None


def test_meme_operateur_derriere_plusieurs_devs(tmp_path):
    from radar import discovery
    db = DB(tmp_path / "r.db")
    for dev in (WATCHED, NEW_WALLET):
        db.add_wallet(dev, "DEV", "découverte", "dev (découverte : $X)", 0, None)
    f1 = discovery.Found(WATCHED, "USDFA", "m1", 1e7, 1, 1, upstream=["EdcS"])
    f2 = discovery.Found(NEW_WALLET, "WOAR", "m2", 1e7, 1, 1, upstream=["EdcS"])
    discovery._same_operator(db, f1)
    assert f1.rug_group is None
    discovery._same_operator(db, f2)
    assert f2.rug_group == "reserve-suspect" and db.wallet(WATCHED)["grp"] == "reserve-suspect"
    db.close()
