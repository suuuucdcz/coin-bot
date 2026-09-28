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


def test_un_seul_projet_rate_n_est_pas_une_serie():
    # Vu en vrai : le dev de $goon (17,7 M$, encore 12,4 M$ deux jours après) classé « réseau à rugs » pour 1 projet
    # raté sur 2. Un token mort après 150 k$, ça arrive à un dev honnête ; deux, ou un faux succès à 12 M$, non.
    rate = network.classify_project(coin("r", 150_000, 3600, 900, age_h=48, complete=True))
    goon = network.classify_project(coin("g", 17_700_000, 40 * 3600, 12_400_000, age_h=48, complete=True))
    assert rate.verdict == "rug" and network.Report(WATCHED, projects=[rate, goon]).verdict()[1] is None
    deux = network.Report(WATCHED, projects=[rate, network.classify_project(coin("r2", 120_000, 3600, 800, age_h=48,
                                                                                  complete=True))])
    assert "réseau à rugs" in (deux.verdict()[1] or "")
    faux = network.classify_project(coin("vsof", 16_100_000, 30 * 3600, 2_000, age_h=72, complete=True))
    assert "réseau à rugs" in (network.Report(WATCHED, projects=[faux, goon]).verdict()[1] or "")


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


def test_anciennes_decouvertes_non_verifiees_revalidees(tmp_path, monkeypatch):
    # Vu en vrai : 19 devs du réseau Reserve gardaient « prouvé » (ATH absurdes de pump.fun : AROS « 482 M$ »)
    from radar import discovery
    db = DB(tmp_path / "r.db")
    p = Pipeline(cfgmod.load(), db, rpc=None, http=object(), tg=None)
    aros, fami, yap = "AROSdevaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "FAMIdevffffffffffffffffffffffffffffffffff", \
        "YAPdevyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
    for a, sym, ath in ((aros, "AROS", "482.3"), (fami, "familiars", "26.9"), (yap, "YAP", "25.7")):
        db.add_wallet(a, f"DEV_{sym}", "découverte", f"dev (découverte : ${sym} ATH {ath} M$)", 0, None)
    assert p.trust(aros) == "faible"                  # plus « prouvé » sans marché vérifié
    coins = {aros: ("mAROS", "AROS", 482e6), fami: ("mFAM", "familiars", 45e6), yap: ("mYAP", "YAP", 27e6)}

    async def coins_by_creator(http, a):
        m, s, ath = coins[a]
        return [{"mint": m, "symbol": s, "ath": ath, "created": int(NOW) - 4 * 86400}]

    async def markets(http, mints):
        vrais = {"mFAM": {"mc": 40_900_000, "liquidity": 2_100_000, "txns24h": 8_000},
                 "mYAP": {"mc": 2_300, "liquidity": 1_800, "txns24h": 12}}
        return {m: vrais[m] for m in mints if m in vrais}

    async def evaluate(pipeline, coin, min_ath):
        f = discovery.Found(coin["creator"], coin["symbol"], coin["mint"], coin["ath"], 3, 1)
        if coin["symbol"] == "AROS":
            f.rug_group, f.reason = "reseau-rugs", "⛔ réseau à rugs : 5/6 projets du dev et de ses wallets rug (−99 %)"
        return f

    monkeypatch.setattr(discovery.pumpfun, "coins_by_creator", coins_by_creator)
    monkeypatch.setattr(discovery.dexscreener, "markets", markets)
    monkeypatch.setattr(discovery, "evaluate", evaluate)
    assert len(asyncio.run(discovery.revalider(p))) == 3
    assert db.wallet(aros)["grp"] == "reseau-rugs" and db.wallet(aros)["role"].startswith("dev reclassé : réseau")
    assert p.trust(fami) == "prouvé" and "40.9 M$ vérifiée DexScreener" in db.wallet(fami)["role"]
    assert p.trust(yap) == "faible" and "non confirmée" in db.wallet(yap)["role"]
    assert asyncio.run(discovery.revalider(p)) == []  # rien à refaire
    db.close()


def test_ferme_de_wallets_sur_des_repartitions_reelles():
    # Relevés du 28/09 : top des détenteurs (hors pools) de coins classés arnaque, et de coins au marché organique
    from radar.analysis.enrich import FERME_MIN, TokenInfo as TI, _rug_flags, ferme
    reels = {
        "WSOS": [0.99] * 10,
        "VSOF": [1.14] + [0.20] * 9,
        "WEPE": [1.44, 1.40, 1.40, 1.39, 1.37, 1.36, 1.36, 1.35, 1.32, 1.32],
        "JEANPHIL": [2.58, 2.22, 1.80, 1.63, 1.41, 1.23, 1.03, 1.03, 1.03, 0.96],
        "YAP": [4.62, 4.41, 3.04, 2.10, 1.95, 1.63, 1.50, 1.35, 1.33, 1.21],
        "COLLECT": [2.19, 2.07, 2.02, 1.79, 1.73, 1.52, 1.24, 1.16, 1.15, 1.12],
    }
    fermes = {k: ferme({f"w{i}": p for i, p in enumerate(v)})[0] >= FERME_MIN for k, v in reels.items()}
    assert fermes == {"WSOS": True, "VSOF": True, "WEPE": True, "JEANPHIL": False, "YAP": False, "COLLECT": False}
    # le créateur ne compte pas dans la ferme
    assert ferme({"dev": 0.79, **{f"w{i}": 0.50 for i in range(9)}}, "dev") == (9, 4.5)
    info = TI("m", farm_n=10, farm_pct=9.9)
    _rug_flags(info)
    # Information affichée, pas un signal bloquant : dans l'échantillon du 28/09, fermes et coins organiques ont tous
    # perdu 40 à 70 % depuis leur plus haut (WEPE, goon / JEANPHIL, YAP) ; rien ne justifie de les exclure de ‼️
    assert any(f.startswith("🧱 ferme de wallets : 10") for f in info.flags) and A.is_safe(info.flags)


def test_chaine_de_relais_seule_ne_classe_pas_sans_ferme(tmp_path, monkeypatch):
    # Vu en vrai : $YAP (14,5 M$ de vrai volume, détenteurs variés) classé arnaque pour 3,36 SOL relayés deux fois
    from radar import discovery
    db = DB(tmp_path / "r.db")
    p = Pipeline(cfgmod.load(), db, rpc=None, http=object(), tg=None)

    async def coins_by_creator(http, a):
        return []

    async def quick(pipeline, creator):
        return network.Report(creator)

    async def rien(self, address):
        return None, 0, ""

    monkeypatch.setattr(discovery.pumpfun, "coins_by_creator", coins_by_creator)
    monkeypatch.setattr(network, "quick", quick)
    monkeypatch.setattr(network, "quick_verdict",
                        lambda rep: network.RELAIS_FLAG + " au même montant (3.36104 SOL → 3.36106 SOL)")
    monkeypatch.setattr(discovery.Tracer, "first_funding", rien)
    coin = {"creator": WATCHED, "symbol": "YAP", "mint": MINT, "ath": 5.7e6}
    for ferme_ou_pas, attendu in ((False, None), (True, discovery.SUSPECT_GROUP)):
        async def _ferme(pipeline, mint, creator, r=ferme_ou_pas):
            return r
        monkeypatch.setattr(discovery, "_ferme", _ferme)
        f = asyncio.run(discovery.evaluate(p, coin, 500_000))
        assert f.rug_group == attendu
        if not ferme_ou_pas:
            assert f.funder_note.startswith("🟠 financement brouillé") and not f.reason
    db.close()
