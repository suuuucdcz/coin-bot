"""Biais corrigés : chacun de ces tests correspond à une erreur de logique qui faussait les alertes."""
import asyncio
import json
import time

import pytest
from conftest import MINT, NEW_WALLET, WATCHED

from radar import alerts as A
from radar import config as cfgmod
from radar.agenda import Agenda
from radar.analysis.classify import Event
from radar.analysis.enrich import TokenInfo, _dev_flags
from radar.db import DB
from radar.pipeline import Alert, Pipeline, is_dev_role

AUTRE_MINT = "CGUfGKcQy7cQbfmjQXSjW6uaQdG7ezGjaGndi98dWDgC"
BANK = "H9L2M3NNQEVxz5vShmYG2esJ1pwkPBrJdMNLBnnzhQSF"


@pytest.fixture
def pipe(tmp_path):
    db = DB(tmp_path / "r.db")
    p = Pipeline(cfgmod.load(), db, rpc=None, http=None, tg=None)
    yield p, db
    db.close()


def test_role_de_dev():
    assert is_dev_role("dev") and is_dev_role("dev probable : créateur de $X")
    assert is_dev_role("wallet principal du dev (bank probable)")
    # avant : « dev » n'importe où dans le rôle -> un wallet financé ou un financeur devenait « LE DEV »
    assert not is_dev_role("financé par DEV_WSOS") and not is_dev_role("a financé le dev (0.5 SOL)")


def test_tokens_morts_ne_sont_pas_des_rugs():
    succes = TokenInfo(MINT, dev_coins=[{"mint": str(i), "ath": 2_000_000 if i == 0 else 5_000, "rug": True}
                                        for i in range(5)])
    _dev_flags(succes)
    assert succes.flags == []                          # un dev qui a déjà fait 2 M$ n'est pas « rug »
    serie = TokenInfo(MINT, dev_coins=[{"mint": str(i), "ath": 3_000, "rug": True} for i in range(4)])
    _dev_flags(serie)
    assert serie.flags and "lanceur en série" in serie.flags[0]
    assert "PRUDENCE" in A.verdict(serie.flags) and "Rien de suspect" in A.verdict([])


def test_copie_du_ticker_pas_presentee_comme_le_coin_annonce(pipe):
    p, db = pipe
    db.insert_announcement(ticker="ASH", handle="AshbornCoin", tweet_url="https://x.com/AshbornCoin/status/1",
                           ca=AUTRE_MINT, status="annoncé")
    ligne = p._announcement_line(TokenInfo(MINT, symbol="ASH"))[0]
    assert "possible copie" in ligne and "Annoncé sur X" not in ligne
    assert "Annoncé sur X" in p._announcement_line(TokenInfo(AUTRE_MINT, symbol="ASH"))[0]


def test_dev_vers_dev_n_est_pas_un_retour_de_profits(pipe):
    p, db = pipe
    db.add_wallet(WATCHED, "DEV_A", "g", "dev", 1, BANK)
    db.add_wallet(NEW_WALLET, "DEV_B", "g", "dev", 1, BANK)
    db.add_wallet(BANK, "BANK", "g", "bank principal", 0, None)
    p.reload_watchlist()
    ev = Event("transfer", WATCHED, "sig1", int(time.time()), sol=50, other=NEW_WALLET)
    assert "TRANSFERT INTERNE" in asyncio.run(p._on_transfer(ev)).text
    ev = Event("transfer", WATCHED, "sig2", int(time.time()), sol=50, other=BANK)
    assert "PROFITS RAPATRIÉS" in asyncio.run(p._on_transfer(ev)).text


def test_wallet_en_veille_refinance_est_reveille(pipe):
    p, db = pipe
    db.add_wallet(BANK, "BANK", "g", "bank principal", 0, None)
    db.add_wallet(NEW_WALLET, "OLD_DEV", "g", "dev", 1, BANK)
    db.deactivate([NEW_WALLET])
    p.reload_watchlist()
    alert = asyncio.run(p._on_transfer(Event("transfer", BANK, "sig3", int(time.time()), sol=2, other=NEW_WALLET)))
    assert "EN VEILLE REFINANCÉ" in alert.text and db.wallet(NEW_WALLET)["active"] == 1


def test_detection_tardive_signalee(pipe):
    p, _db = pipe
    assert "de retard" in p._head(Event("buy", WATCHED, "s", int(time.time()) - 900))
    assert "de retard" not in p._head(Event("buy", WATCHED, "s", int(time.time()) - 5))


class _TG:
    def __init__(self):
        self.msgs = []

    def enqueue(self, text, *a, **k):
        self.msgs.append(text)
        return True

    async def edit_now(self, *a, **k):
        return True


class _P:
    def __init__(self, db):
        self.db, self.cfg, self.http, self.rpc = db, cfgmod.load(), None, None

    def _spawn(self, coro, urgent=False):
        coro.close()

    def label(self, a):
        return None


@pytest.fixture
def agenda(tmp_path):
    db = DB(tmp_path / "r.db")
    ann_id = db.insert_announcement(ticker="ASH", handle="AshbornCoin", tweet_url="u", tweet_text="we launch $ASH",
                                    sources="[]", status="annoncé", flags="[]")
    yield Agenda(_P(db), _TG()), db, ann_id
    db.close()


def test_cluster_dans_un_autre_ticker_ne_relie_pas_l_annonce(agenda):
    a, db, ann_id = agenda
    info = TokenInfo(MINT, symbol="PEPE2", created_ts=int(time.time()))
    asyncio.run(a.on_cluster_entry("$ASH", MINT, [WATCHED, NEW_WALLET], info, None))
    assert db.announcement(ann_id)["ca"] is None and "Ticker différent" in a.tg.msgs[0]


def test_coin_confirme_jamais_declare_faux(agenda, monkeypatch):
    a, db, ann_id = agenda
    db.update_announcement(ann_id, ca=MINT, details=json.dumps({"ca_proof": "fort"}))

    async def analyse_interdite(*args, **kw):
        raise AssertionError("le coin confirmé ne doit pas être analysé comme faux coin")

    monkeypatch.setattr("radar.fakes.analyze", analyse_interdite)
    assert asyncio.run(a.check_fake(ann_id, MINT)) is True
    assert db.announcement(ann_id)["ca"] == MINT


def test_watchlist_pleine_un_dev_remplace_un_satellite(pipe, monkeypatch):
    p, db = pipe
    db.add_wallet(WATCHED, "SAT_1", "$SKY", "satellite de J5AD : acheteur précoce", 2, None)
    p.reload_watchlist()
    monkeypatch.setattr(p, "cfg", cfgmod.Config(**{**p.cfg.__dict__, "watch_max": 1}))
    assert asyncio.run(p.watch(NEW_WALLET, "NEW_X", "g", "financé par BANK", 1, BANK))
    assert NEW_WALLET in p.watched and WATCHED not in p.watched and db.wallet(WATCHED)["active"] == 0
    # un simple satellite, lui, ne prend la place de personne
    assert not asyncio.run(p.watch(AUTRE_MINT, "SAT_2", "$SKY", "acheteur du faux coin", 2, None))


def test_heure_sans_fuseau_teste_plusieurs_fuseaux():
    from datetime import datetime, timezone

    from radar.analysis.xparse import parse_tweet
    i = parse_tweet("$MOON launching at 6pm on pump.fun", datetime(2026, 9, 26, 9, 0, tzinfo=timezone.utc))
    heures = {datetime.fromtimestamp(t, timezone.utc).hour for t in i.launch_alts}
    assert {18, 22, 1} <= heures          # 18 h UTC, 18 h New York (22 h UTC), 18 h Los Angeles (1 h UTC)


def test_meme_ticker_autre_contrat_pas_fusionne(agenda, monkeypatch):
    from radar.analysis.xparse import parse_tweet
    a, db, ann_id = agenda
    db.update_announcement(ann_id, ca=AUTRE_MINT)
    t = {"handle": "Shiller", "url": "https://x.com/Shiller/status/9", "text": f"$ASH CA {MINT} launching now"}
    asyncio.run(a.upsert(t, parse_tweet(t["text"])))
    row = db.announcement(ann_id)
    assert row["ca"] == AUTRE_MINT and "Shiller" not in (row["sources"] or "")


def test_schema_reserve_est_un_groupe_a_eviter():
    assert "reserve-suspect" in cfgmod.load().rug_groups



def test_niveaux_de_confiance(pipe):
    from radar.pipeline import wallet_trust
    p, db = pipe
    db.add_wallet(BANK, "BANK", "reserve-cluster", "bank principal", 0, None)
    db.add_wallet(WATCHED, "DEV_DJT", "découverte", "dev (découverte : $DJT, MC 19 M$ vérifiée DexScreener)", 0, None)
    db.add_wallet(NEW_WALLET, "NEW_1", "découverte", "financé par DEV_DJT", 1, WATCHED)
    db.add_wallet(AUTRE_MINT, "SAT_1", "$DOG", "acheteur du faux coin", 2, None)
    assert p.trust(WATCHED) == "prouvé" and p.trust(NEW_WALLET) == "lié" and p.trust(AUTRE_MINT) == "faible"
    assert wallet_trust(db.wallet(BANK)) == "référence"
    assert wallet_trust({"role": "dev probable : créateur du faux coin $DOG", "depth": 1, "grp": "$DOG"}) == "faible"


def test_ferme_de_bots_detectee(pipe):
    p, db = pipe
    for i in range(3):
        assert not p._farm_check("$DOG", f"mint{i}")
    assert p._farm_check("$DOG", "mint3")                     # 4e token différent en 6 h
    db.add_wallet(WATCHED, "SAT_1", "$DOG", "acheteur du faux coin", 2, None)
    assert "ferme de bots" in p.rug_flags(WATCHED)[0] and "À ÉVITER" in A.verdict(p.rug_flags(WATCHED))


def test_meme_ticker_obligatoire_pour_relier_une_creation():
    ann = {"ticker": "DOG"}
    assert Agenda._same_ticker(ann, "dog") and not Agenda._same_ticker(ann, "TICKER")
    assert not Agenda._same_ticker(ann, None)


def test_wallet_reclasse_jamais_de_confiance_ni_ses_enfants(pipe):
    # Vu en vrai : DEV_USDFA reclassé ⛔ (niveau 0) gardait « référence » ; NEW_77gm qu'il finance était « lié »
    p, db = pipe
    db.add_wallet(BANK, "DEV_USDFA", "reserve-suspect", "dev reclassé : même opérateur", 0, None)
    db.add_wallet(WATCHED, "NEW_77gm", "découverte", "financé par DEV_USDFA", 1, BANK)
    db.add_wallet(NEW_WALLET, "NEW_2GKA", "découverte", "financé par NEW_77gm", 2, WATCHED)
    assert p.trust(BANK) == "faible" and p.trust(WATCHED) == "faible"
    # le drapeau ⛔ remonte les financeurs : le petit-fils est marqué « via son financeur »
    assert "via son financeur" in p.rug_flags(NEW_WALLET)[0]


def test_sniper_retire_de_la_surveillance(pipe):
    p, db = pipe
    db.add_wallet(WATCHED, "SAT_4HgM", "$STARTUP", "satellite de newv : acheteur précoce", 2, None)
    p.reload_watchlist()
    resultats = [asyncio.run(p._sniper_check(Event("buy", WATCHED, f"s{i}", int(time.time()), f"mint{i}")))
                 for i in range(5)]
    assert resultats == [False] * 4 + [True] and WATCHED not in p.watched and db.wallet(WATCHED)["active"] == 0


def test_achat_d_un_satellite_sans_alerte(pipe):
    p, db = pipe
    db.add_wallet(WATCHED, "SAT_5Sug", "$STARTUP", "satellite de newv : acheteur précoce", 2, None)
    assert asyncio.run(p._on_buy(Event("buy", WATCHED, "s", int(time.time()), MINT, sol=1))) is None


def test_service_ne_finance_pas_de_devs(pipe):
    p, db = pipe
    db.add_wallet(BANK, "SAT_9obN", "$STARTUP", "satellite de 3bJH : financeur du dev", 2, None)
    db.set_label(BANK, "hot wallet / service (1000 tx en 3 h 32)")
    p.reload_watchlist()
    ev = Event("transfer", BANK, "sig9", int(time.time()), sol=0.5, other=NEW_WALLET)
    assert asyncio.run(p._on_transfer(ev)) is None and db.wallet(NEW_WALLET) is None


def test_groupe_decouverte_jamais_classe_ferme(pipe):
    # Vu en vrai : 4 devs indépendants du groupe « découverte » achètent 4 tokens -> tout le groupe classé ferme (⛔)
    p, db = pipe

    async def sans_alerte(*a, **k):
        return None

    p._cluster_alert = sans_alerte
    for i, adr in enumerate((WATCHED, NEW_WALLET, BANK, AUTRE_MINT)):
        db.add_wallet(adr, f"DEV_{i}", "découverte", "dev (découverte : $X)", 0, None)
        asyncio.run(p._cluster_track(Event("buy", adr, f"s{i}", int(time.time()), f"mint{i}")))
    assert not p.is_farm("découverte")


def test_un_seul_projet_rug_suffit():
    from radar.analysis import network
    rep = network.Report(WATCHED, projects=[network.classify_project(
        {"mint": "trudy", "symbol": "TRUDY", "creator": WATCHED, "created": int(time.time()) - 7000, "ath": 3_700_000,
         "ath_ts": int(time.time()) - 6000, "mc": 1_300, "complete": True})])
    assert "⛔" in rep.verdict()[0]


def test_deplacement_de_supply_detecte():
    from conftest import make_tx, TOKEN
    from radar.analysis.classify import analyze
    tx = make_tx([(WATCHED, True, 5, 4.999995)], [TOKEN],
                 tokens=[(WATCHED, MINT, 50_000_000, 10_000_000), (NEW_WALLET, MINT, 0, 40_000_000)])
    ev = analyze(tx, {WATCHED})
    assert [e.kind for e in ev] == ["supply_out"] and ev[0].other == NEW_WALLET


def test_le_dev_deplace_sa_supply(pipe, monkeypatch):
    p, db = pipe
    db.add_wallet(WATCHED, "DEV_X", "g", "dev", 0, None)
    p.reload_watchlist()

    async def info(mint, creator_hint=None, dev=True):
        return TokenInfo(mint, symbol="PAID", creator=WATCHED, supply_raw=1_000_000_000, mc_usd=50_000)

    class RPC:
        async def signatures(self, a, limit=6):
            return [{}]   # wallet neuf

    monkeypatch.setattr(p, "_info", info)
    p.rpc = RPC()
    ev = Event("supply_out", WATCHED, "s", int(time.time()), MINT, tokens_raw=50_000_000, pre_tokens_raw=200_000_000,
               other=NEW_WALLET)
    alerte = asyncio.run(p._on_supply_out(ev))
    assert "LE DEV DÉPLACE SA SUPPLY" in alerte.text and "5.0 %" in alerte.text and db.wallet(NEW_WALLET)
    petit = Event("supply_out", WATCHED, "s2", int(time.time()), MINT, tokens_raw=1_000, pre_tokens_raw=2_000, other=BANK)
    assert asyncio.run(p._on_supply_out(petit)) is None and "trop petit" in p.decisions_line()


def test_contrat_lance_plus_jamais_en_attente(pipe):
    p, db = pipe
    db.add_wallet(MINT, "ASH_MINT", "ASH", "contrat du token", 0, None)
    db.mark_alert_sent(f"lp:{MINT}", "lp_add")
    p.reload_watchlist()
    asyncio.run(p.detect_mints())
    assert MINT not in p.mints and MINT not in p.watched


def test_alerte_bloquee_faute_de_donnees_repart_quand_elles_arrivent(pipe, monkeypatch):
    from radar import pipeline as pl
    p, db = pipe
    monkeypatch.setattr(pl, "TOP_RETRY_S", (0, 0))
    envoye = []

    class TG:
        def enqueue_top(self, text, markup=None, key=None, **_kw):
            envoye.append(text)
            return True

    p.tg = TG()
    complet = TokenInfo(MINT, symbol="ASH", creator=WATCHED, supply_raw=10**15, mc_usd=900_000, dev_coins=[],
                        top10_pct=12.0, dev_pct=2.0, created_ts=int(time.time()) - 120)

    async def info(mint, creator_hint=None, dev=True):
        return complet

    monkeypatch.setattr(p, "_info", info)
    incomplet = TokenInfo(MINT, symbol="ASH", supply_raw=10**15, created_ts=int(time.time()) - 60)
    alerte = Alert("lp:x", "lp_add", "…", top_title="TRADING OUVERT", top_why="contrat annoncé", info=incomplet, flags=[])

    async def go():
        p._emit_top(alerte)
        await asyncio.sleep(0.05)

    asyncio.run(go())
    assert envoye and "TRADING OUVERT" in envoye[0] and "données complètes" in envoye[0]
