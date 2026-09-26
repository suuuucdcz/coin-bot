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
from radar.pipeline import Pipeline, is_dev_role

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
