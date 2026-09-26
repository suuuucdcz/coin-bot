"""Sections du groupe : explications épinglées, fiche dev, bandeau arnaques, messages épinglés par section."""
import asyncio

import pytest
from conftest import MINT, NEW_WALLET, WATCHED
from test_bot import html_ok

from radar import config as cfgmod
from radar import devs
from radar.analysis.enrich import TokenInfo
from radar.analysis.tracer import Hop, TraceResult
from radar.db import DB
from radar.pipeline import Alert, Pipeline
from radar.telegram import SECTION_INFO, Telegram


def test_explications_des_sections_en_html_valide():
    for cle, texte in SECTION_INFO.items():
        assert html_ok(texte), cle
        assert len(texte) < 1000, cle


def test_message_epingle_suit_la_section(tmp_path):
    db = DB(tmp_path / "r.db")
    tg = Telegram("000:x", "-100123", db)
    avant = tg.place("agenda")
    tg.forum, tg.threads = True, {"agenda": 12}
    assert avant == "-100123:0" and tg.place("agenda") == "-100123:12"   # recréé dans la section
    db.close()


@pytest.fixture
def pipe(tmp_path):
    db = DB(tmp_path / "r.db")
    p = Pipeline(cfgmod.load(), db, rpc=None, http=None, tg=None)
    yield p, db
    db.close()


def test_fiche_dev_lisible(pipe):
    p, db = pipe
    hop = Hop(WATCHED, 3, True, NEW_WALLET, 0.48, "sig", 0)
    rapport = devs.DevReport(WATCHED, "créateur du token", satellites=[
        devs.Satellite(NEW_WALLET, "financeur du dev"), devs.Satellite(MINT, "détient 12.0 % de la supply")],
        trace=TraceResult(WATCHED, [hop], "source = hot wallet d'exchange", {}))
    info = TokenInfo(MINT, symbol="ASH", dev_coins=[], network="🕸 Réseau du dev : 2 wallet(s) · 1 projet(s)")
    texte, clavier = devs.card(p, rapport, info, "$ASH")
    assert html_ok(texte) and "💰 Argent : dev ⟵ 0.48 SOL" in texte and "🛰 <b>2 satellite(s)</b>" in texte
    assert "Réseau du dev" in texte and any(b.get("callback_data", "").startswith("n:")
                                             for rang in clavier["inline_keyboard"] for b in rang)


def test_bandeau_dans_la_section_arnaques(pipe):
    p, _db = pipe
    alerte = Alert("k", "funding", "🟡 financement … opérateur de rugs en série …")
    assert alerte.topic == "scams"
    p.emit(alerte)
    assert alerte.text.startswith("🏴‍☠️ <b>À SIGNALER — NE PAS ACHETER</b>")
