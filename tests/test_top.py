"""« 🎯 À ne pas rater » : seules les alertes vérifiées et propres, en privé, avec le son."""
import asyncio
import time

import pytest
from conftest import MINT, WATCHED
from test_bot import html_ok

from radar import alerts as A
from radar import config as cfgmod
from radar import pipeline as pl
from radar.analysis.classify import Event
from radar.analysis.enrich import TokenInfo
from radar.db import DB
from radar.telegram import Telegram


class FakeTG:
    def __init__(self, db):
        self.db, self.top, self.sent = db, [], []

    def enqueue(self, text, markup=None, key=None, kind="", topic=None, **_kw):
        self.sent.append(text)
        return True

    def enqueue_top(self, text, markup=None, key=None):
        self.top.append((text, markup, key))
        return True

    async def replace(self, *a, **k):
        return None


@pytest.fixture
def setup(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    tg = FakeTG(db)

    async def fake_info(rpc, http, mint, creator_hint=None, dev=True):
        return TokenInfo(mint, name="Ashborn", symbol="ASH", creator=creator_hint, created_ts=int(time.time()) - 60,
                         supply_raw=10**15, dev_coins=[{"mint": "x", "ath": 11_700_000, "rug": False}],
                         mc_usd=25_000, top10_pct=18.0, dev_pct=3.0)

    async def no_trace(self, creator, info):
        return None

    monkeypatch.setattr(pl, "token_info", fake_info)
    monkeypatch.setattr(pl.Pipeline, "_auto_trace", no_trace)
    yield db, tg
    db.close()


def creer(db, tg, groupe):
    db.add_wallet(WATCHED, "DEV_XBC_7m2S", groupe, "dev (découverte : $XBC, MC 11.7 M$ vérifiée DexScreener)", 0, None)
    p = pl.Pipeline(cfgmod.load(), db, rpc=None, http=None, tg=tg)
    ev = Event("create", WATCHED, "sig", int(time.time()), MINT, sol=1.0, tokens_raw=10**13,
               extra={"symbol": "ASH", "name": "Ashborn"})

    async def go():
        p.emit(await p.process(ev))

    asyncio.run(go())


def test_dev_propre_arrive_dans_a_ne_pas_rater(setup):
    db, tg = setup
    creer(db, tg, "découverte")
    assert len(tg.top) == 1
    texte, clavier, cle = tg.top[0]
    assert "UN DEV SUIVI CRÉE UN TOKEN" in texte and "DEV_XBC_7m2S" in texte and MINT in texte and html_ok(texte)
    boutons = [b for rang in clavier["inline_keyboard"] for b in rang]
    assert boutons[0] == {"text": "📋 Copier le CA", "copy_text": {"text": MINT}}
    assert cle == f"top:create:{MINT}"


def test_operateur_de_rugs_jamais_dans_a_ne_pas_rater(setup):
    db, tg = setup
    creer(db, tg, "reserve-cluster")
    assert tg.top == [] and any("À ÉVITER" in t for t in tg.sent)


def test_signal_grave_reste_dans_le_groupe():
    info = TokenInfo(MINT, symbol="ASH", flags=["mint authority active (le dev peut imprimer des tokens)"])
    assert not A.is_safe(info.flags) and A.is_safe(["2 points à regarder"])


def test_prive_avec_son_groupe_sans_son(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    tg = Telegram("000:x", "-100123", db, top_chat_id="42")
    appels = []

    async def fake_call(method, payload):
        appels.append(payload)
        return {"ok": True, "result": {"message_id": len(appels)}}

    monkeypatch.setattr(tg, "_call", fake_call)
    monkeypatch.setattr("radar.telegram.MIN_INTERVAL", 0)

    async def go():
        w = asyncio.create_task(tg.worker())
        tg.enqueue("alerte du groupe", key="k1", kind="create")
        tg.enqueue_top("🚨 à ne pas rater", key="top:k1")
        await asyncio.sleep(0.05)
        w.cancel()

    asyncio.run(go())
    groupe, prive = appels
    assert groupe["chat_id"] == "-100123" and groupe["disable_notification"] is True
    assert prive["chat_id"] == "42" and "disable_notification" not in prive
    db.close()


def test_donnees_incompletes_jamais_a_ne_pas_rater(setup, monkeypatch):
    db, tg = setup

    async def sans_donnees(rpc, http, mint, creator_hint=None, dev=True):
        return TokenInfo(mint, symbol="ASH", creator=creator_hint, created_ts=int(time.time()) - 60, supply_raw=10**15)

    monkeypatch.setattr(pl, "token_info", sans_donnees)
    creer(db, tg, "découverte")
    assert tg.top == []          # avant : aucune donnée = aucun drapeau = 🟢 « sûr »


def test_createur_faible_jamais_a_ne_pas_rater(setup):
    db, tg = setup
    db.add_wallet(WATCHED, "NEW_3YX6", "$STARTUP", "financé par SAT_9obN", 3, "9obNtb5GyUegcs3a1CbBkLuc5hEWynWfJC6gjz5uWQkE")
    p = pl.Pipeline(cfgmod.load(), db, rpc=None, http=None, tg=tg)
    ev = Event("create", WATCHED, "sig", int(time.time()), MINT, sol=1.0, extra={"symbol": "ASH"})

    async def go():
        p.emit(await p.process(ev))

    asyncio.run(go())
    assert tg.top == [] and tg.sent      # alerte dans le groupe, mais pas « à ne pas rater »


def test_section_du_groupe_des_que_le_bot_est_admin(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    tg = Telegram("000:x", "-100123", db, top_chat_id="42")
    tg.forum, tg.threads = True, {"top": 7, "onchain": 8}     # sujets créés (bot administrateur)
    appels = []

    async def fake_call(method, payload):
        appels.append(payload)
        return {"ok": True, "result": {"message_id": len(appels)}}

    monkeypatch.setattr(tg, "_call", fake_call)
    monkeypatch.setattr("radar.telegram.MIN_INTERVAL", 0)

    async def go():
        w = asyncio.create_task(tg.worker())
        tg.enqueue("alerte détaillée", key="k1", kind="create", topic="onchain")
        tg.enqueue_top("🚨 à ne pas rater", key="top:k1")
        await asyncio.sleep(0.05)
        w.cancel()

    asyncio.run(go())
    detail, top = appels
    assert detail["message_thread_id"] == 8 and detail["disable_notification"] is True     # sans son
    assert top["chat_id"] == "-100123" and top["message_thread_id"] == 7 and "disable_notification" not in top
    db.close()


def test_drapeau_de_l_annonce_bloque_a_ne_pas_rater(setup):
    # Coin annoncé par un compte X racheté : même un dev prouvé ne l'envoie pas dans ‼️
    db, tg = setup
    import json
    db.insert_announcement(ticker="ASH", handle="AshbornCoin", tweet_url="u", ca=MINT, status="annoncé",
                           flags=json.dumps(["@AshbornCoin : a changé de nom d'utilisateur 20 fois : compte racheté "
                                             "ou recyclé ?"]))
    creer(db, tg, "découverte")
    assert tg.top == []


def test_drapeau_leger_de_l_annonce_affiche_dans_a_ne_pas_rater(setup):
    db, tg = setup
    import json
    db.insert_announcement(ticker="ASH", handle="AshbornCoin", tweet_url="u", ca=MINT, status="annoncé",
                           flags=json.dumps(["@AshbornCoin : très peu d'abonnés (226)"]))
    creer(db, tg, "découverte")
    assert len(tg.top) == 1 and "très peu d'abonnés (226)" in tg.top[0][0]
