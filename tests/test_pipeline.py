"""Circuit d'alerte complet, hors ligne : alerte rapide, puis message complété ; dédoublonnage ; plafond."""
import asyncio
import time

import pytest
from conftest import MINT, NEW_WALLET, WATCHED

from radar import config as cfgmod
from radar import pipeline as pl
from radar.analysis.classify import Event
from radar.analysis.enrich import TokenInfo
from radar.db import DB


class FakeTelegram:
    def __init__(self, db):
        self.db, self.sent, self.replaced = db, [], []

    def enqueue(self, text, markup=None, key=None, kind="", topic=None, **_kw):
        if key and self.db.alert_already_sent(key):
            return False
        if key:
            self.db.mark_alert_sent(key, kind)
        self.sent.append((key, topic, text))
        return True

    async def replace(self, key, text, markup=None, topic=None):
        self.replaced.append((key, text))


@pytest.fixture
def setup(tmp_path, monkeypatch):
    db = DB(tmp_path / "radar.db")
    db.add_wallet(WATCHED, "DEV_TEST", "reserve-cluster", "dev", 0, None)
    tg = FakeTelegram(db)

    async def fake_info(rpc, http, mint, creator_hint=None, dev=True):
        return TokenInfo(mint, name="Ashborn", symbol="ASH", creator=creator_hint, created_ts=int(time.time()) - 60,
                         supply_raw=1_000_000_000_000_000, dev_coins=[])

    async def no_trace(self, creator, info):
        return None

    monkeypatch.setattr(pl, "token_info", fake_info)
    monkeypatch.setattr(pl.Pipeline, "_auto_trace", no_trace)
    p = pl.Pipeline(cfgmod.load(), db, rpc=None, http=None, tg=tg)
    yield p, tg, db
    db.close()


def test_creation_alerte_rapide_puis_completee(setup):
    p, tg, _db = setup
    ev = Event("create", WATCHED, "sig", int(time.time()), MINT, sol=1.5, tokens_raw=35_000_000_000_000,
               extra={"symbol": "ASH", "name": "Ashborn"})

    async def go():
        alert = await p.process(ev)
        p.emit(alert)
        await asyncio.sleep(0)   # laisse partir la tâche de remplacement
        return alert

    alert = asyncio.run(go())
    # 1) alerte rapide immédiate, avec le CA et le drapeau du cluster de rugs -> compartiment Arnaques
    key, topic, text = tg.sent[0]
    assert key == f"create:{MINT}" and MINT in text and "analyse en cours" in text.lower() and topic == "scams"
    # 2) message complété (même clé), avec la part de supply
    assert alert.replace and tg.replaced[0][0] == key and "3.50 % supply" in tg.replaced[0][1]
    # 3) la même création revue (PumpPortal puis Helius) ne repart pas
    assert asyncio.run(p.process(ev)) is None and len(tg.sent) == 1


def test_plafond_de_la_watchlist(setup, monkeypatch):
    p, tg, db = setup
    monkeypatch.setattr(p, "cfg", cfgmod.Config(**{**p.cfg.__dict__, "watch_max": len(p.watched)}))
    ajoute = asyncio.run(p.watch(NEW_WALLET, "NEW", "g", "financé", 1, WATCHED))
    assert not ajoute and db.wallet(NEW_WALLET) is None
    assert tg.sent and tg.sent[0][1] == "system"   # prévenu une fois sur Telegram
