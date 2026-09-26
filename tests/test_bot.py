"""Bot Telegram : commandes, boutons, sécurité, et HTML valide (Telegram refuse un HTML mal fermé)."""
import asyncio
import re
import time
from html.parser import HTMLParser

import pytest
from conftest import MINT, WATCHED

from radar import alerts as A
from radar import config as cfgmod
from radar.analysis.enrich import TokenInfo
from radar.bot import Bot
from radar.db import DB
from radar.telegram import Telegram

ALLOWED = {"b", "i", "u", "s", "code", "pre", "a", "blockquote", "tg-spoiler"}


class Checker(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack, self.errors = [], []

    def handle_starttag(self, tag, attrs):
        if tag not in ALLOWED:
            self.errors.append(f"balise interdite <{tag}>")
        self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack.pop() != tag:
            self.errors.append(f"</{tag}> mal fermé")


def html_ok(text: str) -> bool:
    c = Checker()
    c.feed(text)
    return not c.errors and not c.stack


class FakeRPC:
    auth_error = None
    calls = {0: 0, 1: 0}

    def recent_failures(self):
        return 0

    async def mint_info(self, a):
        return None

    async def balance(self, a):
        return 1.5


class FakePipeline:
    def __init__(self, db):
        self.db, self.rpc, self.http, self.mints = db, FakeRPC(), None, set()
        self.watched = {WATCHED}
        self.decisions_since = 0

    def decisions_line(self):
        return "12 événements · 1 alertes · écartés : 8 achat d'un satellite"

    def label(self, a):
        w = self.db.wallet(a)
        return w["label"] if w else None

    def muted(self, a):
        return False

    def rug_flags(self, *a):
        return []

    def _announcement_line(self, info):
        return []

    async def unwatch(self, addrs):
        self.watched -= set(addrs)


class FakeWatcher:
    down_since = None
    addresses = {WATCHED}


class FakeAgenda:
    class jev:
        enabled = False

    class llm:
        enabled, model, calls, last_ms, _ok_until = False, "gemma4:e4b", 0, 0, 0

    def render(self):
        return "📅 <b>AGENDA</b>\nrien"


@pytest.fixture
def bot(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    db.add_wallet(WATCHED, "DEV_TEST", "reserve-cluster", "dev", 0, None)
    tg = Telegram("000:x", "-100123", db)
    envoyes = []

    async def fake_call(method, payload):
        envoyes.append((method, payload))
        return {"ok": True, "result": {"message_id": len(envoyes)}}

    monkeypatch.setattr(tg, "_call", fake_call)
    cfg = cfgmod.Config(**{**cfgmod.load().__dict__, "watchlist_path": tmp_path / "w.csv", "telegram_admins": [42]})
    b = Bot(cfg, db, tg, FakePipeline(db), FakeAgenda(), FakeWatcher(), None, {"tx": 7})
    yield b, envoyes, db
    db.close()


def message(text, chat=-100123, user=1):
    return {"message_id": 5, "text": text, "chat": {"id": chat, "type": "supergroup"}, "from": {"id": user}}


def textes(envoyes):
    return [p.get("text", "") for m, p in envoyes if m in ("sendMessage", "editMessageText")]


def test_aide_statut_watchlist_agenda(bot):
    b, envoyes, _db = bot
    for cmd in ("/aide", "/statut@derovia_coin_bot", "/watchlist", "/agenda"):
        asyncio.run(b.on_message(message(cmd)))
    t = textes(envoyes)
    assert len(t) == 4 and "COMMANDES" in t[0] and "ÉTAT DU RADAR" in t[1] and "WATCHLIST" in t[2]
    assert all(html_ok(x) for x in t), [x for x in t if not html_ok(x)]
    # les réponses partent dans la bonne conversation, en réponse à la commande, sans son
    p = envoyes[0][1]
    assert p["chat_id"] == -100123 and p["reply_parameters"]["message_id"] == 5 and p["disable_notification"]


def test_inconnu_refuse(bot):
    b, envoyes, _db = bot
    asyncio.run(b.on_message({"message_id": 1, "text": "/statut", "chat": {"id": 999, "type": "private"},
                              "from": {"id": 999}}))
    assert textes(envoyes) == ["⛔ Ce radar est privé."]


def test_admin_en_prive_autorise(bot):
    b, envoyes, _db = bot
    asyncio.run(b.on_message({"message_id": 1, "text": "/statut", "chat": {"id": 42, "type": "private"},
                              "from": {"id": 42}}))
    assert "ÉTAT DU RADAR" in textes(envoyes)[0]


def test_adresse_collee_affiche_la_fiche_wallet(bot, monkeypatch):
    b, envoyes, _db = bot

    async def pas_de_coins(http, a):
        return []

    monkeypatch.setattr("radar.bot.pumpfun.coins_by_creator", pas_de_coins)
    asyncio.run(b.on_message(message(WATCHED)))
    t = textes(envoyes)[0]
    assert "FICHE WALLET" in t and "DEV_TEST" in t and "1.500 SOL" in t and html_ok(t)


def test_bouton_couper_24h(bot):
    b, envoyes, db = bot
    cb = {"id": "cb1", "data": f"m:{WATCHED}", "from": {"id": 1},
          "message": {"message_id": 9, "chat": {"id": -100123}}}
    asyncio.run(b.on_callback(cb))
    assert float(db.get(f"mute:{WATCHED}")) > time.time() + 23 * 3600
    assert any(m == "answerCallbackQuery" for m, _p in envoyes)


def test_silence(bot):
    b, envoyes, _db = bot
    asyncio.run(b.on_message(message("/silence 30")))
    assert b.tg.silent and "sans son" in textes(envoyes)[0]
    asyncio.run(b.on_message(message("/silence off")))
    assert not b.tg.silent


def test_suivre_ecrit_dans_le_csv(bot, tmp_path):
    b, envoyes, _db = bot
    nouveau = "33yhak3xPxpcRB9XbhbhSYgqsZa1rBmF7k55VydUXEHp"

    async def watch(addr, *a):
        b.p.watched.add(addr)
        return True

    b.p.watch = watch
    asyncio.run(b.on_message(message(f"/suivre {nouveau} Mon dev")))
    assert nouveau in (tmp_path / "w.csv").read_text(encoding="utf-8") and "Surveillé" in textes(envoyes)[0]


def test_toutes_les_alertes_en_html_valide():
    info = TokenInfo(MINT, name="Ash <born>", symbol="ASH", creator=WATCHED, created_ts=int(time.time()) - 60,
                     supply_raw=10**15, dev_coins=[], twitter="https://x.com/AshbornCoin/status/1",
                     flags=["mint authority active (le dev peut imprimer des tokens)"])
    texte = A.card("🔴 <b>DEV CRÉE UN TOKEN</b>", info, ["a & b < c", "x"] * 3,
                   [A.wallet_line(WATCHED, "DEV <1>", "grp & co")], A.token_block(info, []))
    assert html_ok(texte) and "PRUDENCE" in texte and "&lt;born&gt;" in texte
    kb = A.token_buttons(info, WATCHED, mute=WATCHED, follow=WATCHED)
    assert all(len(b.get("callback_data", "")) <= 64 for row in kb["inline_keyboard"] for b in row)
    assert re.search(r"n:\w+", str(kb))   # bouton « 🕸 Réseau du dev »
