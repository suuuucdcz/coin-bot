"""File d'envoi Telegram : nouveaux essais, oubli de la clé après échec définitif, remplacement d'un message."""
import asyncio

from radar import telegram as T
from radar.db import DB


def run_worker(tg, scenario):
    async def go():
        w = asyncio.create_task(tg.worker())
        try:
            return await scenario()
        finally:
            w.cancel()
    return asyncio.run(go())


def test_echec_definitif_oublie_la_cle(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "RETRY_DELAY", 0.01)
    monkeypatch.setattr(T, "MIN_INTERVAL", 0)
    db = DB(tmp_path / "r.db")
    tg = T.Telegram("token", "chat", db)
    essais = []

    async def send_ko(*a, **k):
        essais.append(1)
        raise RuntimeError("réseau coupé")

    tg.send_now = send_ko

    async def scenario():
        assert tg.enqueue("alerte", key="create:X", kind="create")
        await asyncio.sleep(0.3)

    run_worker(tg, scenario)
    assert len(essais) == T.SEND_RETRIES + 1
    assert not db.alert_already_sent("create:X")   # pourra repartir si l'événement se reproduit
    db.close()


def test_remplacement_du_message_rapide(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "MIN_INTERVAL", 0)
    db = DB(tmp_path / "r.db")
    tg = T.Telegram("token", "chat", db)
    edits = []

    async def send_ok(*a, **k):
        return {"result": {"message_id": 42}}

    async def edit(mid, text, markup=None):
        edits.append((mid, text))
        return True

    tg.send_now, tg.edit_now = send_ok, edit

    async def scenario():
        tg.enqueue("rapide", key="create:Y", kind="create")
        await tg.replace("create:Y", "complet")

    run_worker(tg, scenario)
    assert edits == [(42, "complet")]
    db.close()
