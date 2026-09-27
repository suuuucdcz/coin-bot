"""📈 Suivi des résultats (chaque alerte suivie 24 h) et ⚡ délai événement -> Telegram."""
import asyncio
import time

from conftest import MINT, WATCHED
from test_pipeline import setup  # noqa: F401  (fixture partagée)

from radar import results as R
from radar.analysis.classify import Event
from radar.db import DB
from radar.telegram import Telegram

AUTRE = "Ash2pumpxxxxxxxxxxxxxxxxxxxxxxxxxxxxxpump"


def test_mesures_plus_haut_et_fin_de_suivi(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    res = R.Results(db, http=object())
    t0 = int(time.time()) - 25 * 3600
    db.add_result("create:A", AUTRE, "create", "CAT", "découverte", "prouvé", 10_000, sent_at=t0)
    marche = {"mc": 30_000}

    async def markets(http, mints):
        return {AUTRE: dict(marche)}

    async def coin(http, mint):
        # ATH de 80 k$ atteint APRÈS l'alerte : c'est le plus haut, même si aucune mesure ne l'a vu
        return {"mc": marche["mc"], "ath": 80_000, "ath_ts": t0 + 600}

    monkeypatch.setattr(R.dexscreener, "markets", markets)
    monkeypatch.setattr(R.pumpfun, "coin", coin)
    asyncio.run(res.check_once(now=t0 + 2 * 3600))           # mesure à +2 h
    r = db.results_since(0)[0]
    assert r["mc0"] == 10_000 and r["mc_max"] == 80_000 and r["mc_1h"] == 30_000 and not r["done"]
    marche["mc"] = 2_000                                       # à +24 h : il ne reste que 20 %
    asyncio.run(res.check_once(now=t0 + 24 * 3600 + 60))
    r = db.results_since(0)[0]
    assert r["done"] and r["mc_24h"] == 2_000
    assert R._mult(r) == 8 and R._rug(r)
    texte = res.report()
    assert "🔴 Créations</b> : 1 · ×2 : 1 (100 %) · ×5 : 1 · rug : 1 (100 %)" in texte
    assert "$CAT ×8.0" in texte and "prouvé : 1 alertes" in texte


def test_ath_d_avant_l_alerte_ignore(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    res = R.Results(db, http=object())
    t0 = int(time.time()) - 3600
    db.add_result("buy:A", AUTRE, "buy", "OLD", None, "faible", 50_000, sent_at=t0)

    async def markets(http, mints):
        return {AUTRE: {"mc": 40_000}}

    async def coin(http, mint):
        return {"mc": 40_000, "ath": 900_000, "ath_ts": t0 - 86400}   # vieux sommet : pas un résultat de l'alerte

    monkeypatch.setattr(R.dexscreener, "markets", markets)
    monkeypatch.setattr(R.pumpfun, "coin", coin)
    asyncio.run(res.check_once(now=t0 + 600))
    assert db.results_since(0)[0]["mc_max"] == 40_000


def test_alerte_envoyee_mise_en_suivi_avec_confiance(setup):  # noqa: F811
    p, tg, db = setup
    p.results = R.Results(db, None)
    ev = Event("create", WATCHED, "sig", int(time.time()) - 3, MINT, sol=1.0, tokens_raw=10**13,
               extra={"symbol": "ASH"})

    async def go():
        p.emit(await p.process(ev))
        await asyncio.sleep(0)
    asyncio.run(go())
    rows = db.results_since(0)
    assert [(r["kind"], r["mint"], r["grp"]) for r in rows] == [("create", MINT, "reserve-cluster")]
    assert rows[0]["trust"] == p.trust(WATCHED)


def test_bilan_vide_explique():
    assert "Pas encore de mesure" in R.report_text([])


def test_delai_evenement_jusqu_a_telegram(tmp_path, monkeypatch):
    db = DB(tmp_path / "r.db")
    tg = Telegram("000:x", "-100123", db)

    async def fake_call(method, payload):
        return {"ok": True, "result": {"message_id": 1}}
    monkeypatch.setattr(tg, "_call", fake_call)
    monkeypatch.setattr("radar.telegram.MIN_INTERVAL", 0)

    async def go():
        tg.enqueue("🔴 création", key="create:X", kind="create", event_ts=time.time() - 4)
        tg.enqueue("vieux", key="create:Y", kind="create", event_ts=time.time() - 3600)   # rattrapage : exclu
        tache = asyncio.create_task(tg.worker())
        for _ in range(50):
            if len(tg.latencies) == 2:
                break
            await asyncio.sleep(0.01)
        tache.cancel()
    asyncio.run(go())
    ligne = tg.latency_line()
    assert ligne and "médiane <b>4 s</b>" in ligne and "(1 alertes)" in ligne
