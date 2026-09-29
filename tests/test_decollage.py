"""🚀 Décolle proprement : photo de chaque décollage, filtres de la section, jugement à 24 h et bilan."""
import asyncio
import time

from test_top import setup  # noqa: F401  (fixture partagée)

from radar import config as cfgmod
from radar import decollage as D
from radar import lancements as L
from radar import pipeline as pl

PROPRE = {"mc": 45_000, "liquidity": 18_000, "txns24h": 240, "buys5": 150, "sells5": 80, "vol5": 22_000, "change5": 60}


def _radar(db, tg, monkeypatch, detenteurs=(12.0, 3.0, 2, 1.1)):
    p = pl.Pipeline(cfgmod.load(), db, rpc=object(), http=object(), tg=tg)
    p.lancements = L.LaunchWatch(p)
    d = D.Decollage(p)
    p.decollage = d

    async def holders(rpc, mint, supply_raw, creator):
        return detenteurs
    monkeypatch.setattr(D, "holders", holders)
    return p, d


def _t(i=0, age_s=240, dev=1.5, creator=None, now=None):
    return {"mint": f"MINT{i}" + "x" * 36, "creator": creator or f"CRE{i}" + "c" * 37, "symbol": f"TK{i}", "name": "Tok",
            "ts": (now or time.time()) - age_s, "achat_dev": dev}


def test_decollage_propre_publie_et_plafonne(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    p, d = _radar(db, tg, monkeypatch)
    for i in range(7):
        asyncio.run(d.observer(_t(i), PROPRE))
    publies = [s for s in tg.sent if "DÉCOLLE PROPREMENT" in s]
    assert len(publies) == D.ALERTES_PAR_H and "top 10 12 %" in publies[0] and "mise de départ 1.5 SOL" in publies[0]
    rows = db.decollages_depuis(0)
    assert len(rows) == 7 and sum(r["alerte"] for r in rows) == 6
    assert [r["refus"] for r in rows if not r["alerte"]] == ["plafond d'alertes atteint"]


def test_filtres_de_la_section(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    p, d = _radar(db, tg, monkeypatch)
    rug = "RUGdevrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
    db.add_wallet(rug, "DEV_RESERVE", "reserve-cluster", "dev", 1, None)
    cas = [(_t(1, creator=rug), PROPRE, "réseau à rugs"),
           (_t(2, dev=100), PROPRE, "mise du dev"),                           # schéma Reserve : 100 SOL à la création
           (_t(3), {**PROPRE, "buys5": 40, "sells5": 90}, "ventes"),
           (_t(4, age_s=30), PROPRE, "trop tôt"),
           (_t(5), {**PROPRE, "mc": 900_000}, "market cap")]
    for t, m, raison in cas:
        assert raison in asyncio.run(d.observer(t, m))["refus"]
    p2, d2 = _radar(db, tg, monkeypatch, detenteurs=(9.9, 0.5, 10, 9.9))       # 10 wallets à 0,99 % : ferme
    assert asyncio.run(d2.observer(_t(6), PROPRE))["refus"] == "ferme de wallets"
    assert not any("DÉCOLLE PROPREMENT" in s for s in tg.sent)
    assert len(db.decollages_depuis(0)) == 6                                    # tous photographiés quand même


def test_jugement_a_24_h_et_bilan(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    p, d = _radar(db, tg, monkeypatch)
    hier = time.time() - 25 * 3600
    for i in range(4):
        asyncio.run(d.observer(_t(i, dev=0.5 if i < 2 else 20, now=hier), PROPRE, now=hier))
    chutes = {"MINT0": (2_500_000, "lente"), "MINT1": (300_000, "aucune"), "MINT2": (4_000_000, "brutale")}

    async def chute_token(http, base, mint, fin=None):
        h = chutes.get(mint[:5])
        return {"pic": h[0], "pic_ts": 0, "chute": h[1], "duree": None} if h else {"pic": 20_000, "chute": "aucune"}

    async def markets(http, mints):
        return {m: {"mc": 50_000} for m in mints}
    monkeypatch.setattr(D.geckoterminal, "chute_token", chute_token)
    monkeypatch.setattr(D.dexscreener, "markets", markets)
    assert asyncio.run(d.juger()) == 4
    issues = {r["mint"][:5]: r["issue"] for r in db.decollages_depuis(0)}
    assert issues == {"MINT0": "gros succès", "MINT1": "succès", "MINT2": "rug", "MINT3": "mort"}
    b = d.bilan()
    assert "4 · succès 50 % (dont ≥ 1 M$ : 1) · rugs 25 %" in b
    assert "≤ 1 SOL : 2 · succès 100 %" in b and "> 10 SOL : 2 · succès 0 %" in b
    assert "4 photographiés · 4 jugés à 24 h · 2 succès · 2 publiés" in d.status_line()


def test_la_veille_des_lancements_prend_la_photo(setup, monkeypatch):  # noqa: F811
    db, tg = setup
    p, d = _radar(db, tg, monkeypatch)

    async def markets(http, mints):
        return {m: PROPRE for m in mints}
    monkeypatch.setattr(L.dexscreener, "markets", markets)
    w = p.lancements
    w.on_new_token({"mint": "MINTlv" + "x" * 34, "traderPublicKey": "CRElv" + "c" * 36, "symbol": "LV", "name": "Live",
                    "solAmount": 0.8})
    w.pending["MINTlv" + "x" * 34]["ts"] = time.time() - 240
    asyncio.run(w.check_once())
    row = db.decollages_depuis(0)[0]
    assert row["achat_dev"] == 0.8 and row["alerte"] == 1 and any("DÉCOLLE PROPREMENT — $LV" in s for s in tg.sent)


def test_issue():
    assert D.issue(None, None) == "mort" and D.issue(50_000, "aucune") == "mort"
    assert D.issue(3e6, "brutale") == "rug" and D.issue(3e6, "lente") == "gros succès"
    assert D.issue(400_000, "aucune") == "succès"
