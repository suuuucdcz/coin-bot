"""Market cap pump.fun, base SQLite, priorité RPC, pause de nuit X, analyse des tweets, config."""
import asyncio
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from radar import config as cfgmod
from radar.analysis.xparse import parse_tweet
from radar.db import DB
from radar.sources import helius
from radar.sources.pumpfun import CURVE_MC_MAX, _coin, market_cap_usd
from radar.sources.x_watch import quiet_until

PARIS = ZoneInfo("Europe/Paris")


# --- market cap pump.fun -------------------------------------------------------------
def test_mc_absurde_recalculee():
    """Cas vu en vrai : des milliards pour un token de 5 h. On recalcule avec MC en SOL × prix du SOL."""
    assert market_cap_usd({"usd_market_cap": 5e9, "market_cap": 30, "complete": False}, 200) == 6000


def test_mc_coherente_gardee():
    assert market_cap_usd({"usd_market_cap": 6100, "market_cap": 30, "complete": False}, 200) == 6100


def test_mc_depuis_reserves_de_la_curve():
    c = {"usd_market_cap": 9e12, "virtual_sol_reserves": 30e9, "virtual_token_reserves": 1.073e15,
         "total_supply": 1e15, "complete": False}
    assert abs(market_cap_usd(c, 200) - 30 / 1.073 * 200) < 1


def test_mc_impossible_sur_la_curve_sans_prix():
    assert market_cap_usd({"usd_market_cap": CURVE_MC_MAX * 10, "complete": False}, None) is None


def test_rug_detecte():
    c = _coin({"mint": "M", "creator": "C", "usd_market_cap": 2000, "ath_market_cap": 12_200_000})
    assert c["rug"] and c["creator"] == "C"


# --- base SQLite ----------------------------------------------------------------------
@pytest.fixture
def db(tmp_path):
    d = DB(tmp_path / "radar.db")
    yield d
    d.close()


def test_dedoublonnage_et_oubli(db):
    db.mark_alert_sent("create:X", "create")
    assert db.alert_already_sent("create:X")
    db.forget_alert("create:X")
    assert not db.alert_already_sent("create:X")


def test_purge_des_wallets_inactifs(db):
    vieux = int(time.time()) - 30 * 86400
    db.add_wallet("AUTO", "NEW_AUTO", "g", "financé", 1, "BANK")
    db.add_wallet("SEED", "BANK", "g", "bank", 0, None)
    db.conn.execute("UPDATE wallets SET added_at=?", (vieux,))
    db.conn.commit()
    assert db.stale_wallets(10) == ["AUTO"]          # la watchlist de départ n'est jamais purgée
    db.set_last_sig("AUTO", "sig")                   # activité récente -> gardé
    assert db.stale_wallets(10) == []
    db.deactivate(["AUTO"])
    assert db.wallet("AUTO")["active"] == 0
    assert db.add_wallet("AUTO", "NEW_AUTO", "g", "financé", 1, "BANK")  # réactivé s'il revient
    assert db.wallet("AUTO")["active"] == 1


# --- priorité RPC ---------------------------------------------------------------------
def test_le_temps_reel_passe_devant_le_tracage():
    async def scenario():
        rpc = helius.SolanaRPC("http://test", rps=20)
        ordre = []

        async def appel(nom):
            await rpc._throttle()
            ordre.append(nom)

        async def fond():
            await asyncio.gather(*(appel(f"fond{i}") for i in range(5)))

        t = asyncio.create_task(helius.in_background(fond()))
        await asyncio.sleep(0.12)                    # le traçage a commencé
        await asyncio.gather(appel("alerte1"), appel("alerte2"))
        await t
        return ordre

    ordre = asyncio.run(scenario())
    # les deux alertes passent avant la fin du traçage
    assert ordre.index("alerte2") < ordre.index("fond4")


# --- pause de nuit X ---------------------------------------------------------------------
def test_pause_de_nuit():
    assert quiet_until((3, 8), datetime(2026, 9, 26, 4, 30, tzinfo=PARIS)).hour == 8
    assert quiet_until((3, 8), datetime(2026, 9, 26, 12, 0, tzinfo=PARIS)) is None
    assert quiet_until((23, 6), datetime(2026, 9, 26, 23, 30, tzinfo=PARIS)).day == 27
    assert quiet_until(None) is None


# --- tweets ---------------------------------------------------------------------------------
REF = datetime(2026, 9, 26, 9, 0, tzinfo=timezone.utc)


def test_annonce_ash():
    i = parse_tweet("$ASH launching today 18:00 UTC on Raydium, CA: DU5L11pfQ1EyDWvBhM5sp2piGeHEYDy9sdTvDkfcKrNs", REF)
    assert i.is_candidate and i.tickers == ["ASH"] and i.platform == "Raydium"
    assert i.cas == ["DU5L11pfQ1EyDWvBhM5sp2piGeHEYDy9sdTvDkfcKrNs"]
    assert datetime.fromtimestamp(i.launch_ts, timezone.utc).hour == 18 and not i.launch_alts


def test_signaux_d_arnaque():
    i = parse_tweet("Stealth launch of $MOON on pump.fun at 2pm EST 1000x guaranteed, drop your SOL address", REF)
    assert i.is_candidate and len(i.scam) >= 2


def test_tweet_banal_ignore():
    assert not parse_tweet("gm frens, $SOL looking strong", REF).is_candidate


# --- config -----------------------------------------------------------------------------------
def test_valeurs_avec_commentaire(monkeypatch):
    monkeypatch.setenv("X_POLL_SECONDS", "240   # secondes")
    monkeypatch.setenv("X_QUIET_HOURS", "0")
    monkeypatch.setenv("X_HEADLESS", "0 # visible")
    cfg = cfgmod.load()
    assert cfg.x_poll_seconds == 240 and cfg.x_quiet_hours is None and cfg.x_headless is False


def test_heure_et_fuseau_sur_la_meme_ligne():
    # Vu en vrai ($SKY) : « GMT+8 6PM–10PM » puis « UTC 10AM–2PM » à la ligne -> lancement 10:00 UTC, pas 22:00
    i = parse_tweet("$SKY LAUNCHES TOMORROW\nGMT+8 6PM–10PM\nUTC 10AM–2PM", REF)
    assert datetime.fromtimestamp(i.launch_ts, timezone.utc).hour == 10
    i = parse_tweet("$ABC launch at 18:00 GMT+2", REF)
    assert datetime.fromtimestamp(i.launch_ts, timezone.utc).hour == 16
    assert parse_tweet("on se retrouve et 10 minutes après on lance $ABC", REF).launch_ts is None  # « et » ≠ ET


def test_date_ecrite_prime_sur_demain():
    # Vu en vrai : tweet du 25/09 « LAUNCHES TOMORROW Saturday 26/09 · 18:00 UTC », relu le 26 -> restait le 26
    lu_le_26 = datetime(2026, 9, 26, 9, 0, tzinfo=timezone.utc)
    i = parse_tweet("$ASH LAUNCHES TOMORROW Saturday 26/09 · 18:00 UTC", lu_le_26)
    assert datetime.fromtimestamp(i.launch_ts, timezone.utc).strftime("%d/%m %H:%M") == "26/09 18:00"
    i = parse_tweet("$SKY LAUNCHES TOMORROW — SEPTEMBER 26\nUTC 10AM–2PM", lu_le_26)
    assert datetime.fromtimestamp(i.launch_ts, timezone.utc).strftime("%d/%m %H:%M") == "26/09 10:00"
    i = parse_tweet("$BZL launches tomorrow 15:00 UTC, price 1.5x", lu_le_26)   # « 1.5 » n'est pas une date
    assert datetime.fromtimestamp(i.launch_ts, timezone.utc).strftime("%d/%m %H:%M") == "27/09 15:00"
