"""Liens X : un faux token ne doit jamais être « confirmé » sur un lien copiable ; le vrai compte est reconnu."""
import asyncio
import json
import time
from datetime import datetime

import pytest

from radar import config as cfgmod
from radar.agenda import Agenda
from radar.analysis import xlinks
from radar.db import DB
from radar.sources.x_watch import parse_about

MINT = "DU5L11pfQ1EyDWvBhM5sp2piGeHEYDy9sdTvDkfcKrNs"
DEV = "HiQdmuwcQuzhL5KJxqMh7WGzMcdjjLfsGSe1pcLM6YnM"


# --- lecture des liens -----------------------------------------------------------------------
def test_types_de_liens():
    assert xlinks.parse_x_url("https://x.com/AshbornCoin").kind == "profil"
    lien = xlinks.parse_x_url("https://twitter.com/elonmusk/status/1234567")
    assert lien.kind == "tweet" and lien.handle == "elonmusk"
    assert xlinks.parse_x_url("https://x.com/i/communities/1789").kind == "communauté"
    assert xlinks.profile_handle("https://x.com/elonmusk/status/1") is None   # un tweet n'est pas le compte du projet


def test_comptes_qui_imitent():
    assert xlinks.lookalike("AshbornOfficial", "AshbornCoin")
    assert xlinks.lookalike("Ashb0rnCoin", "AshbornCoin")
    assert not xlinks.lookalike("AshbornCoin", "ashborncoin")          # même compte (casse)
    assert not xlinks.lookalike("zenkaixbt", "AshbornCoin")


# --- fiabilité d'un compte ---------------------------------------------------------------------
NOW = datetime(2026, 9, 26)


def test_certif_bleue_ne_rapporte_rien():
    sans = xlinks.account_trust({"followers": 5000, "joined": "01/2024"}, now=NOW)
    avec = xlinks.account_trust({"followers": 5000, "joined": "01/2024", "verified": True,
                                 "verified_type": "blue"}, now=NOW)
    assert avec.score == sans.score and any("payant" in p for p in avec.plus)


def test_compte_ancien_mais_renomme():
    t = xlinks.account_trust({"followers": 3000, "joined": "03/2019", "username_changes": 3}, now=NOW)
    assert t.level != "fiable" and any("racheté" in f for f in t.flags)


def test_compte_neuf_aux_abonnes_achetes():
    t = xlinks.account_trust({"followers": 45000, "joined": "09/2026"}, now=NOW)
    assert t.level == "douteux" and any("achetés" in f for f in t.flags)


def test_page_a_propos():
    assert parse_about("Date joined\nMarch 2021\nAccount based in\nNigeria\n4 username changes") == {
        "joined": "03/2021", "based_in": "Nigeria", "username_changes": 4}


# --- niveau de preuve ------------------------------------------------------------------------------
def test_lien_vers_le_tweet_d_annonce_n_est_pas_une_preuve():
    ev = xlinks.link_evidence("AshbornCoin", "https://x.com/AshbornCoin/status/99")
    assert ev.level == "faible"


def test_profil_officiel_dans_les_metadonnees_est_une_preuve_moyenne():
    assert xlinks.link_evidence("AshbornCoin", "https://x.com/AshbornCoin").level == "moyen"


def test_profil_qui_imite_est_contre():
    ev = xlinks.link_evidence("AshbornCoin", "https://x.com/AshbornOfficial", time_match=True)
    assert ev.level == "faible" and ev.against


def test_dev_on_chain_est_une_preuve_forte():
    assert xlinks.link_evidence("AshbornCoin", None, by_dev=True).level == "fort"


# --- agenda : bout en bout, hors ligne ----------------------------------------------------------------
class FakeTG:
    def __init__(self):
        self.msgs = []

    def enqueue(self, text, markup=None, key=None, kind="", topic=None, **_kw):
        self.msgs.append(text)
        return True

    async def edit_now(self, *a, **k):
        return True


class FakePipeline:
    def __init__(self, db):
        self.db, self.cfg, self.http, self.rpc = db, cfgmod.load(), None, None
        self.spawned = []

    def _spawn(self, coro, urgent=False):
        self.spawned.append(coro.__qualname__)
        coro.close()

    def label(self, a):
        return None

    def trust(self, a):
        return "prouvé"


@pytest.fixture
def agenda(tmp_path):
    db = DB(tmp_path / "r.db")
    ann_id = db.insert_announcement(
        ticker="ASH", handle="CryptoCaller", tweet_url="https://x.com/CryptoCaller/status/1",
        tweet_text="$ASH launching today 18:00 UTC, huge alpha",
        sources=json.dumps([
            {"handle": "CryptoCaller", "url": "https://x.com/CryptoCaller/status/1",
             "text": "$ASH launching today 18:00 UTC, huge alpha"},
            {"handle": "AshbornCoin", "url": "https://x.com/AshbornCoin/status/2",
             "text": "We are launching $ASH today at 18:00 UTC on Raydium"}]),
        launch_ts=int(time.time()) + 6 * 3600, status="annoncé", flags="[]")
    a = Agenda(FakePipeline(db), FakeTG())
    yield a, db, ann_id
    db.close()


def test_le_compte_officiel_n_est_pas_le_caller(agenda):
    a, db, ann_id = agenda
    official, why = a._official(db.announcement(ann_id))
    assert official == "AshbornCoin" and why


def test_faux_token_qui_colle_le_tweet_d_annonce(agenda):
    a, db, ann_id = agenda
    asyncio.run(a._candidate(db.announcement(ann_id), MINT, "Copieur111", "https://x.com/AshbornCoin/status/2",
                             "pump.fun"))
    assert db.announcement(ann_id)["ca"] is None and not a.tg.msgs   # compté comme copie, aucune alerte


def test_metadonnees_vers_le_profil_officiel_candidat_seulement(agenda):
    a, db, ann_id = agenda
    asyncio.run(a._candidate(db.announcement(ann_id), MINT, "Createur1", "https://x.com/AshbornCoin", "pump.fun"))
    assert db.announcement(ann_id)["ca"] is None
    assert "Candidat" in a.tg.msgs[0] and "_verify_official" in " ".join(a.p.spawned)


def test_cree_par_le_dev_repere_on_chain(agenda):
    a, db, ann_id = agenda
    asyncio.run(a._candidate(db.announcement(ann_id), MINT, DEV, None, "pump.fun", by_dev=True))
    assert db.announcement(ann_id)["ca"] == MINT and "COIN ANNONCÉ CRÉÉ" in a.tg.msgs[0]


def test_ca_tweete_par_un_autre_compte_n_est_pas_relie(agenda, monkeypatch):
    a, db, ann_id = agenda

    async def frais(ca):
        return True

    monkeypatch.setattr(a, "_fresh_ca", frais)
    from radar.analysis.xparse import parse_tweet
    t = {"handle": "RandomShill", "url": "https://x.com/RandomShill/status/3",
         "text": f"$ASH CA: {MINT} launching today"}
    asyncio.run(a.upsert(t, parse_tweet(t["text"])))
    assert db.announcement(ann_id)["ca"] is None and "Agenda._candidate" in a.p.spawned


def test_details_non_effaces_par_la_fiche_dev(tmp_path):
    """Régression : resolve() / traçage auto écrasaient `details` (faux coins, devs probables perdus)."""
    import inspect

    from radar import agenda as ag
    from radar import pipeline as pl
    assert "json.loads(row[\"details\"]" in inspect.getsource(ag.Agenda.resolve)
    assert "details.update" in inspect.getsource(pl.Pipeline._auto_trace)


def test_watchlist_csv_invalide(tmp_path):
    csv = tmp_path / "w.csv"
    csv.write_text("group,label,address,role,notes\n"
                   f"g,DEV,{DEV},dev,\n"
                   "g,MAUVAIS,pasuneadresse,dev,\n"
                   f"g,DOUBLON,{DEV},dev,\n", encoding="utf-8")
    db = DB(tmp_path / "r.db")
    assert db.import_watchlist(csv) == 1
    assert len(db.import_warnings) == 2
    db.close()


def test_fiche_agenda_html_valide(agenda):
    from test_bot import html_ok
    a, db, ann_id = agenda
    db.update_announcement(ann_id, account=json.dumps({"handle": "AshbornCoin", "followers": 226, "joined": "09/2026",
                                                       "verified": True, "verified_type": "blue",
                                                       "username_changes": 2}),
                           flags=json.dumps(["@AshbornCoin : compte X créé il y a moins d'un mois (09/2026)"]))
    texte, _kb = a.card(db.announcement(ann_id))
    assert html_ok(texte) and "Compte officiel probable : @AshbornCoin" in texte and "payante" in texte
    assert html_ok(a.render())
