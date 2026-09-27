"""Agenda X : réglages et petits outils partagés (heures de Paris, compte à rebours, ticker normalisé)."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from . import alerts as A

PARIS = ZoneInfo("Europe/Paris")
MATCH_WINDOW_S = 36 * 3600          # une annonce reste « active » 36 h
PROBABLE_WINDOW_S = 20 * 60         # même ticker créé à ± 20 min de l'heure annoncée = probable
FRESH_CA_S = 3 * 3600               # un CA déjà tradé depuis plus de 3 h = coin déjà lancé (ignoré)
COMMON_FUNDER_STRONG = 3            # un wallet qui a financé ≥ 3 acheteurs du faux = opération du dev
# Vérification d'un candidat : le compte officiel (X, bio, site de sa bio) affiche-t-il CE contrat ?
VERIFY_DELAYS_S = (30, 120, 300, 900, 1800)
PROFILE_FRESH_S = 1800
READ_PER_BATCH = 12          # tweets lus par l'IA par page X (plafond ; 4 avec le quota gratuit de Gemini)
PLAN_EVERY_S = 600           # le chef d'orchestre choisit les prochaines recherches X toutes les 10 min


def norm_ticker(s: str | None) -> str:
    return "".join(ch for ch in (s or "").upper() if ch.isalnum())


def paris(ts: int) -> str:
    return datetime.fromtimestamp(ts, PARIS).strftime("%H:%M")


def utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M UTC")


def countdown(ts: int) -> str:
    d = ts - int(time.time())
    if d <= 0:
        return f"il y a {A.age(-d)}"
    return f"dans {A.age(d)}"
