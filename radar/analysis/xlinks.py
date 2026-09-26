"""Liens X fiables : quel compte est le VRAI, et un token est-il vraiment celui qu'il prétend être ?

Principe : un lien ne compte comme une preuve que s'il existe DANS LES DEUX SENS.
  - N'importe qui peut écrire « twitter: x.com/ProjetOfficiel » dans les métadonnées de son faux token,
    ou coller le lien du tweet d'annonce : c'est un indice, jamais une preuve.
  - La preuve, c'est le compte officiel (ou le site lié dans SA bio) qui affiche CE contrat précis,
    ou le token créé par un wallet du dev repéré on-chain.
  - La certification bleue s'achète (abonnement X) : elle ne prouve rien.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

X_URL_RE = re.compile(r"(?:https?://)?(?:www\.|mobile\.)?(?:twitter|x)\.com/([^?#\s\"'<>]+)", re.I)
HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
RESERVED = {"i", "search", "home", "intent", "share", "hashtag", "explore", "settings", "messages",
            "notifications", "compose", "login", "signup", "tos", "privacy"}
# Mots ajoutés autour d'un nom pour imiter un compte (« AshbornCoin » -> « Ashborn_Official »)
AFFIXES = ("official", "offical", "onsolana", "onsol", "solana", "sol", "coin", "token", "meme", "xyz", "app",
           "hq", "io", "fun", "real", "the", "ai", "team", "dev", "labs", "cto", "portal")
# « we / our » SUIVI d'un mot de lancement : un caller écrit aussi « my gem » ou « join us » ; le projet
# écrit « we launch », « our CA », « our token goes live ».
FIRST_PERSON_RE = re.compile(
    r"\b(we|we're|we are|we'll|our|nous|notre|nos)\b[^.!?\n]{0,40}?\b(launch\w*|deploy\w*|live|ca|contract|"
    r"token|coin|lanc\w*|contrat)\b", re.I)


# --- lecture d'un lien X ------------------------------------------------------------------
@dataclass
class XLink:
    kind: str            # "profil" | "tweet" | "communauté" | "autre"
    handle: str | None   # compte du profil, ou auteur du tweet
    url: str

    def describe(self) -> str:
        if self.kind == "profil":
            return f"compte @{self.handle}"
        if self.kind == "tweet":
            return f"un tweet de @{self.handle}"
        if self.kind == "communauté":
            return "une communauté X"
        return "un lien X"


def parse_x_url(url: str | None) -> XLink | None:
    m = X_URL_RE.search(url or "")
    if not m:
        return None
    parts = [p for p in m.group(1).split("/") if p]
    if not parts:
        return None
    first = parts[0]
    if first.lower() == "i":
        kind = "communauté" if len(parts) >= 2 and parts[1] == "communities" else "autre"
        return XLink(kind, None, url or "")
    if first.lower() in RESERVED or not HANDLE_RE.fullmatch(first):
        return XLink("autre", None, url or "")
    if len(parts) >= 3 and parts[1] == "status":
        return XLink("tweet", first, url or "")
    return XLink("profil", first, url or "")


def profile_handle(url: str | None) -> str | None:
    """Compte X désigné par un lien de PROFIL (None pour un tweet, une communauté, une recherche)."""
    link = parse_x_url(url)
    return link.handle if link and link.kind == "profil" else None


# --- comptes qui en imitent un autre ------------------------------------------------------------
def _core(handle: str) -> str:
    h = handle.lower().replace("_", "")
    h = re.sub(r"\d+$", "", h)
    changed = True
    while changed and len(h) > 3:
        changed = False
        for a in AFFIXES:
            if h.endswith(a) and len(h) - len(a) >= 3:
                h, changed = h[: -len(a)], True
            elif h.startswith(a) and len(h) - len(a) >= 3:
                h, changed = h[len(a):], True
    return h


def _distance(a: str, b: str) -> int:
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def lookalike(a: str | None, b: str | None) -> bool:
    """@a ressemble à @b sans être le même compte (Ashborn_Official, AshbornCoin1, Ashb0rnCoin…)."""
    if not a or not b or a.lower() == b.lower():
        return False
    ca, cb = _core(a), _core(b)
    if len(ca) < 3 or len(cb) < 3:
        return False
    return ca == cb or (min(len(ca), len(cb)) >= 5 and _distance(ca, cb) <= 1)


def mentions_token(handle: str | None, ticker: str | None, name: str | None) -> bool:
    """Le nom du compte reprend-il le ticker ou le nom du token ?"""
    if not handle:
        return False
    core = re.sub(r"[^a-z0-9]", "", handle.lower())
    for w in (ticker, name):
        w = re.sub(r"[^a-z0-9]", "", (w or "").lower())
        if len(w) >= 3 and w in core:
            return True
    return False


# --- fiabilité d'un compte X --------------------------------------------------------------
@dataclass
class Trust:
    score: int
    plus: list[str] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)

    @property
    def level(self) -> str:
        return "fiable" if self.score >= 65 else "moyen" if self.score >= 40 else "douteux"

    @property
    def icon(self) -> str:
        return {"fiable": "🟢", "moyen": "🟡", "douteux": "🔴"}[self.level]


def account_age_days(prof: dict, now: datetime | None = None) -> int | None:
    joined = prof.get("joined") or ""
    m = re.fullmatch(r"(\d{2})/(\d{4})", joined)
    if not m:
        return None
    now = now or datetime.now()
    return max(0, (now - datetime(int(m.group(2)), int(m.group(1)), 1)).days)


def account_trust(prof: dict, ticker: str | None = None, name: str | None = None, ca: str | None = None,
                  now: datetime | None = None) -> Trust:
    """Note de 0 à 100 d'un compte X, à partir de ce qu'on peut vérifier (pas de ce qui s'achète)."""
    from .xparse import SCAM_PATTERNS
    t = Trust(50)
    age = account_age_days(prof, now)
    followers = prof.get("followers")
    following = prof.get("following")
    renames = prof.get("username_changes")
    vtype = prof.get("verified_type") or ("blue" if prof.get("verified") else None)

    if vtype == "blue":
        t.plus.append("certif bleue = abonnement payant, ne prouve rien")
    elif vtype in ("gold", "grey"):
        t.score += 20
        t.plus.append("badge organisation vérifiée (or)" if vtype == "gold" else "badge gris (institution)")

    if age is not None:
        if age < 30:
            t.score -= 30
            t.flags.append(f"compte X créé il y a moins d'un mois ({prof['joined']})")
        elif age < 90:
            t.score -= 15
            t.flags.append(f"compte X récent (créé {prof['joined']})")
        elif age > 365 and not renames:
            t.score += 10
            t.plus.append(f"compte ancien (créé {prof['joined']})")
    if renames:
        t.score -= 15 if renames == 1 else 30
        t.flags.append(f"a changé de nom d'utilisateur {renames} fois : compte racheté ou recyclé ?")
    if prof.get("based_in"):
        t.plus.append(f"basé : {prof['based_in']}")

    if followers is not None:
        if followers < 200:
            t.score -= 10
            t.flags.append(f"très peu d'abonnés ({followers})")
        elif followers >= 10_000:
            t.score += 10
            t.plus.append(f"{followers:,} abonnés".replace(",", " "))
        if age is not None and age < 60 and followers > 20_000:
            t.score -= 15
            t.flags.append(f"{followers:,} abonnés pour un compte de moins de 2 mois : abonnés achetés ?".replace(",", " "))
    if following and followers is not None and followers > 100 and following > 3 * followers:
        t.score -= 5
        t.flags.append("suit bien plus de comptes qu'il n'a d'abonnés (follow-for-follow)")

    bio = prof.get("bio") or ""
    if ca and ca in bio + " ".join(prof.get("links") or []):
        t.score += 15
        t.plus.append("le CA est dans sa bio")
    elif mentions_token(prof.get("handle"), ticker, name) or (ticker and f"${ticker.upper()}" in bio.upper()):
        t.score += 5
        t.plus.append("le compte porte le nom du token")
    for rx, label in SCAM_PATTERNS:
        if rx.search(bio):
            t.score -= 20
            t.flags.append(f"bio suspecte : {label}")
    t.score = max(0, min(100, t.score))
    return t


# --- preuve qu'un token est bien le coin annoncé ------------------------------------------------
@dataclass
class Evidence:
    strong: list[str] = field(default_factory=list)
    medium: list[str] = field(default_factory=list)
    weak: list[str] = field(default_factory=list)
    against: list[str] = field(default_factory=list)

    @property
    def level(self) -> str:
        if self.strong and not self.against:
            return "fort"
        if self.strong or (self.medium and not self.against):
            return "moyen"
        return "faible"

    @property
    def icon(self) -> str:
        return {"fort": "🟢", "moyen": "🟡", "faible": "🔴"}[self.level]

    def lines(self) -> list[str]:
        return ([f"✅ {x}" for x in self.strong] + [f"☑️ {x}" for x in self.medium]
                + [f"▫️ {x}" for x in self.weak] + [f"❌ {x}" for x in self.against])


def link_evidence(official: str | None, meta_twitter: str | None, by_dev: bool = False,
                  time_match: bool = False, other_handles: set[str] | None = None) -> Evidence:
    """Niveau de preuve qu'un token appartient au compte officiel `official`."""
    ev = Evidence()
    if by_dev:
        ev.strong.append("créé par un wallet du dev repéré on-chain avant le lancement")
    link = parse_x_url(meta_twitter)
    off = (official or "").lower()
    if link is None:
        ev.weak.append("aucun lien X dans les métadonnées du token")
    elif link.kind == "profil" and link.handle.lower() == off:
        ev.medium.append(f"métadonnées → @{link.handle}, le compte officiel (copiable : pas une preuve seule)")
    elif link.kind == "profil" and lookalike(link.handle, official):
        ev.against.append(f"métadonnées → @{link.handle}, qui IMITE @{official}")
    elif link.kind == "profil":
        other = {h.lower() for h in other_handles or set()}
        (ev.weak if link.handle.lower() in other else ev.against).append(
            f"métadonnées → @{link.handle}, pas le compte officiel @{official or '?'}")
    elif link.kind == "tweet":
        ev.weak.append(f"métadonnées → {link.describe()} (n'importe qui peut coller ce lien)")
    else:
        ev.weak.append(f"métadonnées → {link.describe()}")
    if time_match:
        ev.weak.append("créé à l'heure annoncée")
    return ev


def official_score(handle: str, text: str, ticker: str | None, name: str | None, mentions: int,
                   meta_handle: str | None = None) -> tuple[int, list[str]]:
    """Ce compte est-il celui du projet (et pas un caller qui relaie) ?"""
    score, why = 0, []
    if meta_handle and meta_handle.lower() == handle.lower():
        score += 3
        why.append("désigné par les métadonnées du token")
    if mentions_token(handle, ticker, name):
        score += 3
        why.append("le nom du compte reprend le token")
    if FIRST_PERSON_RE.search(text or ""):
        score += 2
        why.append("annonce à la 1re personne (« we / our »)")
    if mentions:
        score += min(3, mentions)
        why.append(f"cité par {mentions} autre(s) compte(s)")
    return score, why
