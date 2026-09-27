"""Confiance dans un wallet suivi, selon COMMENT il a été trouvé (référence, prouvé, lié, faible)."""
from __future__ import annotations


def is_dev_role(role: str | None) -> bool:
    """Wallet de dev (et pas « financé par DEV_X » ni « a financé le dev » : le mot seul ne suffit pas)."""
    r = (role or "").strip().lower()
    return r.startswith(("dev", "wallet principal du dev"))


def is_upstream_role(role: str | None) -> bool:
    """Wallet « en amont » d'un cluster (bank, seed, source, distributeur) : là où reviennent les profits."""
    r = (role or "").lower()
    return any(k in r for k in ("bank", "seed", "source", "distributeur", "financeur"))
TRUSTED = ("référence", "prouvé", "lié")


def wallet_trust(row, parent_trust: str | None = None, bad_groups: set[str] | frozenset = frozenset(),
                 parent_role: str | None = None) -> str:
    """Confiance dans un wallet suivi, selon COMMENT il a été trouvé.

    référence : watchlist de départ (hors cluster de rugs) · prouvé : dev d'un vrai succès (vérifié
    DexScreener) ou dev relié à un compte X par un lien dans les deux sens · lié : financé directement par
    un wallet de confiance, ou adresse publiée par le compte officiel · faible : tout le reste (satellites,
    acheteurs, détenteurs, créateurs ou financeurs de faux coins, chaînes de financement lointaines).
    Vu en vrai : 30 alertes « le cluster entre » déclenchées par les acheteurs d'un faux coin = une ferme de bots.
    """
    if row is None:
        return "faible"
    r = (row["role"] or "").lower()
    if row["grp"] in bad_groups or r.startswith("dev reclassé"):
        # Vu en vrai : un dev reclassé ⛔ gardait « référence » (niveau 0), et les wallets qu'il finance
        # devenaient « de confiance ». Un wallet d'un groupe à éviter n'est jamais de confiance.
        return "faible"
    if row["depth"] == 0 and row["grp"] != "découverte":
        return "référence"
    if r.startswith("dev (découverte"):
        return "prouvé"
    if r.startswith("dev probable") and "renvoie vers" in r:
        return "prouvé"   # CA publié par le compte officiel ET le token renvoie vers lui
    if r.startswith("dev probable") and "adresse publiée par" in r:
        return "lié"
    if r.startswith(("bank probable", "financé par")) and parent_trust in ("référence", "prouvé"):
        return "lié"
    if r.startswith("financé par") and parent_trust == "lié" and (parent_role or "").lower().startswith("bank probable"):
        # Nouveau wallet financé par le bank d'un dev à succès : c'est LE scénario suivi (le dev relance avec un
        # wallet neuf). Vu à la relecture : il retombait « faible », donc sa création n'allait jamais dans ‼️.
        return "lié"
    return "faible"


def is_smart_role(role: str | None) -> bool:
    """Wallet « smart money » (radar/smart.py) : il achète beaucoup de tokens, c'est normal."""
    return (role or "").strip().lower().startswith("smart money")


def watch_priority(role: str | None, depth: int) -> int:
    """0 = watchlist de départ, 1 = dev / wallet financé / bank / contrat, 2 = satellite, acheteur, détenteur."""
    r = (role or "").lower()
    if depth == 0:
        return 0
    if is_dev_role(role) or is_upstream_role(role) or r.startswith(("financé par", "smart money")) or "contrat" in r:
        return 1
    return 2


def is_service(row, label: str | None) -> bool:
    txt = f"{row['role'] if row else ''} {label or ''}".lower()
    return any(k in txt for k in ("hot wallet", "cex", "exchange", "usine à tokens"))
