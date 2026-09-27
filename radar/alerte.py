"""Une alerte prête à partir : texte, boutons, section Telegram, et de quoi la suivre ensuite."""
from __future__ import annotations

from dataclasses import dataclass

from .alerts import RUG_MARK
from .analysis.enrich import TokenInfo


# Compartiment Telegram de chaque type d'alerte
KIND_TOPIC = {"create": "onchain", "buy": "onchain", "supply_in": "onchain", "lp_add": "onchain", "supply_out": "onchain",
              "sell": "onchain", "transfer": "clusters", "funding": "clusters", "cex": "clusters",
              "trace": "devs", "mute": "clusters", "cluster": "onchain", "discovery": "devs",
              "system": "system", "resultats": "resultats",
              "smart": "onchain"}


@dataclass
class Alert:
    key: str
    kind: str
    text: str
    markup: dict | None = None
    replace: bool = False   # complète l'alerte rapide déjà envoyée sous la même clé
    # « 🎯 À ne pas rater » : titre et raison, seulement pour les signaux vérifiés et sans drapeau grave
    top_title: str = ""
    top_why: str = ""
    info: TokenInfo | None = None
    flags: list[str] | None = None
    wallet: str | None = None       # wallet suivi à l'origine (suivi des résultats : groupe, confiance)
    event_ts: int | None = None     # heure de l'événement on-chain (mesure du délai jusqu'à Telegram)

    @property
    def topic(self) -> str:
        # Tout ce qui touche un cluster de rugs connu part dans 🚩 Arnaques repérées
        return "scams" if RUG_MARK in self.text else KIND_TOPIC.get(self.kind, "onchain")
