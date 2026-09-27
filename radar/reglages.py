"""Réglages du pipeline : seuils et fenêtres de temps (valeurs apprises sur des cas réels)."""
from __future__ import annotations

TOP_KINDS = ("create", "cluster", "lp_add", "match", "smart")   # alertes qui peuvent aller dans « ‼️ À ne pas rater »
YOUNG_TOKEN_S = 24 * 3600      # « mint jeune » = moins de 24 h
NEW_WALLET_MAX_TX = 5          # un wallet avec moins de 5 tx = nouveau wallet
SUPPLY_IN_MIN_PCT = 1.0        # réception de supply significative
RESERVE_MIN_PCT = 10.0         # wallet de réserve = détient au moins 10 %
DEV_BIG_BUY_PCT = 20.0         # achat initial du dev au-delà duquel on met un drapeau rouge
BURST_WINDOW_S = 120
BURST_MAX_ALERTS = 5           # au-delà, les fundings en rafale sont ajoutés sans alerte
TRADE_KINDS = {"buy", "sell", "supply_in"}
CLUSTER_WINDOW_S = 15 * 60     # achats d'un même groupe dans cette fenêtre = entrée du cluster
TRADE_MAX_PER_HOUR = 3         # alertes achat/vente max par wallet et par heure
TRADE_MUTE_S = 6 * 3600        # durée de la sourdine d'un wallet qui trade en boucle
FARM_WINDOW_S = 6 * 3600
FARM_MIN_TOKENS = 4            # un groupe qui « entre » dans 4 tokens différents en 6 h = ferme de bots
SNIPER_MIN_TOKENS = 5          # un wallet qui achète 5 tokens différents en 6 h = sniper
FUNDED_MAX_24H = 12            # au-delà, un financeur est un distributeur : ses nouveaux wallets ne sont plus ajoutés
FACTORY_FLAG_24H = 3           # 3 tokens créés en 24 h : signal grave (lanceur en série)
FACTORY_UNWATCH_24H = 5        # 5 tokens créés en 24 h : usine / plateforme de lancement, plus un dev à suivre
SUPPLY_OUT_MIN_PCT = 1.0       # déplacement de supply signalé au-delà de 1 % de la supply
CHECK5_S = 300                 # contrôle d'une alerte « à ne pas rater » 5 min après l'envoi
TOP_RETRY_S = (60, 180)        # alerte « à ne pas rater » retentée quand seules des données manquaient
LAUNCH_OLD_S = 30 * 60        # pool plus vieux que ça : le token s'échange déjà, ce n'est plus un lancement
INDEPENDENT_GROUPS = {"découverte", "manuel"}   # wallets rassemblés par le radar, sans lien entre eux
