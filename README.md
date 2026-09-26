# Memecoin Radar

Radar d'**alertes** Telegram pour les memecoins Solana : il suit des wallets de devs et de clusters en temps réel, repère les lancements annoncés sur X et vérifie les liens (vrai compte, faux coins, opérateurs de rugs).

> **Alerte uniquement.** Aucune clé privée, aucun swap, aucune connexion à une plateforme de trading.

## Ce qu'il fait

- **On-chain (Helius + PumpPortal)** : il détecte le financement d'un nouveau wallet, une création de token, un achat de supply, un ajout de liquidité, les profits rapatriés et la vente d'un wallet de réserve.
- **Traçage** : il remonte le financement hop par hop, repère les wallets frères et les relais, et s'arrête sur les exchanges et les services.
- **Veille X (Playwright, gratuite)** : il lit les annonces de lancement, identifie le compte officiel probable et donne au compte une note de fiabilité (la certification bleue ne compte pas).
- **Liens fiables** : une preuve ne compte que si elle existe dans les deux sens. Par exemple, le compte officiel qui publie *ce* CA, ou un token créé par un wallet du dev repéré on-chain.
- **Découverte automatique** : il ajoute les devs de lancements qui ont vraiment marché (vérifiés sur DexScreener) et écarte les market caps gonflées ainsi que le schéma des faux fonds souverains.
- **Bot Telegram** : chaque alerte affiche un verdict (⛔ / 🟠 / 🟡 / 🟢), avec des boutons (tracer, suivre, couper 24 h). Commandes : `/statut`, `/token`, `/wallet`, `/tracer`, `/x`, `/suivre`, `/silence`… Un tableau de bord épinglé donne l'état du radar.
- **IA Jev (TypeSafe, optionnelle)** : elle donne un avis en plus des règles, jamais seule juge.

## Démarrage (Windows)

1. Installer Python 3.12 ou plus récent.
2. Double-cliquer sur `configurer.bat` : installation, clé Helius, bot Telegram, compte X, démarrage automatique.
3. Double-cliquer sur `run.bat`.

Détails : [GUIDE_DEMARRAGE.md](GUIDE_DEMARRAGE.md) · Cahier des charges : [CLAUDE.md](CLAUDE.md)

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests
```

Les secrets restent dans `.env` (jamais versionné). Modèle : `.env.example`.
