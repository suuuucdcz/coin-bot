# Guide de démarrage (Windows)

Programme d'**alerte uniquement** : pas de clé privée, pas de trading, aucune connexion à Padre/Terminal.

## 1. Configuration (une seule fois, environ 10 min)

Double-clique sur **`configurer.bat`**. L'assistant installe ce qu'il faut (au premier lancement), puis te guide étape par étape :

1. **Clé Helius** (gratuite) : crée un compte sur https://dashboard.helius.dev et colle ta clé. L'assistant la teste.
2. **Bot Telegram** : dans Telegram, ouvre **@BotFather** (badge bleu), envoie `/newbot`, puis colle le token qu'il te donne.
3. **Où recevoir les alertes** :
   - simple : une conversation privée avec ton bot (bouton « Démarrer ») ;
   - recommandé : un **groupe à sujets**. Crée un groupe, ajoute le bot, active **Sujets** dans les paramètres du groupe, mets le bot **administrateur** (droits « Gérer les sujets » et « Épingler des messages »), puis écris `/start` dans le groupe.
   L'assistant trouve la conversation tout seul, tu n'as qu'à choisir son numéro.
4. **Message de test** sur Telegram.
5. **Compte X** : une fenêtre Edge s'ouvre et tu te connectes à X (compte secondaire conseillé). Elle se ferme d'elle-même une fois la connexion détectée.
6. **Démarrage automatique** avec Windows (facultatif).

Relance `configurer.bat` quand tu veux changer un réglage : Entrée = garder la valeur actuelle.
Tes secrets sont dans le fichier `.env`, que tu ne dois jamais partager.

Python doit être installé (3.12 ou plus récent, depuis python.org, avec la case « Add python.exe to PATH » cochée).

## 2. Lancer le radar

Double-clique sur **`run.bat`**. Laisse la fenêtre ouverte : la fermer arrête le radar. Il redémarre tout seul s'il plante.

| Fichier | Rôle |
|---|---|
| `configurer.bat` | installation, puis tes comptes (Helius, Telegram, X, démarrage auto) |
| `run.bat` | lance le radar 24 h/24 |
| `connexion_x.bat` | se reconnecter à X (si le radar signale « session X déconnectée ») |
| `desinstaller_demarrage.bat` | arrête le lancement automatique avec Windows |

## 3. Les alertes

Compartiments Telegram (groupe à sujets) : 📅 Agenda X · 🔴 Alertes dev on-chain · 🟡 Mouvements des clusters · 🧬 Devs & satellites · 🚩 Arnaques repérées · 🎭 Faux coins · ⚙️ État du radar.

- 🔴 un wallet suivi crée un token. L'alerte part **tout de suite** avec le CA, puis le même message se complète quelques secondes plus tard : dev, X, market cap, drapeaux 🚩.
- 🟢 liquidité ajoutée ou premier pool sur un contrat suivi : le trading est ouvert (même principe d'alerte immédiate).
- 🎯 le dev ou plusieurs wallets d'un même cluster entrent dans un token jeune.
- 🟠 un wallet suivi achète un token de moins de 24 h · 🟣 il reçoit de la supply sans payer.
- 🟡 un wallet suivi finance un nouveau wallet, qui entre alors dans la watchlist.
- ⚫ profits rapatriés vers le bank (rug probable) · 🔁 transfert interne · 🏦 envoi vers un exchange.
- ⚠️ vente du dev ou d'un wallet qui détient au moins 10 % de la supply.
- 🧭 **découverte automatique** (toutes les 6 h) : les créateurs de tokens pump.fun qui ont vraiment marché ces 3 derniers jours entrent dans la watchlist, avec le wallet qui les a financés.
- ⚙️ **état du radar** : coupure internet, Helius qui refuse la clé ou sature, session X déconnectée, et un bilan chaque matin.

## 4. La base de données

`data/watchlist.csv` est la référence des wallets de départ : une adresse invalide ou en double est ignorée et signalée dans ⚙️ État du radar. Un label modifié est mis à jour, et une ligne supprimée met le wallet en veille.


Tout est dans `data/radar.db` (SQLite), créé au premier lancement : wallets suivis, tokens vus, alertes déjà envoyées (pas de doublon), liens entre wallets, annonces X.

```powershell
.\.venv\Scripts\python -m radar.db            # résumé
.\.venv\Scripts\python -m radar.db wallets    # wallets suivis
```

La watchlist est limitée à `WATCH_MAX` adresses (300 par défaut, pour le plan gratuit Helius). Chaque matin, les wallets ajoutés automatiquement et restés inactifs `WATCH_STALE_DAYS` jours sont mis en veille ; ils reviennent s'ils bougent de nouveau. Ceux de `data/watchlist.csv` ne sont jamais purgés.

## 5. Outils en ligne de commande

```powershell
# Remonter le funding d'une adresse et ses wallets frères
.\.venv\Scripts\python -m radar.analysis.tracer <adresse> [--hops 5] [--sans-freres] [--ajouter] [--json]

# Rejouer une transaction passée comme si elle arrivait (sans rien envoyer, ou avec --envoyer)
.\.venv\Scripts\python -m radar.analysis.classify <signature>

# Voir ce que la découverte automatique ajouterait (n'ajoute rien)
.\.venv\Scripts\python -m radar.discovery

# Tester la veille X (2 recherches)
.\.venv\Scripts\python -m radar.sources.x_watch test

# Tests automatiques
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
.\.venv\Scripts\python -m pytest tests
```

## 6. Comment le radar relie un token à un compte X

Le radar ne se fie qu'aux liens qui existent **dans les deux sens** :

| Indice | Valeur |
|---|---|
| Token créé par un wallet du dev repéré on-chain | 🟢 preuve forte |
| Le compte officiel affiche **ce** CA (tweet, bio, site lié dans sa bio) | 🟢 preuve forte |
| Métadonnées du token → profil du compte officiel | 🟡 moyen : n'importe qui peut le copier |
| Métadonnées → un **tweet** (même celui de l'annonce) | 🔴 faible : lien copiable |
| Métadonnées → un compte qui **imite** le compte officiel | ❌ contre |
| Certification bleue | rien : elle s'achète |

- **Compte officiel probable** : pas forcément le premier qui parle du ticker (souvent un caller). Le radar préfère le compte qui porte le nom du token, parle à la 1re personne, est cité par les autres ou désigné par les métadonnées.
- **Fiabilité du compte** (note sur 100) : âge, abonnés (et abonnés achetés), **changements de nom d'utilisateur** (compte racheté ou recyclé), pays, bio. Pour voir la fiche d'un compte :
  ```powershell
  .\.venv\Scripts\python -m radar.sources.x_watch profil NomDuCompte
  ```
- Un candidat 🟡 n'est **jamais** annoncé comme le bon coin : le radar vérifie ensuite pendant 1 h si le compte officiel publie ce CA, et envoie 🎯 seulement à ce moment-là.

## 7. IA Jev (optionnel)

Avec une clé TypeSafe (`configurer.bat`, étape 6), Jev donne un avis rapide avec une probabilité :
- un tweet est-il une vraie annonce, la promo d'un caller ou une arnaque ?
- un compte X est-il un projet, un caller, une célébrité ou un bot ?

Il ajoute des drapeaux 🚩 ou aide à choisir le compte officiel, mais ne confirme **jamais** un token tout seul : Jev n'est pas conçu pour du contenu fait pour tromper. Coût : environ 0,04 $ par million de tokens, soit quelques centimes par mois. Sans clé, le radar fonctionne exactement pareil.

## 8. Veille X : limiter le risque de ban

- Compte secondaire conseillé. Le radar ne publie rien et ne like rien : il lit seulement.
- Rythme lent et aléatoire, avec une **pause la nuit** (`X_QUIET_HOURS=3-8`, heure de Paris).
- Si X bloque le mode invisible, mets `X_HEADLESS=0` dans `.env` : la fenêtre sera visible.
- Si X te demande souvent de te reconnecter, augmente `X_POLL_SECONDS` (par exemple 300).
