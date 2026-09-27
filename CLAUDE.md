# Memecoin Radar — cahier des charges pour Claude Code

Tu construis un programme **d'alerte** (pas de trading automatique) qui tourne en continu sur le PC Windows de Maxence et envoie des alertes sur **Telegram**.
Langue des messages et des commentaires : français.

## Objectif

Repérer un token **avant qu'il soit annoncé**, en suivant les développeurs :

1. On surveille une **watchlist de wallets dev / bank** (fichier `data/watchlist.csv`).
2. Dès qu'un de ces wallets **finance un nouveau wallet**, **crée un token** ou **achète la supply d'un token** → alerte immédiate.
3. Pour ce token, le programme **retrouve le lancement associé** : métadonnées (nom, ticker, X, site, Telegram), compte X lié, tweets qui mentionnent le ticker ou le CA.
4. En parallèle, une **veille X** repère les annonces de lancement (« launching today », « CA drops », comptes à rebours, contrat publié à l'avance comme $ASH) et **ajoute automatiquement** les wallets du déployeur à la watchlist.
5. Chaque nouveau token alerté est **tracé automatiquement** : funding du dev, anciens tokens, wallets frères (cluster), puis les nouveaux wallets suspects entrent dans la watchlist.

## Contraintes impératives

- **Aucune action de trading.** Pas de clé privée, pas de swap. Maxence achète lui-même sur Padre/Terminal. Sa priorité absolue est de **ne prendre aucun risque de ban** sur sa plateforme de trading : le programme ne s'y connecte jamais.
- Secrets uniquement dans `.env` (voir `.env.example`), jamais dans le code ni dans git.
- Le programme doit tourner **24/7 sur Windows** : il reconnecte automatiquement les websockets, gère le rate limit (backoff), et journalise dans `logs/`.
- Dédoublonnage : ne jamais renvoyer la même alerte (état dans SQLite).

## Stack conseillée

- Python 3.11+, `asyncio`, `aiohttp`, `websockets`
- SQLite (`data/radar.db`) pour l'état : wallets suivis, tokens vus, alertes envoyées, liens de cluster
- Telegram : appel HTTP direct à l'API Bot (`sendMessage`, HTML, boutons de lien)
- X : Playwright (Chromium) avec un **profil persistant** où Maxence est connecté (un compte secondaire est préférable)
- Lancement Windows : `run.bat` + option de tâche planifiée au démarrage

## Architecture (modules)

```
radar/
  main.py            # orchestre les tâches asyncio, reconnexions
  config.py          # lit .env
  db.py              # SQLite
  telegram.py        # envoi des alertes (file d'attente + anti-spam)
  sources/
    helius.py        # RPC + websockets Helius (logs/transactions des wallets suivis)
    pumpfun.py       # API pump.fun (métadonnées, tokens par créateur, nouveaux tokens)
    pumpportal.py    # websocket temps réel des nouveaux tokens et trades pump.fun (à valider)
    x_watch.py       # veille X via Playwright
  analysis/
    tracer.py        # remontée du funding (hop par hop) + cluster
    classify.py      # scoring / détection des patterns
  pipeline.py        # cœur : watchlist, confiance, « À ne pas rater », envoi ; evenements.py = un handler par événement
  agenda*.py         # veille X -> agenda (agenda_candidats, agenda_faux, agenda_affichage, agenda_outils)
  results.py         # suivi 24 h de chaque alerte (section 📈 Résultats, /resultats)
  top.py             # « ‼️ À ne pas rater » : sélection + contrôle du token 5 min après l'alerte
  smart.py           # smart money : gros détenteurs de plusieurs vrais succès -> alerte 🧠 quand ils entrent ensemble
  discovery.py       # découverte auto : créateurs pump.fun à succès (+ leur bank) -> watchlist
  setup.py           # assistant de configuration (configurer.bat)
data/watchlist.csv   # wallets de départ
tests/               # pytest, hors ligne (transactions fabriquées, faux Telegram)
```

Règles ajoutées à l'usage :

- **Priorité RPC** : tout travail de fond (traçage, agenda, chasse au dev, découverte) passe par `Pipeline._spawn` ou `in_background` : ses appels Helius cèdent la place aux alertes temps réel.
- **Alerte rapide** : création et ajout de liquidité partent tout de suite (CA et liens), puis le même message est complété (`Alert.replace`).
- La watchlist est plafonnée (`WATCH_MAX`) et purgée chaque jour des wallets auto-ajoutés inactifs (`WATCH_STALE_DAYS`).
- Tout ce qui concerne la santé du radar part dans le compartiment Telegram `system`.
- **Liens X (`analysis/xlinks.py`)** : un lien ne prouve que s'il existe dans les deux sens. Métadonnées → profil
  officiel = preuve moyenne (copiable) ; métadonnées → tweet = faible ; certification bleue = rien (payante).
  Preuve forte = wallet du dev repéré on-chain, ou compte officiel (tweets, bio, site de sa bio) qui affiche ce CA.
  Un CA tweeté par un autre compte que le compte officiel n'est jamais relié directement à l'annonce.
- **Jev (`analysis/jev.py`, optionnel)** : avis en plus des règles, jamais seul juge (pas conçu pour du contenu hostile).
- **IA de lecture des tweets (`analysis/llm.py`)** : Ollama sur le PC, API Gemini sur un serveur (`GEMINI_API_KEY`, quota
  quotidien plafonné). Elle ne fait que lire et choisir dans une liste ; sa réponse est vérifiée champ par champ. Sans CA ni
  heure, seule une annonce confirmée par l'IA crée une fiche d'agenda ; sans IA, il faut un CA ou une heure.
- **Serveur (Google Cloud e2-micro, `deploy/installer_serveur.sh`, `HEBERGEMENT.md`)** : un seul radar par bot (le radar
  prévient s'il en voit un 2e) ; `X_LIGHT=1` (navigateur allégé, fermé entre deux tours) et swap zram sur 1 Go.
- **Smart money (`smart.py`)** : seulement les succès de devs « découverte » (propres) ; parts identiques = bundle
  écarté ; 3 succès de 3 devs différents ; « À ne pas rater » à partir de 3 wallets ensemble (anti-appât).
- **Quota Helius gratuit (1 M crédits/mois)** : toute nouvelle fonction qui appelle Helius par événement ou à chaque
  démarrage doit être mise en cache ou mémorisée en base ; suivi dans `/statut` et le journal (« RPC par méthode »).

## Détection on-chain : les règles

Pour chaque wallet de la watchlist, il faut écouter ses transactions en temps réel (websocket Helius, abonnement aux logs qui mentionnent l'adresse) et parser chaque transaction (`getTransaction` en `jsonParsed`, `maxSupportedTransactionVersion: 1`).

| Événement | Comment le détecter | Alerte |
|---|---|---|
| **Funding d'un nouveau wallet** | SOL sortant vers une adresse qui a moins de 5 tx | « 🟡 BANK X a financé Y avec N SOL », puis Y entre en watchlist (profondeur max 3) |
| **Création de token** | Instruction `create` du programme pump.fun, ou création d'un mint par un wallet suivi | « 🔴 DEV X vient de créer $TICKER », avec le CA, le lien pump.fun et un lien graphique |
| **Achat de supply** | La transaction augmente le solde SPL d'un mint jeune (moins de 24 h) pour un wallet suivi (`postTokenBalances` − `preTokenBalances` > 0, avec SOL sortant) | « 🟠 DEV X achète $TICKER : N SOL, % de la supply » |
| **Retour de profits** | Gros SOL entrant depuis un wallet dev vers le bank | « ⚫ Profits rapatriés » : signe que le token vient d'être rug |
| **Vente des wallets de réserve** | Un wallet qui détient au moins 10 % d'un token suivi vend | « ⚠️ Vente d'un wallet de réserve » |

Pour la remontée du funding (`tracer.py`), méthode validée à la main :

- Pour une adresse A, prendre sa **plus ancienne** transaction réussie. La source est le compte dont le solde SOL baisse le plus (en général le fee payer). Ne pas se fier uniquement aux instructions `system transfer` : dans certains cas elles ne sont pas visibles.
- Si la source a **plus de 1 000 tx en quelques minutes**, c'est un hot wallet d'exchange ou un service. On arrête la remontée et on étiquette l'adresse.
- Les **chaînes de relais** (wallets à 2 tx qui se passent le même montant à la même minute) servent à brouiller la piste : il faut les suivre jusqu'au bout, en ignorant les frais.
- **Wallets frères** : lister les autres sorties de la source dans la même fenêtre de temps (même montant, par exemple 0,10 SOL vers 5 wallets en 1 minute). Ils appartiennent souvent au même opérateur et lanceront les prochains tokens.
- Pour chaque dev, appeler `GET https://frontend-api-v3.pump.fun/coins?creator=<addr>&limit=50&offset=0&includeNsfw=true` pour obtenir ses anciens tokens et leur ATH.
- **Address poisoning** : un transfert quasi nul (≤ 0,002 SOL) venant d'une adresse qui ressemble à une autre (mêmes 4 premiers et 4 derniers caractères) est du bruit. Il faut l'ignorer.

## Veille X (`x_watch.py`)

- Playwright avec un profil persistant (`X_PROFILE_DIR`). Au premier lancement, Maxence se connecte à la main.
- On fait tourner des recherches « Latest » (`https://x.com/search?q=...&f=live`) toutes les 2 à 5 minutes, avec un délai aléatoire. Requêtes de départ :
  - `"launching today" solana`, `"launches tomorrow" solana`, `"CA drops"`, `"stealth launch" pump`, `"fair launch" pump.fun`, `"official contract" solana`, `countdown token launch solana`, `UsePaid`
  - ajouter `since:<date d'hier>` pour rester sur du frais
- Profils à suivre (timeline) : liste dans `.env` (`X_ACCOUNTS`), par exemple @zenkaixbt, @1dev_zen.
- Extraction dans le DOM : `article[data-testid="tweet"]`, `[data-testid="User-Name"]`, `[data-testid="icon-verified"]`, `time[datetime]`, `[data-testid="tweetText"]`, et les stats via `[role="group"]` (attribut aria-label).
- Il faut repérer :
  - une **adresse Solana** (regex base58, 32 à 44 caractères, souvent terminée par `pump`) ;
  - un **ticker** (`$[A-Za-z0-9]{2,10}`) ;
  - une **heure de lancement** (`\d{1,2}:\d{2} ?UTC`, `PM ET`, `CET`…).
- Si un CA est publié **avant le lancement** (cas $ASH), on lit la mint et ses premières transactions, on retrouve le déployeur et les détenteurs de la supply, et on ajoute tout à la watchlist avec le groupe = ticker.
- Profil du compte, à récupérer : date de création, abonnés, certification. Et **signaux d'arnaque** : « drop your SOL address », « airdrop to first 2,500 », promesses de « 1000x », renvoi vers un Telegram pour le CA. Dans ce cas, l'alerte est marquée 🚩.
- Rythme lent pour ne pas faire bannir le compte X.

## Pump.fun (appris en pratique)

- Liste des tokens : `GET /coins?offset=0&limit=50&includeNsfw=false&sort=<market_cap|last_trade_timestamp|created_timestamp>&order=DESC&complete=<true|false>`. Champs utiles : `mint`, `name`, `symbol`, `creator`, `created_timestamp`, `usd_market_cap`, `ath_market_cap`, `complete`, `twitter`, `website`, `telegram`.
- ⚠️ `usd_market_cap` est parfois **absurde** (des milliards pour un token vieux de 5 h). Il faut plafonner cette valeur ou la recalculer avec une autre source avant de l'afficher.
- `GET /coins/<mint>` a renvoyé des erreurs : prévoir un repli.
- Nouveaux tokens et trades en temps réel : websocket PumpPortal (`wss://pumpportal.fun/api/data`, méthodes `subscribeNewToken` et `subscribeAccountTrade` avec une liste de wallets). **Vérifier la doc actuelle avant de l'utiliser.** Si ça marche, `subscribeAccountTrade` sur la watchlist est le moyen le plus rapide de détecter un achat de supply par un dev sur pump.fun.

## Solana RPC

- Utiliser **Helius** (clé gratuite, `HELIUS_API_KEY`). Les RPC publics ont posé problème :
  - `api.mainnet-beta.solana.com` bloque ;
  - publicnode tronque l'historique au-delà d'environ 2 jours et refuse les requêtes indexées (`getTokenLargestAccounts`, `getTokenAccountsByOwner` filtré par mint).
- Méthodes : `getSignaturesForAddress` (paginer avec `before`), `getTransaction`, `getBalance`, `getAccountInfo` (sur la mint : vérifier que `mintAuthority` et `freezeAuthority` sont à null), `getTokenLargestAccounts`.
- L'API Helius « Enhanced Transactions » (transactions déjà parsées, type SWAP) simplifie la détection des achats : **vérifier ce qu'autorise le plan gratuit**.

## Format d'une alerte Telegram

```
🟠 DEV ACHÈTE — $ASH (Ashborn)
Wallet : HiQd…6YnM  [ASH_DEV_MAIN]
Montant : 2.5 SOL → 3.1 % supply
CA : DU5L11pfQ1EyDWvBhM5sp2piGeHEYDy9sdTvDkfcKrNs
Âge token : 4 min · MC : 18 k$
Dev : 2 anciens tokens (ATH max 3,9 k$) 🚩
X : @AshbornCoin (226 abonnés, créé 09/2026)
[pump.fun] [Solscan] [DexScreener] [X]
```

Chaque alerte donne : le type d'événement, le wallet et son label, le CA complet (copiable), l'âge et la market cap, l'historique du dev, le compte X trouvé, les drapeaux rouges, et les liens.

## Ordre de construction (itératif, tester à chaque étape)

1. `config`, `db`, `telegram` → envoyer un message de test.
2. `tracer.py` en CLI : `python -m radar.analysis.tracer <adresse>` affiche la chaîne de funding et les wallets frères. Vérifier que la chaîne documentée ci-dessous est retrouvée.
3. Suivi temps réel de la watchlist (Helius websocket), avec les alertes funding, création et achat.
4. Module pump.fun et PumpPortal (enrichissement des tokens, anciens tokens du dev).
5. Veille X Playwright.
6. `run.bat`, reconnexion, logs, démarrage automatique Windows.

## Données de départ (analyse manuelle du 26/09/2026)

Voir `data/watchlist.csv`. Deux cas documentés servent à valider le traceur :

- **Cluster « Reserve »** (faux fonds souverains : WSOS, NTDA, UDR, AOR, VSOF) : bank `H9L2M3NNQEVxz5vShmYG2esJ1pwkPBrJdMNLBnnzhQSF`, seed `99yFt79XKfq4akF59TceJb42FC1Fb3M5B24YiChc9cf1`. Schéma : 0,10 SOL vers 5 relais, puis 90 SOL par dev, puis lancement et rug, puis les profits reviennent au bank. **Un opérateur qui fait des rugs en série.** À signaler, pas à acheter.
- **$ASH (Ashborn)**, lancement le 26/09 à 18:00 UTC sur Raydium, CA publié à l'avance. Chaîne : MEXC → `33yhak3x…` → mint `CGUfGKcQ…` → wallet LP `Bqxuzhw4…` (6 milliards). Wallet principal du dev : `HiQdmuwc…`, dont les anciens tokens pump.fun sont morts.
