# Héberger le radar gratuitement 24 h/24 : Google Cloud

On utilise la machine **e2-micro « Always Free »** de Google Cloud. Elle est gratuite à vie, toujours disponible (pas de « out of capacity » comme chez Oracle), et son disque est persistant : la base SQLite est conservée.

Ce que ça donne :

| | |
|---|---|
| Radar on-chain (Helius, PumpPortal, Telegram) | ✅ comme sur le PC |
| Veille X | ✅ en mode allégé (sans images), si X accepte la session depuis le serveur |
| IA pour lire les tweets | ✅ via l'API Gemini gratuite (voir « Activer l'IA Gemini » plus bas) ; sans elle, règles strictes |
| Coût | 0 € si tu suis les réglages ci-dessous |

## Les 4 pièges qui feraient payer

1. **Région** : uniquement `us-central1` (Iowa), `us-east1` (Caroline du Sud) ou `us-west1` (Oregon).
2. **Type de machine** : `e2-micro`, rien d'autre.
3. **Disque** : **« Standard persistent disk », 30 Go maximum.** Le disque proposé par défaut (« Balanced ») est payant.
4. **Fin de l'essai** : l'inscription ouvre un essai de 90 jours (300 $ de crédit). **Avant la fin, clique « Passer à un compte payant »** (*Upgrade*). Sinon Google arrête puis supprime la machine. La machine e2-micro reste gratuite après.
   - Mets aussi une **alerte de budget à 1 €** : *Facturation → Budgets et alertes*.

Trafic sortant : 1 Go gratuit par mois. Le radar en consomme peu, et le mode allégé de la veille X (activé automatiquement) le limite.

## Étapes

### 1. Créer la machine (toi, sur console.cloud.google.com)

1. Crée le compte Google Cloud. La carte sert à la vérification ; rien n'est débité pendant l'essai.
2. *Compute Engine → Instances de VM → Créer une instance* :
   - Nom : `radar` ;
   - Région : `us-central1` (Iowa), zone au choix ;
   - Série **E2**, type **e2-micro** ;
   - *Disque de démarrage → Modifier* : **Ubuntu 24.04 LTS (x86/64)**, type **Disque persistant standard**, **30 Go** ;
   - Pare-feu : ne coche rien (le radar n'a besoin d'aucune connexion entrante).
3. *Créer*. La page doit indiquer que l'instance bénéficie de l'offre gratuite.

### 2. Préparer le PC

Dans le dossier du radar :

1. **Ferme la fenêtre du radar** (run.bat) : le profil X et la base ne doivent plus être utilisés.
2. Exporte la session X (pas de mot de passe à retaper sur le serveur) :

   ```
   .venv\Scripts\python -m radar.sources.x_watch exporter
   ```

   `data\session_x_export.json` **vaut un mot de passe** : il ne va que sur TON serveur, puis il est supprimé.

### 3. Envoyer le radar sur la machine

1. Dans la liste des instances, bouton **SSH** : un terminal s'ouvre dans le navigateur, sans clé à gérer.
2. Dans ce terminal :

   ```
   git clone https://github.com/suuuucdcz/coin-bot.git radar
   ```

3. En haut du terminal, **⚙️ / « Importer un fichier »** (*Upload file*). Envoie ces trois fichiers du dossier du radar :
   - `.env`
   - `data\radar.db`
   - `data\session_x_export.json`
4. Ils arrivent dans ton dossier personnel. Range-les :

   ```
   mv ~/.env ~/radar/.env && mv ~/radar.db ~/session_x_export.json ~/radar/data/
   ```

   Si `.env` n'apparaît pas, vérifie avec `ls -a ~`.
5. Sur le PC, supprime `data\session_x_export.json`.

### 4. Installer et lancer

Dans le terminal SSH :

```
cd ~/radar && bash deploy/installer_serveur.sh
```

Le script fait tout, en 5 à 10 minutes :
- 2 Go de swap (mémoire d'appoint) ;
- Python et Chromium, avec le navigateur allégé ;
- reprise de la session X ;
- lancement du radar comme **service** : il démarre avec la machine et redémarre tout seul s'il s'arrête.

Tu dois recevoir « 🛰 MEMECOIN RADAR EN LIGNE » dans la section système du groupe Telegram.

### 5. Ne garder qu'un seul radar

**Ne relance plus run.bat sur le PC.** Deux radars sur le même bot, c'est des alertes en double et des commandes Telegram en conflit.

## Activer l'IA Gemini (lecture des tweets)

Le serveur n'a pas de carte graphique pour l'IA locale : il utilise l'API Gemini, gratuite (≈ 500 lectures par jour, le radar
s'arrête à 450 puis repasse sur les règles strictes jusqu'au lendemain).

1. Sur `aistudio.google.com/apikey`, crée une clé dans un projet **sans facturation** (un `gen-lang-client-…` ou celui
   qu'AI Studio propose). Dans un projet facturé, c'est le palier payant. Ne colle jamais la clé dans une discussion.
2. Dans le terminal SSH du serveur (remplace `TA_CLE`, garde `GEMINI_API_KEY=` devant) :

   ```
   echo 'GEMINI_API_KEY=TA_CLE' >> ~/radar/.env && sed -i 's/^LLM_ENABLED=.*/LLM_ENABLED=1/' ~/radar/.env && sudo systemctl restart memecoin-radar
   ```

3. `/statut` doit afficher « 🧠 IA : 🟢 gemini-3.5-flash-lite (API Gemini) » avec le quota du jour.

Changer de clé : `nano ~/radar/.env`, remplace la valeur de la ligne `GEMINI_API_KEY=`, puis relance le service.

## Au quotidien (bouton SSH de la console)

| Action | Commande |
|---|---|
| Journal en direct | `tail -f ~/radar/logs/radar.log` |
| État du service | `systemctl status memecoin-radar` |
| Relancer / arrêter | `sudo systemctl restart memecoin-radar` / `sudo systemctl stop memecoin-radar` |
| Mettre à jour le code | `cd ~/radar && git pull && sudo systemctl restart memecoin-radar` |
| Modifier un réglage | `nano ~/radar/.env` puis relancer |
| Mémoire | `free -h` |

Depuis Telegram, `/statut` suffit : sources, crédits Helius du mois, décisions de la dernière heure.

## Si quelque chose coince

- **Veille X refusée** (X voit ta session arriver d'un datacenter américain) :
  1. Reconnecte-toi sur le PC avec connexion_x.bat.
  2. Réexporte la session, renvoie le fichier, puis lance `cd ~/radar && .venv/bin/python -m radar.sources.x_watch importer && sudo systemctl restart memecoin-radar`.
  3. Si X refuse encore, le radar tourne sans veille X.

  Un compte X secondaire est fortement conseillé.
- **Manque de mémoire** (le journal parle de Chromium qui plante) : mets `X_ENABLED=0` dans `.env` et relance. Le radar on-chain seul tient largement en 1 Go.
- **Revenir sur le PC** : arrête le service (`sudo systemctl stop memecoin-radar`). Télécharge `~/radar/data/radar.db` (menu ⚙️ « Télécharger un fichier » du terminal SSH), remets-le dans `data\` sur le PC, puis relance run.bat.

## Autre option : Oracle Cloud

Oracle offre une machine plus puissante (2 cœurs ARM, 12 Go de mémoire, assez pour l'IA locale). Mais elle est souvent indisponible (« Out of capacity »), et Oracle peut récupérer une machine gratuite jugée inactive.

Si tu l'obtiens :
- passe le compte en « Pay As You Go » (toujours gratuit dans les limites « Always Free ») ;
- installe avec `bash deploy/installer_serveur.sh --ia` ;
- le reste des étapes est identique (Ubuntu 24.04).
