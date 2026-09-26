# Héberger le radar gratuitement 24 h/24

## Quel hébergeur ?

| Offre | Verdict |
|---|---|
| **Oracle Cloud « Always Free »** (VM ARM, 2 cœurs, 12 Go, disque 200 Go) | ✅ **Celle-ci.** Vraie machine allumée en permanence, disque persistant (la base SQLite reste), Chromium pour la veille X. |
| Render (gratuit) | ❌ Pas de disque persistant : la base (alertes déjà envoyées, watchlist) serait effacée. Mise en veille sans trafic. |
| Railway, Fly.io, Koyeb | ❌ Essai limité ou mise en veille : pas fait pour un radar 24 h/24. |

Ce qu'il faut savoir sur Oracle :

- Une **carte bancaire** est demandée à l'inscription (vérification d'identité, rien n'est débité tant que tu restes sur les ressources « Always Free »).
- Depuis juin 2026, le gratuit ARM est limité à **2 cœurs et 12 Go** : prends exactement ça, pas plus.
- Oracle peut **récupérer une VM gratuite jugée inactive** (processeur, réseau ET mémoire sous 20 % pendant 7 jours). Le radar consomme peu. Deux parades :
  - passer le compte en **« Pay As You Go »** (les ressources Always Free restent gratuites ; mets une alerte de budget à 1 € par sécurité) ;
  - ou installer l'IA locale (`--ia`) : le modèle chargé occupe plus de 20 % de la mémoire.
- Le message « **Out of capacity** » à la création de la VM est fréquent : réessaie plus tard ou dans un autre « availability domain ».

## Étapes

### 1. Créer le serveur (toi, sur oracle.com/cloud/free)

1. Crée le compte, région proche (Paris, Marseille ou Francfort).
2. *Compute → Instances → Create instance* :
   - Image : **Canonical Ubuntu 24.04** (pas 22.04 : il faut Python 3.11 ou plus).
   - Shape : **Ampere, VM.Standard.A1.Flex, 2 OCPU, 12 Go**.
   - *Add SSH keys → Generate a key pair* : **télécharge la clé privée** (ex. `oracle.key`) dans le dossier du radar. Ne la partage jamais.
3. Note l'**adresse IP publique** de l'instance.

### 2. Préparer le PC

Dans le dossier du radar (PowerShell) :

1. **Ferme la fenêtre du radar** (run.bat). Le profil X et la base ne doivent plus être utilisés.
2. Exporte la session X (pas de mot de passe à retaper sur le serveur) :

   ```
   .venv\Scripts\python -m radar.sources.x_watch exporter
   ```

   Le fichier `data\session_x_export.json` **vaut un mot de passe** : il ne va que sur TON serveur, puis il est supprimé.

### 3. Copier le radar sur le serveur

Toujours dans PowerShell, dans le dossier du radar (remplace `IP` par l'adresse du serveur) :

```
ssh -i oracle.key ubuntu@IP "git clone https://github.com/suuuucdcz/coin-bot.git radar"
scp -i oracle.key .env ubuntu@IP:radar/.env
scp -i oracle.key data\radar.db data\session_x_export.json ubuntu@IP:radar/data/
del data\session_x_export.json
```

(Si Windows refuse la clé « trop ouverte » : clic droit sur `oracle.key` → Propriétés → Sécurité, ne laisse que ton compte.)

### 4. Installer et lancer

```
ssh -i oracle.key ubuntu@IP
cd radar
bash deploy/installer_serveur.sh
```

Ajoute `--ia` à la fin pour installer aussi l'IA locale (Ollama). Sur 2 cœurs ARM sans carte graphique, elle est lente (plusieurs dizaines de secondes par tweet). Sans elle, les tweets sont lus par les règles seules.

Le script installe tout, reprend la session X, puis lance le radar comme **service** : il démarre avec le serveur et redémarre tout seul en cas d'arrêt. Tu dois recevoir « 🛰 MEMECOIN RADAR EN LIGNE » dans la section système du groupe Telegram.

### 5. Ne garder qu'un seul radar

**Ne relance plus run.bat sur le PC.** Deux radars sur le même bot, c'est des alertes en double et des commandes Telegram en conflit.

Tu peux aussi retirer le démarrage automatique du PC s'il avait été installé (`desinstaller_demarrage.bat`).

## Au quotidien (connecté en SSH)

| Action | Commande |
|---|---|
| Journal en direct | `tail -f ~/radar/logs/radar.log` |
| État du service | `systemctl status memecoin-radar` |
| Relancer / arrêter | `sudo systemctl restart memecoin-radar` / `sudo systemctl stop memecoin-radar` |
| Mettre à jour le code | `cd ~/radar && git pull && sudo systemctl restart memecoin-radar` |
| Modifier un réglage | `nano ~/radar/.env` puis relancer |

Depuis Telegram, `/statut` suffit : sources, crédits Helius du mois, décisions de la dernière heure.

## Limites à connaître

- **Veille X depuis un serveur** : X voit ta session arriver depuis l'IP d'un datacenter et peut demander une vérification.
  - Utilise un **compte X secondaire**.
  - Si l'import échoue, le radar tourne quand même, sans la veille X.
- **Quota Helius** : identique au PC (1 million de crédits gratuits par mois). Le compteur est dans `/statut`.
- **Revenir au PC** : arrête le service (`sudo systemctl stop memecoin-radar`), récupère la base, puis relance run.bat :

  ```
  scp -i oracle.key ubuntu@IP:radar/data/radar.db data\
  ```
