"""Assistant de configuration : tes comptes (Helius, bot Telegram, groupe, X) en quelques questions.

Lancement : double-clic sur configurer.bat  (ou  python -m radar.setup)
Chaque étape peut être passée : Entrée = garder la valeur actuelle.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import sys
from pathlib import Path

import aiohttp

from . import config as cfgmod

ENV = cfgmod.ROOT / ".env"
EXAMPLE = cfgmod.ROOT / ".env.example"
STARTUP = Path(os.environ.get("APPDATA", "")) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
STARTUP_FILE = STARTUP / "memecoin-radar.bat"


# --- saisie -------------------------------------------------------------------------
def titre(n: int, texte: str) -> None:
    print(f"\n{'─' * 70}\n  ÉTAPE {n} — {texte}\n{'─' * 70}")


def demander(question: str, actuel: str = "", secret: bool = False) -> str:
    montre = cfgmod.mask(actuel) if secret else actuel
    suffixe = f" [Entrée = garder {montre}]" if actuel else ""
    try:
        v = input(f"{question}{suffixe} : ").strip()
    except EOFError:
        v = ""
    return v or actuel


def oui(question: str, defaut: bool = True) -> bool:
    try:
        v = input(f"{question} [{'O/n' if defaut else 'o/N'}] : ").strip().lower()
    except EOFError:
        v = ""
    return defaut if not v else v.startswith(("o", "y"))


def ecrire(cle: str, valeur: str) -> None:
    cfgmod.set_env_value(cle, valeur)
    os.environ[cle] = valeur  # pour que la suite de l'assistant voie la nouvelle valeur


# --- vérifications en ligne -------------------------------------------------------------
async def verifier_helius(cle: str) -> tuple[bool, str]:
    url = f"https://mainnet.helius-rpc.com/?api-key={cle}"
    try:
        async with aiohttp.ClientSession() as s, s.post(
                url, json={"jsonrpc": "2.0", "id": 1, "method": "getSlot"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status in (401, 403):
                return False, "clé refusée par Helius"
            data = await r.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        return False, f"Helius injoignable ({e})"
    if isinstance(data.get("result"), int):
        return True, f"OK (bloc {data['result']:,})".replace(",", " ")
    return False, str(data.get("error") or data)[:120]


async def telegram(token: str, methode: str, **params) -> dict:
    url = f"https://api.telegram.org/bot{token}/{methode}"
    try:
        async with aiohttp.ClientSession() as s, s.post(url, json=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        return {"ok": False, "description": f"Telegram injoignable ({e})"}


async def chats_recents(token: str) -> dict[int, str]:
    data = await telegram(token, "getUpdates")
    vus: dict[int, str] = {}
    for u in data.get("result", []) if data.get("ok") else []:
        msg = u.get("message") or u.get("channel_post") or u.get("my_chat_member") or {}
        chat = msg.get("chat")
        if chat:
            genre = ("groupe à sujets ✅" if chat.get("is_forum") else
                     "conversation privée" if chat.get("type") == "private" else "groupe sans sujets")
            nom = chat.get("title") or chat.get("first_name") or chat.get("username") or "?"
            vus[chat["id"]] = f"{nom} — {genre}"
    return vus


# --- étapes -------------------------------------------------------------------------------
def etape_helius() -> None:
    titre(1, "Clé Helius (lecture de la blockchain Solana, gratuit)")
    print("1. Va sur https://dashboard.helius.dev et crée un compte gratuit (plan « Free »).")
    print("2. Copie ta clé API (menu « API Keys ») et colle-la ici.")
    for _ in range(3):
        cle = demander("Clé Helius", os.getenv("HELIUS_API_KEY", ""), secret=True)
        if not cle:
            print("⚠️  Sans clé Helius, le radar ne peut pas surveiller les wallets en temps réel.")
            return
        ok, info = asyncio.run(verifier_helius(cle))
        print(("✅ " if ok else "❌ ") + info)
        if ok:
            ecrire("HELIUS_API_KEY", cle)
            return
        os.environ["HELIUS_API_KEY"] = ""
    print("Clé non enregistrée : relance configurer.bat quand tu l'auras.")


def etape_bot() -> str | None:
    titre(2, "Ton bot Telegram")
    print("1. Dans Telegram, ouvre @BotFather (badge bleu), envoie /newbot.")
    print("2. Choisis un nom, puis un identifiant qui finit par « bot » (ex. radar_maxence_bot).")
    print("3. BotFather te donne un token du genre 123456789:AA… : colle-le ici.")
    for _ in range(3):
        token = demander("Token du bot", os.getenv("TELEGRAM_BOT_TOKEN", ""), secret=True)
        if not token:
            return None
        me = asyncio.run(telegram(token, "getMe"))
        if me.get("ok"):
            nom = me["result"].get("username")
            print(f"✅ Bot trouvé : @{nom}")
            ecrire("TELEGRAM_BOT_TOKEN", token)
            return nom
        print(f"❌ Token refusé : {me.get('description')}")
        os.environ["TELEGRAM_BOT_TOKEN"] = ""
    return None


def etape_chat(bot: str) -> bool:
    titre(3, "Où recevoir les alertes")
    actuel = os.getenv("TELEGRAM_CHAT_ID", "")
    if actuel and not oui(f"Une conversation est déjà configurée ({actuel}). En choisir une autre ?", False):
        return True
    print("Deux possibilités :")
    print(f"  A) Simple : ouvre https://t.me/{bot} et appuie sur « Démarrer ».")
    print("  B) Recommandé (alertes rangées par compartiments) :")
    print("     1. crée un groupe Telegram (juste toi) et ajoutes-y ton bot ;")
    print("     2. paramètres du groupe → active « Sujets » (Topics) ;")
    print("     3. mets le bot administrateur (droit « Gérer les sujets ») ;")
    print("     4. écris /start dans le groupe.")
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    for _ in range(5):
        input("\nQuand c'est fait, appuie sur Entrée…")
        chats = asyncio.run(chats_recents(token))
        if not chats:
            print("Aucun message reçu par le bot. Écris /start (au bot ou dans le groupe) puis réessaie.")
            continue
        liste = list(chats.items())
        for i, (cid, nom) in enumerate(liste, 1):
            print(f"  {i}. {nom}  (id {cid})")
        choix = input("Numéro de la conversation à utiliser [1] : ").strip() or "1"
        if choix.isdigit() and 1 <= int(choix) <= len(liste):
            ecrire("TELEGRAM_CHAT_ID", str(liste[int(choix) - 1][0]))
            print("✅ Conversation enregistrée.")
            return True
    return False


def etape_test() -> None:
    titre(4, "Message de test sur Telegram")
    if not oui("Envoyer un message de test maintenant ?"):
        return
    from . import telegram as tg
    cfgmod.setup_logging("telegram")
    code = asyncio.run(tg._test())
    print("✅ Regarde ton Telegram." if code == 0 else "❌ Le test a échoué (voir le message ci-dessus).")


def etape_x() -> None:
    titre(5, "Veille X (gratuite, via ton navigateur Edge)")
    print("Le radar lit X avec un navigateur à part, connecté à TON compte X.")
    print("⚠️  Utilise de préférence un compte secondaire : le rythme est lent, mais le risque zéro n'existe pas.")
    comptes = demander("Comptes X à suivre (séparés par des virgules, sans @)", os.getenv("X_ACCOUNTS", ""))
    ecrire("X_ACCOUNTS", ",".join(c.strip().lstrip("@") for c in comptes.split(",") if c.strip()))
    cfg = cfgmod.load()
    from .sources.x_watch import SESSION_MARKER, has_session, login
    if has_session(cfg.x_profile_dir):
        if not oui("Une session X existe déjà. Te reconnecter (autre compte) ?", False):
            ecrire("X_ENABLED", "1")
            return
        (cfg.x_profile_dir / SESSION_MARKER).unlink()  # le profil actuel sera mis de côté
    if not oui("Te connecter à X maintenant ?"):
        print("Plus tard : double-clique sur connexion_x.bat.")
        return
    if asyncio.run(login(cfg)):
        ecrire("X_ENABLED", "1")


async def verifier_jev(cle: str) -> tuple[bool, str]:
    try:
        async with aiohttp.ClientSession() as s, s.get(
                "https://api.typesafe.ai/v1/models", headers={"Authorization": f"Bearer {cle}"},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 401:
                return False, "clé refusée par TypeSafe"
            return r.status == 200, f"HTTP {r.status}"
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        return False, f"TypeSafe injoignable ({e})"


def etape_jev() -> None:
    titre(6, "IA Jev de TypeSafe (optionnel, presque gratuit)")
    print("Jev donne un avis rapide en plus des règles : tweet = vraie annonce, promo d'un caller ou arnaque ?")
    print("Compte X = projet, caller, célébrité ou bot ? Il n'a jamais le dernier mot.")
    print("Clé : https://console.typesafe.ai (accès anticipé). Environ 0,04 $ par million de tokens.")
    cle = demander("Clé TypeSafe (Entrée pour passer)", os.getenv("TYPESAFE_API_KEY", ""), secret=True)
    if not cle:
        return
    ok, info = asyncio.run(verifier_jev(cle))
    print(("✅ " if ok else "❌ ") + info)
    if ok:
        ecrire("TYPESAFE_API_KEY", cle)


def etape_demarrage() -> None:
    titre(7, "Démarrage automatique avec Windows")
    if STARTUP_FILE.exists():
        print("✅ Déjà activé (le radar se lance à l'ouverture de session).")
        if oui("Le désactiver ?", False):
            STARTUP_FILE.unlink()
            print("Démarrage automatique désactivé.")
        return
    if not oui("Lancer le radar automatiquement quand tu ouvres ta session Windows ?"):
        return
    run_bat = cfgmod.ROOT / "run.bat"
    STARTUP.mkdir(parents=True, exist_ok=True)
    STARTUP_FILE.write_text(f'@echo off\r\nstart "Memecoin Radar" /min "{run_bat}"\r\n', encoding="utf-8")
    print("✅ Activé. (Pour l'enlever : relance configurer.bat ou desinstaller_demarrage.bat)")


def main() -> int:
    cfgmod.setup_logging("setup")
    if not ENV.exists() and EXAMPLE.exists():
        shutil.copy(EXAMPLE, ENV)
    print("=" * 70)
    print("  MEMECOIN RADAR — configuration")
    print("  Programme d'ALERTE uniquement : aucune clé privée, aucun trading,")
    print("  aucune connexion à ta plateforme de trading.")
    print("  Entrée = garder la valeur actuelle. Tes secrets restent dans le fichier .env.")
    print("=" * 70)
    etape_helius()
    bot = etape_bot()
    if bot and etape_chat(bot):
        etape_test()
    else:
        print("⚠️  Telegram pas configuré : le radar ne pourra pas t'envoyer d'alertes.")
    etape_x()
    etape_jev()
    etape_demarrage()
    cfg = cfgmod.load()
    from .sources.x_watch import has_session
    print("\n" + "=" * 70)
    print("  RÉCAPITULATIF")
    print(f"  Helius   : {'✅' if cfg.helius_api_key else '❌ manquant'}")
    print(f"  Telegram : {'✅' if cfg.telegram_bot_token and cfg.telegram_chat_id else '❌ incomplet'}")
    print(f"  X        : {'✅ connecté' if has_session(cfg.x_profile_dir) else '⚠️  non connecté (connexion_x.bat)'}")
    print(f"  IA Jev   : {'✅' if cfg.typesafe_api_key else 'non (optionnel)'}")
    print(f"  Démarrage auto : {'✅' if STARTUP_FILE.exists() else 'non'}")
    print("  → Pour lancer le radar : double-clique sur run.bat")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
