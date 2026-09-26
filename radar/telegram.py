"""Envoi des alertes Telegram (API Bot en HTTP direct), rangées par sujets.

- Groupe « avec sujets » (forum) : chaque alerte va dans son compartiment ;
  les sujets sont créés automatiquement au premier lancement.
- Conversation privée : tout arrive au même endroit (repli).
- File d'attente anti-spam, dédoublonnage (SQLite), gestion du 429.

Commandes :
    python -m radar.telegram            -> message de test (dans chaque sujet si groupe)
    python -m radar.telegram chatid     -> affiche les chat_id des conversations récentes du bot
"""
from __future__ import annotations

import asyncio
import html
import logging
import re
import sys
import time
from collections import OrderedDict

import aiohttp

from . import config as cfgmod
from .db import DB

log = logging.getLogger("telegram")

API = "https://api.telegram.org/bot{token}/{method}"
MIN_INTERVAL = 1.1  # secondes entre deux messages
SEND_RETRIES = 3    # nouveaux essais (espacés de RETRY_DELAY) si Telegram est injoignable
RETRY_DELAY = 30

# Compartiments : clé -> (icône, titre, couleur de secours). L'icône doit exister dans
# getForumTopicIconStickers ; sinon l'emoji est mis devant le titre et la couleur sert d'icône.
TOPICS = {
    "agenda":   ("📆", "Agenda X — coins annoncés", 7322096),
    "onchain":  ("🔥", "Alertes dev on-chain", 16478047),
    "clusters": ("💸", "Mouvements des clusters", 16766590),
    "devs":     ("🧠", "Devs & satellites", 13338331),
    "scams":    ("🏴‍☠️", "Arnaques repérées", 16749490),
    "fakes":    ("🎭", "Faux coins du jour", 9367192),
    "system":   ("🤖", "État du radar", 7322096),
}
# Alertes secondaires : envoyées sans son (les importantes gardent la notification)
QUIET_KINDS = {"transfer", "cex", "mute", "discovery", "trace", "devs", "system", "fakes"}

BOT_COMMANDS = [
    ("statut", "État du radar : sources, wallets suivis, alertes"),
    ("agenda", "Coins annoncés sur X à venir"),
    ("token", "Fiche d'un token : /token <CA>"),
    ("wallet", "Ce que le radar sait d'un wallet : /wallet <adresse>"),
    ("tracer", "Remonter le financement : /tracer <adresse>"),
    ("suivre", "Surveiller un wallet : /suivre <adresse> [nom]"),
    ("retirer", "Arrêter de surveiller : /retirer <adresse>"),
    ("x", "Fiabilité d'un compte X : /x <compte>"),
    ("watchlist", "Wallets surveillés, par groupe"),
    ("silence", "Alertes sans son : /silence 60 ou /silence off"),
    ("aide", "Toutes les commandes"),
]
BOT_SHORT = "Radar d'alertes memecoins Solana : devs suivis, lancements annoncés sur X, arnaques. Aucun trading."
BOT_DESCRIPTION = (
    "🛰 Memecoin Radar\n\n"
    "Surveille en temps réel les wallets de devs et de clusters sur Solana, repère les lancements annoncés "
    "sur X et vérifie les liens (vrai compte, faux coins, rugs).\n\n"
    "Alerte uniquement : aucune clé privée, aucun trading.\n\nTape /aide pour les commandes."
)
PROFILE_VERSION = "3"


def esc(text: object) -> str:
    """Échappe un texte pour le mode HTML de Telegram."""
    return html.escape(str(text), quote=False)


def _button(text: str, value: str) -> dict:
    """Lien si la valeur est une URL, sinon bouton d'action (callback géré par radar/bot.py)."""
    if value.startswith(("http://", "https://", "tg://")):
        return {"text": text, "url": value}
    return {"text": text, "callback_data": value[:64]}


def buttons(*links: tuple[str, str], per_row: int = 4) -> dict:
    """Clavier sur une grille : buttons(("pump.fun", url), ("🧬 Tracer", "t:<adresse>"))."""
    items = [_button(t, u) for t, u in links if u]
    rows = [items[i:i + per_row] for i in range(0, len(items), per_row)]
    return {"inline_keyboard": rows}


def keyboard(*rows: list[tuple[str, str]]) -> dict:
    """Clavier ligne par ligne (lignes vides ignorées)."""
    return {"inline_keyboard": [[_button(t, u) for t, u in row if u] for row in rows if row]}


def strip_html(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text))


class TelegramError(RuntimeError):
    def __init__(self, method: str, code, description: str):
        super().__init__(f"Telegram {method} : {code} {description}")
        self.code, self.description = code, description or ""


class Telegram:
    def __init__(self, token: str, chat_id: str, db: DB | None = None):
        if not token or not chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN et TELEGRAM_CHAT_ID doivent être remplis dans .env")
        self.token = token
        self.chat_id = chat_id
        self.db = db
        self.forum = False
        self.threads: dict[str, int] = {}
        self.queue: asyncio.Queue = asyncio.Queue()
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()
        # clé d'alerte -> identifiant du message envoyé (pour compléter une alerte rapide)
        self._sent: OrderedDict[str, asyncio.Future] = OrderedDict()
        self._icons: dict[str, str] | None = None
        self.silent_until = float((db.get("silent_until") if db else None) or 0)
        self.last_alert_ts = 0.0
        self.sent_count = 0

    async def _call(self, method: str, payload: dict) -> dict:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        url = API.format(token=self.token, method=method)
        for tentative in range(6):
            try:
                async with self._session.post(url, json=payload) as r:
                    data = await r.json(content_type=None)
                if data.get("ok"):
                    return data
                if data.get("error_code") == 429:
                    attente = data.get("parameters", {}).get("retry_after", 5)
                    log.warning("Telegram limite le débit, pause de %ss", attente)
                    await asyncio.sleep(attente + 1)
                    continue
                new_id = (data.get("parameters") or {}).get("migrate_to_chat_id")
                if new_id and "chat_id" in payload:
                    # Activer les sujets transforme le groupe en supergroupe : nouvel identifiant
                    self._migrate(str(new_id))
                    payload["chat_id"] = self.chat_id
                    continue
                raise TelegramError(method, data.get("error_code"), data.get("description"))
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                attente = min(60, 2 ** tentative)
                log.warning("Erreur réseau Telegram (%s), nouvel essai dans %ss", e, attente)
                await asyncio.sleep(attente)
        raise RuntimeError(f"Telegram {method} : échec après plusieurs essais")

    def _migrate(self, new_id: str) -> None:
        if new_id == str(self.chat_id):
            return  # déjà fait (deux envois simultanés ont reçu la même réponse)
        log.info("Le groupe a changé d'identifiant : %s -> %s (.env mis à jour)", self.chat_id, new_id)
        self.chat_id = new_id
        self.forum = False
        self.threads.clear()
        try:
            cfgmod.set_env_value("TELEGRAM_CHAT_ID", new_id)
        except OSError as e:
            log.error("Impossible de mettre à jour .env : %s", e)

    async def watch_topics(self) -> None:
        """Toutes les 2 min : sujets activés ou bot devenu admin ? On crée les compartiments manquants."""
        while True:
            await asyncio.sleep(120)
            if self.forum and len(self.threads) == len(TOPICS):
                continue
            try:
                avant = set(self.threads)
                await self.setup_topics()
                for key in TOPICS:
                    if key in self.threads and key not in avant:
                        emoji, name, _c = TOPICS[key]
                        await self.send_now(f"✅ Compartiment prêt : {emoji} <b>{esc(name)}</b>", topic=key, quiet=True)
            except Exception as e:
                log.debug("Vérification des sujets : %s", e)

    # --- sujets -----------------------------------------------------------------------
    async def setup_topics(self) -> None:
        """Détecte si le chat est un groupe à sujets et crée les compartiments manquants."""
        chat = (await self._call("getChat", {"chat_id": self.chat_id}))["result"]
        self.forum = bool(chat.get("is_forum"))
        if not self.forum:
            log.info("Chat sans sujets : toutes les alertes arriveront au même endroit")
            return
        for key in TOPICS:
            saved = self.db.get(f"topic:{self.chat_id}:{key}") if self.db else None
            if saved:
                self.threads[key] = int(saved)
                continue
            try:
                await self._create_topic(key)
            except TelegramError as e:
                if "not enough rights" in e.description.lower() or "administrator" in e.description.lower():
                    self._warn_rights()
                    return
                raise

    def _warn_rights(self) -> None:
        """Sujets activés mais bot pas admin : on prévient une seule fois dans le groupe."""
        if self.db and self.db.get(f"rights_warned:{self.chat_id}"):
            return
        if self.db:
            self.db.put(f"rights_warned:{self.chat_id}", 1)
        log.warning("Sujets activés mais le bot n'est pas administrateur : compartiments non créés")
        self.queue.put_nowait((
            "⚙️ <b>Sujets activés 👍 il manque une étape</b>\n"
            "Pour que je range les alertes par compartiment, nomme-moi <b>administrateur</b> du groupe "
            "avec les droits <b>Gérer les sujets</b> et <b>Épingler des messages</b>.\n"
            "<i>Je crée les compartiments tout seul dans les 2 min qui suivent.</i>",
            None, None, None, None, None, 0, False))

    async def _topic_icons(self) -> dict[str, str]:
        """Emoji -> identifiant d'icône de sujet autorisée par Telegram."""
        if self._icons is None:
            try:
                res = await self._call("getForumTopicIconStickers", {})
                self._icons = {st["emoji"].replace("\ufe0f", ""): st["custom_emoji_id"]
                               for st in res.get("result", []) if st.get("emoji") and st.get("custom_emoji_id")}
            except Exception as e:
                log.debug("Icônes de sujets indisponibles : %s", e)
                self._icons = {}
        return self._icons

    async def _create_topic(self, key: str) -> int:
        emoji, title, color = TOPICS[key]
        icon = (await self._topic_icons()).get(emoji.replace("\ufe0f", ""))
        name = title if icon else f"{emoji} {title}"
        payload = {"chat_id": self.chat_id, "name": name, "icon_color": color}
        if icon:
            payload["icon_custom_emoji_id"] = icon
        res = await self._call("createForumTopic", payload)
        tid = res["result"]["message_thread_id"]
        self.threads[key] = tid
        if self.db:
            self.db.put(f"topic:{self.chat_id}:{key}", tid)
        log.info("Sujet créé : %s", name)
        return tid

    # --- envoi / modification ------------------------------------------------------------
    @property
    def silent(self) -> bool:
        return self.silent_until > time.time()

    def set_silence(self, minutes: int) -> None:
        self.silent_until = time.time() + minutes * 60 if minutes > 0 else 0
        if self.db:
            self.db.put("silent_until", int(self.silent_until) or None)

    async def send_now(self, text: str, reply_markup: dict | None = None, topic: str | None = None,
                       reply_to: int | None = None, quiet: bool = False, chat_id: str | int | None = None,
                       thread_id: int | None = None) -> dict:
        payload = {"chat_id": chat_id or self.chat_id, "text": text, "parse_mode": "HTML",
                   "link_preview_options": {"is_disabled": True}}
        if quiet or self.silent:
            payload["disable_notification"] = True
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if reply_to:
            payload["reply_parameters"] = {"message_id": reply_to, "allow_sending_without_reply": True}
        if thread_id:
            payload["message_thread_id"] = thread_id
        elif chat_id is None and self.forum and topic in self.threads:
            payload["message_thread_id"] = self.threads[topic]
        if len(text) > 4096:
            payload.update(text=strip_html(text)[:4090] + " …", parse_mode=None)
            payload.pop("parse_mode")
        try:
            return await self._call("sendMessage", payload)
        except TelegramError as e:
            desc = e.description.lower()
            if chat_id is None and self.forum and topic and "thread not found" in desc:
                # Sujet supprimé à la main : on le recrée
                payload["message_thread_id"] = await self._create_topic(topic)
                return await self._call("sendMessage", payload)
            if "can't parse entities" in desc:
                # HTML refusé (texte externe mal formé) : on envoie le texte brut plutôt que rien
                payload.pop("parse_mode", None)
                payload["text"] = strip_html(text)[:4096]
                return await self._call("sendMessage", payload)
            raise

    async def edit_in(self, chat_id: str | int, message_id: int, text: str, reply_markup: dict | None = None) -> bool:
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text[:4096], "parse_mode": "HTML",
                   "link_preview_options": {"is_disabled": True}}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            await self._call("editMessageText", payload)
            return True
        except TelegramError as e:
            if "not modified" in e.description:
                return True
            log.debug("Modification impossible : %s", e)
            return False

    async def answer_callback(self, callback_id: str, text: str = "", alert: bool = False) -> None:
        try:
            await self._call("answerCallbackQuery", {"callback_query_id": callback_id, "text": text[:200],
                                                     "show_alert": alert})
        except Exception as e:
            log.debug("answerCallbackQuery : %s", e)

    async def get_updates(self, offset: int, timeout: int = 25) -> list[dict]:
        """Messages et clics reçus par le bot (appel long : attend jusqu'à `timeout` s)."""
        url = API.format(token=self.token, method="getUpdates")
        body = {"offset": offset, "timeout": timeout, "allowed_updates": ["message", "callback_query"]}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout + 15)) as s:
            async with s.post(url, json=body) as r:
                data = await r.json(content_type=None)
        if not data.get("ok"):
            raise TelegramError("getUpdates", data.get("error_code"), data.get("description"))
        return data.get("result", [])

    async def setup_profile(self) -> None:
        """Commandes du menu, description et présentation du bot (une fois par version)."""
        if self.db and self.db.get("bot_profile") == PROFILE_VERSION:
            return
        cmds = [{"command": c, "description": d} for c, d in BOT_COMMANDS]
        for scope in ({"type": "default"}, {"type": "all_group_chats"}, {"type": "all_private_chats"}):
            await self._call("setMyCommands", {"commands": cmds, "scope": scope, "language_code": ""})
        await self._call("setMyShortDescription", {"short_description": BOT_SHORT})
        await self._call("setMyDescription", {"description": BOT_DESCRIPTION})
        if self.db:
            self.db.put("bot_profile", PROFILE_VERSION)
        log.info("Profil du bot mis à jour (commandes, description)")

    async def edit_now(self, message_id: int, text: str, reply_markup: dict | None = None) -> bool:
        """Modifie un message. False si le message n'existe plus (à renvoyer)."""
        payload = {"chat_id": self.chat_id, "message_id": message_id, "text": text[:4096],
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            await self._call("editMessageText", payload)
            return True
        except TelegramError as e:
            if "not modified" in e.description:
                return True
            log.warning("Modification impossible : %s", e)
            return False

    async def pin(self, message_id: int) -> None:
        try:
            await self._call("pinChatMessage", {"chat_id": self.chat_id, "message_id": message_id,
                                                "disable_notification": True})
        except TelegramError as e:
            log.warning("Épinglage impossible (le bot doit être admin) : %s", e)

    def enqueue(self, text: str, reply_markup: dict | None = None, key: str | None = None, kind: str = "",
                topic: str | None = None, reply_to: int | None = None, on_sent=None) -> bool:
        """Met une alerte en file. Renvoie False si elle a déjà été envoyée (clé connue)."""
        if key and self.db and self.db.alert_already_sent(key):
            return False
        if key and self.db:
            self.db.mark_alert_sent(key, kind)
        if key:
            self._sent[key] = asyncio.get_running_loop().create_future()
            while len(self._sent) > 500:
                self._sent.popitem(last=False)
        self.queue.put_nowait((text, reply_markup, topic, reply_to, on_sent, key, 0, kind in QUIET_KINDS))
        return True

    async def replace(self, key: str, text: str, reply_markup: dict | None = None, topic: str | None = None) -> None:
        """Remplace le message envoyé sous la clé `key` (alerte rapide complétée après analyse).

        Si le message n'a pas pu être envoyé ou modifié, le texte complet part en nouveau message.
        """
        fut = self._sent.get(key)
        mid = None
        if fut is not None:
            try:
                mid = await asyncio.wait_for(asyncio.shield(fut), 120)
            except asyncio.TimeoutError:
                mid = None
        if mid and await self.edit_now(mid, text, reply_markup):
            return
        self.queue.put_nowait((text, reply_markup, topic, None, None, None, 0, False))

    async def worker(self) -> None:
        while True:
            text, markup, topic, reply_to, on_sent, key, essais, quiet = await self.queue.get()
            try:
                res = await self.send_now(text, markup, topic, reply_to, quiet=quiet)
                mid = res["result"]["message_id"]
                self.sent_count += 1
                if key:
                    self.last_alert_ts = time.time()
                if key and key in self._sent and not self._sent[key].done():
                    self._sent[key].set_result(mid)
                if on_sent:
                    on_sent(mid)
            except Exception as e:  # une alerte ratée ne doit jamais arrêter le programme
                if essais < SEND_RETRIES and not isinstance(e, TelegramError):
                    log.warning("Alerte non envoyée (%s), nouvel essai dans %ss", e, RETRY_DELAY)
                    asyncio.get_running_loop().call_later(
                        RETRY_DELAY, self.queue.put_nowait,
                        (text, markup, topic, reply_to, on_sent, key, essais + 1, quiet))
                else:
                    log.error("Alerte définitivement non envoyée : %s", e)
                    if key:
                        if self.db:
                            self.db.forget_alert(key)
                        fut = self._sent.get(key)
                        if fut and not fut.done():
                            fut.set_result(None)
            await asyncio.sleep(MIN_INTERVAL)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


async def _test() -> int:
    cfg = cfgmod.load()
    db = DB(cfg.db_path)
    n = db.import_watchlist(cfg.watchlist_path)
    nb_wallets = len(db.active_wallets())
    tg = Telegram(cfg.telegram_bot_token, cfg.telegram_chat_id, db)
    try:
        await tg.setup_topics()
        texte = (
            "✅ <b>Memecoin Radar — test</b>\n"
            f"Connexion au bot OK · watchlist : <b>{nb_wallets}</b> wallets ({n} nouveaux importés).\n"
            f"RPC : {'Helius' if cfg.helius_api_key else 'RPC de secours (pas de clé Helius)'}\n"
            "<i>Programme d'alerte uniquement — aucune fonction de trading.</i>"
        )
        await tg.send_now(texte, buttons(("Solscan", "https://solscan.io"), ("pump.fun", "https://pump.fun")))
        if tg.forum:
            for key, (emoji, name, _c) in TOPICS.items():
                await tg.send_now(f"✅ Compartiment prêt : {emoji} <b>{esc(name)}</b>", topic=key)
            print(f"Groupe à sujets détecté : {len(TOPICS)} compartiments prêts. Regarde ton Telegram.")
        else:
            print("Message de test envoyé (conversation sans sujets). Regarde ton Telegram.")
        return 0
    finally:
        await tg.close()
        db.close()


async def _chat_ids() -> int:
    cfg = cfgmod.load()
    if not cfg.telegram_bot_token:
        print("Remplis d'abord TELEGRAM_BOT_TOKEN dans .env")
        return 1
    url = API.format(token=cfg.telegram_bot_token, method="getUpdates")
    async with aiohttp.ClientSession() as s, s.get(url) as r:
        data = await r.json(content_type=None)
    if not data.get("ok"):
        print("Erreur :", data.get("description"))
        return 1
    vus = {}
    for u in data.get("result", []):
        msg = u.get("message") or u.get("channel_post") or u.get("my_chat_member") or {}
        chat = msg.get("chat")
        if chat:
            genre = "groupe à sujets" if chat.get("is_forum") else chat.get("type", "")
            vus[chat["id"]] = f"{chat.get('title') or chat.get('first_name') or chat.get('username') or ''} — {genre}"
    if not vus:
        print("Aucun message reçu. Écris un message (au bot ou dans le groupe), puis relance cette commande.")
        return 1
    for cid, nom in vus.items():
        print(f"TELEGRAM_CHAT_ID={cid}    ({nom})")
    return 0


def main() -> int:
    cfgmod.setup_logging("telegram")
    if len(sys.argv) > 1 and sys.argv[1] == "chatid":
        return asyncio.run(_chat_ids())
    try:
        return asyncio.run(_test())
    except (ValueError, RuntimeError) as e:
        print("❌", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
