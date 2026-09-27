"""Memecoin Radar — programme principal (surveillance temps réel de la watchlist).

Lancement :  python -m radar.main
Arrêt     :  Ctrl+C
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time
from collections import defaultdict, deque

import aiohttp

from datetime import datetime

from . import config as cfgmod
from . import discovery
from .agenda import PARIS, Agenda
from .bot import MENU, Bot
from .db import DB
from .pipeline import Pipeline
from .results import Results
from .smart import SmartMoney
from .sources import pumpportal
from .sources.helius import LogsWatcher, RpcError, SolanaRPC, in_background, notable_logs
from .sources.x_watch import XWatcher, has_session
from .telegram import Telegram, esc

log = logging.getLogger("radar")


def A_short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}"

WORKERS = 3                  # transactions analysées en parallèle
MUTE_PER_MINUTE = 150        # au-delà, l'adresse est mise en sourdine (anti-flood : exchange suivi par erreur)
MUTE_S = 20 * 60             # courte : un bank qui finance 100 relais d'un coup ne doit pas masquer le lancement
NOISY_PER_HOUR = 40          # au-delà : wallet « bavard », seules les tx notables sont téléchargées
SERVICE_PER_HOUR = 200       # au-delà : même les virements de SOL sont ignorés (distributeur, bot)
NOISY_KEEP_S = 24 * 3600     # un wallet bavard le reste 24 h (pas 40 tx gaspillées à chaque heure)
HELIUS_FREE_CREDITS = 1_000_000
BACKFILL_MAX_AGE_S = 30 * 60
SHORT_GAP_S = 10 * 60        # coupure courte : on ne rattrape pas les wallets qui dorment depuis 3 jours
DORMANT_S = 3 * 86400 # après une coupure, on rattrape les tx de moins de 30 min
DOWN_ALERT_S = 180           # coupure websocket / PumpPortal signalée sur Telegram après 3 min
DAILY_REPORT_HOUR = 9        # bilan quotidien (heure de Paris)
GUIDE_VERSION = "2"
GUIDE = (
    "📖 <b>MODE D'EMPLOI — À NE PAS RATER</b>\n"
    "Ici, tu reçois <b>uniquement</b> les alertes vérifiées, avec le son. Tout le reste (agenda X, clusters, "
    "fiches dev, arnaques) arrive ailleurs, sans son.\n\n"
    "🚨 <b>UN DEV SUIVI CRÉE UN TOKEN</b> — un wallet de dev surveillé vient de lancer un token.\n"
    "🚨 <b>LE DEV / LE CLUSTER ENTRE</b> — le dev ou plusieurs wallets d'un même groupe achètent un token "
    "tout jeune.\n"
    "🚨 <b>TRADING OUVERT</b> — un contrat annoncé à l'avance vient de recevoir sa liquidité.\n"
    "🚨 <b>$XXX ANNONCÉ SUR X EST LANCÉ</b> — le coin annoncé, avec une preuve forte (créé par le dev "
    "repéré, ou CA publié par le compte officiel).\n\n"
    "<b>Lire une alerte</b> : verdict (🟢 rien de suspect · 🟡 points à vérifier), 💡 pourquoi, CA, âge, "
    "market cap, dev, lien X.\n"
    "<b>Boutons</b> : 📋 Copier le CA (à coller dans ta plateforme) · 📈 GMGN · 📊 DexScreener · "
    "🔎 Fiche complète · 🧬 Tracer le dev.\n\n"
    "<b>Jamais ici</b> : les tokens ⛔ (opérateur de rugs) ou 🟠 (signal grave). Ils restent dans le groupe.\n"
    "⚠️ C'est une alerte, pas un conseil : le radar ne garantit rien, tu décides et tu achètes toi-même."
)


async def amain() -> int:
    cfg = cfgmod.load()
    manque = [n for n, v in (("HELIUS_API_KEY", cfg.helius_api_key), ("TELEGRAM_BOT_TOKEN", cfg.telegram_bot_token),
                             ("TELEGRAM_CHAT_ID", cfg.telegram_chat_id)) if not v]
    if manque:
        print(f"❌ Configuration incomplète ({', '.join(manque)}). Double-clique sur configurer.bat.")
        return 1
    db = DB(cfg.db_path)
    db.import_watchlist(cfg.watchlist_path)
    db.import_labels(cfg.labels_path)
    for w in db.import_warnings:
        log.warning("Données d'entrée : %s", w)
    tg = Telegram(cfg.telegram_bot_token, cfg.telegram_chat_id, db, cfg.telegram_top_chat_id)
    queue: asyncio.Queue[str] = asyncio.Queue()
    rate: dict[str, deque] = defaultdict(deque)
    muted: dict[str, float] = {}
    stats = {"notifs": 0, "tx": 0, "alertes": 0}

    async with SolanaRPC(cfg.rpc_url) as rpc, aiohttp.ClientSession() as http:
        try:
            await tg.setup_topics()
            await tg.ensure_headers()
        except Exception as e:
            log.error("Sujets Telegram non créés (%s) : tout arrivera dans la même conversation", e)
        try:
            await tg.setup_profile()
        except Exception as e:
            log.warning("Profil du bot (commandes, description) non mis à jour : %s", e)
        try:
            tg.guide = GUIDE
            if not tg.top_in_group:   # la section ‼️ du groupe n'existe pas encore (bot pas admin) : en privé
                await tg.post_guide(GUIDE, GUIDE_VERSION)
        except Exception as e:
            log.warning("Mode d'emploi non envoyé en privé (écris /start au bot) : %s", e)
        pipeline = Pipeline(cfg, db, rpc, http, tg)
        try:
            await pipeline.detect_mints()
        except RpcError as e:
            log.error("Helius ne répond pas correctement au démarrage (%s) : vérifie la clé avec configurer.bat", e)

        # Veille X + agenda
        pipeline.results = Results(db, http)   # suivi de chaque alerte pendant 24 h (📈)
        agenda = Agenda(pipeline, tg)
        xwatcher = None
        if cfg.x_enabled:
            if has_session(cfg.x_profile_dir):
                xwatcher = XWatcher(cfg, agenda.on_tweets, lambda msg: tg.enqueue(msg, topic="system"))
                agenda.xw = xwatcher
            else:
                log.warning("Veille X désactivée : pas encore de session X (%s)", cfgmod.AIDE_X)

        async def on_new_token(msg: dict) -> None:
            await pipeline.on_pumpportal_create(msg)
            await agenda.on_new_token(msg)

        heure: dict[str, deque] = defaultdict(deque)
        bavards: dict[str, float] = {}

        async def on_signature(addr: str, sig: str, err, logs: list | None = None) -> None:
            stats["notifs"] += 1
            if err is not None:
                return
            now = time.time()
            dh = heure[addr]
            dh.append(now)
            while dh and now - dh[0] > 3600:
                dh.popleft()
            if len(dh) > NOISY_PER_HOUR and bavards.get(addr, 0) < now:
                log.info("%s très actif (%d tx/h) : seules ses créations, pools et virements de SOL sont analysés",
                         pipeline.label(addr) or addr[:6], len(dh))
                db.put(f"noisy:{addr}", int(now))
            if len(dh) > NOISY_PER_HOUR:
                bavards[addr] = now + NOISY_KEEP_S
            smart = len(dh) <= SERVICE_PER_HOUR and pipeline.is_smart(addr)   # ses achats SONT le signal
            if bavards.get(addr, 0) > now and not smart and not notable_logs(logs, strict=len(dh) > SERVICE_PER_HOUR):
                stats["filtrées"] = stats.get("filtrées", 0) + 1
                try:
                    db.set_last_sig(addr, sig)
                except Exception:
                    pass
                return
            if muted.get(addr, 0) > now:
                return
            dq = rate[addr]
            dq.append(now)
            while dq and now - dq[0] > 60:
                dq.popleft()
            if len(dq) > MUTE_PER_MINUTE:
                muted[addr] = now + MUTE_S
                dq.clear()
                tg.enqueue(f"🔇 {esc(pipeline.label(addr) or addr)} est trop actif (> {MUTE_PER_MINUTE} tx/min) : "
                           f"mis en sourdine {MUTE_S // 60} min.", kind="mute", topic="system")
                return
            try:
                db.set_last_sig(addr, sig)
            except Exception as e:  # base momentanément verrouillée : la transaction est quand même analysée
                log.warning("Point de reprise non enregistré pour %s : %s", addr[:6], e)
            if not pipeline.seen_signature(sig):
                queue.put_nowait(sig)

        async def backfill() -> None:
            """Après (re)connexion : rattrape les tx récentes manquées pendant la coupure."""
            n = sautes = 0
            courte = watcher.last_gap is not None and watcher.last_gap < SHORT_GAP_S
            for addr in list(watcher.addresses):
                if courte and time.time() - db.last_activity(addr) > DORMANT_S:
                    # Vu en vrai : ~10 micro-coupures internet par soirée x 160 wallets = 1 600 crédits Helius
                    # pour rien. Un wallet endormi depuis 3 jours n'a presque aucune chance d'agir pendant la coupure.
                    sautes += 1
                    continue
                try:
                    last = db.last_sig(addr)
                    sigs = await rpc.signatures(addr, until=last, limit=50) if last else await rpc.signatures(addr, limit=1)
                    if sigs:
                        db.set_last_sig(addr, sigs[0]["signature"], sigs[0].get("blockTime"))
                    if not last or bavards.get(addr, 0) > time.time():
                        continue  # premier démarrage (on part de maintenant) ou wallet très actif (pas de logs à trier)
                    for s in reversed(sigs):
                        if s.get("err") is None and time.time() - (s.get("blockTime") or 0) < BACKFILL_MAX_AGE_S:
                            if not pipeline.seen_signature(s["signature"]):
                                queue.put_nowait(s["signature"])
                                n += 1
                except Exception as e:
                    log.warning("Rattrapage impossible pour %s : %s", addr[:6], e)
            if n or sautes:
                log.info("Rattrapage : %d transaction(s) manquée(s) remises en file%s", n,
                         f" ({sautes} wallets endormis non interrogés, coupure de {int(watcher.last_gap)} s)"
                         if sautes else "")

        # Wallets déjà repérés bavards dans les dernières 24 h (sinon 40 tx gaspillées après chaque redémarrage)
        for cle, t in db.settings_like("noisy:"):
            if time.time() - float(t) < NOISY_KEEP_S:
                bavards[cle.split(":", 1)[1]] = float(t) + NOISY_KEEP_S
        watcher = LogsWatcher(cfg.ws_url, on_signature, backfill)
        pipeline.watcher = watcher
        await pipeline.purge_services()
        watcher.addresses |= pipeline.watched

        async def worker() -> None:
            while True:
                sig = await queue.get()
                try:
                    for alert in await pipeline.handle_signature(sig):
                        stats["alertes"] += 1
                        pipeline.emit(alert)
                    stats["tx"] += 1
                except Exception:
                    log.exception("Erreur sur la transaction %s", sig)

        async def heartbeat() -> None:
            deja = 0
            tours = 0
            prevenu = ""
            while True:
                await asyncio.sleep(900)
                tours += 1
                total = sum(rpc.by_method.values())
                mois = time.strftime("%Y-%m")
                try:
                    credits = int(db.get(f"rpc_month:{mois}") or 0) + total - deja
                    db.put(f"rpc_month:{mois}", credits)
                    deja = total
                except Exception as e:
                    log.warning("Compteur Helius non enregistré : %s", e)
                    credits = int(db.get(f"rpc_month:{mois}") or 0)
                log.info("En vie : %d wallets suivis · %d notifications · %d tx analysées · %d filtrées · %d alertes "
                         "· file %d · RPC temps réel %d / arrière-plan %d · Helius ce mois %d crédits",
                         len(watcher.addresses), stats["notifs"], stats["tx"], stats.get("filtrées", 0),
                         stats["alertes"], queue.qsize(), rpc.calls[0], rpc.calls[1], credits)
                if tours % 4 == 0:
                    log.info("RPC par méthode depuis le démarrage : %s",
                             ", ".join(f"{m} {n}" for m, n in rpc.by_method.most_common(8)))
                # Projection sur 30 jours à partir du début réel du comptage (pas du 1er du mois)
                debut = float(db.get(f"rpc_since:{mois}") or 0)
                if not debut:
                    debut = time.time() - 900
                    db.put(f"rpc_since:{mois}", int(debut))
                jours = max(0.25, (time.time() - debut) / 86400)
                projection = credits / jours * 30
                if projection > 0.9 * HELIUS_FREE_CREDITS and jours >= 1 and prevenu != mois:
                    prevenu = mois
                    system(f"⚠️ <b>Quota Helius</b> : {credits:,} crédits utilisés ce mois, projection "
                           f"{int(projection):,} pour {HELIUS_FREE_CREDITS:,} gratuits.\n"
                           "Réduis la watchlist (WATCH_MAX) ou retire des wallets très actifs.".replace(",", " "))

        def system(text: str) -> None:
            tg.enqueue(text, topic="system")

        async def health() -> None:
            """Prévient sur Telegram si une source tombe (et quand elle revient)."""
            signale: dict[str, bool] = {"ws": False, "pp": False, "rpc": False}
            while True:
                await asyncio.sleep(60)
                now = time.time()
                for nom, down, libelle in (("ws", watcher.down_since, "Websocket Helius (wallets suivis)"),
                                           ("pp", pumpportal.state["down_since"], "PumpPortal (nouveaux tokens)")):
                    if down and now - down > DOWN_ALERT_S and not signale[nom]:
                        signale[nom] = True
                        system(f"🔌 <b>{libelle} coupé depuis {int((now - down) // 60)} min</b>\n"
                               f"Reconnexion automatique en cours. Vérifie la connexion internet {cfgmod.MACHINE}.")
                    elif not down and signale[nom]:
                        signale[nom] = False
                        system(f"✅ {libelle} : reconnecté.")
                if rpc.auth_error and not signale["rpc"]:
                    signale["rpc"] = True
                    system(f"⛔ <b>Helius refuse les requêtes</b> ({esc(rpc.auth_error)})\n"
                           "Clé invalide ou crédits du mois épuisés : vérifie sur dashboard.helius.dev, "
                           f"puis {cfgmod.AIDE_CLE}.")
                elif rpc.recent_failures() >= 20 and not signale["rpc"]:
                    signale["rpc"] = True
                    system("⚠️ <b>Helius sature</b> (limite de débit atteinte plusieurs fois en 10 min). "
                           "Des alertes peuvent arriver en retard. Baisse WATCH_MAX si ça se répète.")
                elif not rpc.auth_error and rpc.recent_failures() == 0 and signale["rpc"]:
                    signale["rpc"] = False
                    system("✅ Helius répond normalement.")

        async def maintenance() -> None:
            """Chaque heure : mémoire. Chaque jour : purge des wallets inactifs + bilan."""
            dernier_bilan = db.get("daily_report")
            while True:
                await asyncio.sleep(3600)
                pipeline.cleanup()
                await pipeline.purge_services()
                now = datetime.now(PARIS)
                jour = now.strftime("%Y-%m-%d")
                if now.hour < DAILY_REPORT_HOUR or dernier_bilan == jour:
                    continue
                dernier_bilan = jour
                db.put("daily_report", jour)
                inactifs = db.stale_wallets(cfg.watch_stale_days)
                if inactifs:
                    await pipeline.unwatch(inactifs)
                    log.info("Purge : %d wallet(s) inactif(s) mis en veille", len(inactifs))
                par_type = db.alerts_since(int(time.time()) - 86400)
                system("📊 <b>Bilan des dernières 24 h</b>\n"
                       f"Wallets suivis : {len(watcher.addresses)} / {cfg.watch_max}"
                       + (f" ({len(inactifs)} inactifs depuis {cfg.watch_stale_days} j mis en veille)" if inactifs else "")
                       + f"\nAlertes : {sum(par_type.values())}"
                       + (" (" + ", ".join(f"{k} {v}" for k, v in sorted(par_type.items())) + ")" if par_type else "")
                       + f"\nNouveaux tokens pump.fun vus : {pumpportal.state['tokens']}"
                       + f"\nVeille X : {'active' if xwatcher else 'inactive (' + cfgmod.AIDE_X + ')'}")
                tg.enqueue(pipeline.results.report(), kind="resultats", topic="resultats")

        async def discovery_loop() -> None:
            await asyncio.sleep(120)  # laisse le radar démarrer
            while True:
                # Heure du dernier passage gardée en base : sinon chaque redémarrage relançait une découverte
                # complète (des centaines de crédits Helius) au lieu d'attendre la prochaine échéance.
                attente = float(db.get("discovery_last") or 0) + cfg.discovery_every_h * 3600 - time.time()
                if attente > 0:
                    await asyncio.sleep(attente)
                    continue
                db.put("discovery_last", int(time.time()))
                try:
                    texte = discovery.report(await discovery.run_once(pipeline))
                    if texte:
                        tg.enqueue(texte, topic="devs")
                except Exception:
                    log.exception("Découverte automatique en échec")
                await asyncio.sleep(cfg.discovery_every_h * 3600)

        bot = Bot(cfg, db, tg, pipeline, agenda, watcher, xwatcher, stats)
        pipeline._spawn(agenda.llm.warm_up())   # IA préparée en arrière-plan (modèle chargé ou clé vérifiée)
        ignores = len(db.active_wallets()) - len(pipeline.watched)
        contrats = ", ".join(esc(pipeline.label(m) or A_short(m)) for m in pipeline.mints)
        pause = f", pause {cfg.x_quiet_hours[0]} h-{cfg.x_quiet_hours[1]} h" if cfg.x_quiet_hours else ""
        tg.enqueue("\n".join(filter(None, [
            "🛰 <b>MEMECOIN RADAR EN LIGNE</b>",
            "───────────────",
            f"👛 <b>{len(pipeline.watched)}</b> wallets suivis en temps réel"
            + (f" <i>({ignores} exchange(s) ignoré(s))</i>" if ignores else ""),
            f"📜 En attente de lancement : {contrats}" if pipeline.mints else None,
            "🟣 Nouveaux tokens pump.fun : PumpPortal",
            f"🐦 Veille X : active (rythme lent{pause})" if xwatcher else f"⚠️ Veille X : inactive ({cfgmod.AIDE_X})",
            f"🧭 Découverte auto de devs : toutes les {cfg.discovery_every_h} h" if cfg.discovery_enabled else None,
            (f"🧠 IA pour lire les tweets : {esc(agenda.llm.label)}" if agenda.llm.enabled_cfg
             else "🧠 IA pour lire les tweets : désactivée (règles strictes seules)"),
            "🤖 IA Jev : active (avis en plus des règles)" if cfg.typesafe_api_key else None,
            "───────────────",
            "<i>Tape /aide pour les commandes · colle un CA pour sa fiche.</i>",
        ])), MENU, kind="system", topic="system")
        if db.import_warnings:
            system("🧹 <b>Données d'entrée à corriger</b>\n" + "\n".join(f"• {esc(w)}" for w in db.import_warnings[:15]))
        log.info("Démarrage : %d adresses suivies", len(pipeline.watched))
        # Temps réel en priorité RPC haute ; agenda, veille X et découverte en arrière-plan
        tasks = [watcher.run(), tg.worker(), tg.watch_topics(), heartbeat(), health(), maintenance(),
                 bot.run(), bot.dashboard(),
                 *[worker() for _ in range(WORKERS)], pumpportal.run(on_new_token),
                 in_background(agenda.refresh_loop()), in_background(agenda.poll_dexscreener()),
                 pipeline.results.loop(), in_background(SmartMoney(pipeline).loop())]
        if xwatcher:
            tasks.append(in_background(xwatcher.run()))
        if cfg.discovery_enabled:
            tasks.append(in_background(discovery_loop()))
        try:
            await asyncio.gather(*tasks)
        finally:
            await tg.close()
            db.close()
    return 0


def main() -> int:
    cfgmod.setup_logging("radar")
    try:
        return asyncio.run(amain())
    except KeyboardInterrupt:
        print("\nArrêt demandé (Ctrl+C). À bientôt.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
