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
from .sources import pumpportal
from .sources.helius import LogsWatcher, RpcError, SolanaRPC, in_background
from .sources.x_watch import XWatcher, has_session
from .telegram import Telegram, esc

log = logging.getLogger("radar")


def A_short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}"

WORKERS = 3                  # transactions analysées en parallèle
MUTE_PER_MINUTE = 150        # au-delà, l'adresse est mise en sourdine (anti-flood : exchange suivi par erreur)
MUTE_S = 20 * 60             # courte : un bank qui finance 100 relais d'un coup ne doit pas masquer le lancement
BACKFILL_MAX_AGE_S = 30 * 60 # après une coupure, on rattrape les tx de moins de 30 min
DOWN_ALERT_S = 180           # coupure websocket / PumpPortal signalée sur Telegram après 3 min
DAILY_REPORT_HOUR = 9        # bilan quotidien (heure de Paris)


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
    tg = Telegram(cfg.telegram_bot_token, cfg.telegram_chat_id, db)
    queue: asyncio.Queue[str] = asyncio.Queue()
    rate: dict[str, deque] = defaultdict(deque)
    muted: dict[str, float] = {}
    stats = {"notifs": 0, "tx": 0, "alertes": 0}

    async with SolanaRPC(cfg.rpc_url) as rpc, aiohttp.ClientSession() as http:
        try:
            await tg.setup_topics()
        except Exception as e:
            log.error("Sujets Telegram non créés (%s) : tout arrivera dans la même conversation", e)
        try:
            await tg.setup_profile()
        except Exception as e:
            log.warning("Profil du bot (commandes, description) non mis à jour : %s", e)
        pipeline = Pipeline(cfg, db, rpc, http, tg)
        try:
            await pipeline.detect_mints()
        except RpcError as e:
            log.error("Helius ne répond pas correctement au démarrage (%s) : vérifie la clé avec configurer.bat", e)

        # Veille X + agenda
        agenda = Agenda(pipeline, tg)
        xwatcher = None
        if cfg.x_enabled:
            if has_session(cfg.x_profile_dir):
                xwatcher = XWatcher(cfg, agenda.on_tweets, lambda msg: tg.enqueue(msg, topic="system"))
                agenda.xw = xwatcher
            else:
                log.warning("Veille X désactivée : pas encore de session X (double-clique sur connexion_x.bat)")

        async def on_new_token(msg: dict) -> None:
            await pipeline.on_pumpportal_create(msg)
            await agenda.on_new_token(msg)

        async def on_signature(addr: str, sig: str, err) -> None:
            stats["notifs"] += 1
            if err is not None:
                return
            now = time.time()
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
            db.set_last_sig(addr, sig)
            if not pipeline.seen_signature(sig):
                queue.put_nowait(sig)

        async def backfill() -> None:
            """Après (re)connexion : rattrape les tx récentes manquées pendant la coupure."""
            n = 0
            for addr in list(watcher.addresses):
                try:
                    last = db.last_sig(addr)
                    sigs = await rpc.signatures(addr, until=last, limit=50) if last else await rpc.signatures(addr, limit=1)
                    if sigs:
                        db.set_last_sig(addr, sigs[0]["signature"])
                    if not last:
                        continue  # premier démarrage : on part de maintenant
                    for s in reversed(sigs):
                        if s.get("err") is None and time.time() - (s.get("blockTime") or 0) < BACKFILL_MAX_AGE_S:
                            if not pipeline.seen_signature(s["signature"]):
                                queue.put_nowait(s["signature"])
                                n += 1
                except Exception as e:
                    log.warning("Rattrapage impossible pour %s : %s", addr[:6], e)
            if n:
                log.info("Rattrapage : %d transaction(s) manquée(s) remises en file", n)

        watcher = LogsWatcher(cfg.ws_url, on_signature, backfill)
        pipeline.watcher = watcher
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
            while True:
                await asyncio.sleep(900)
                log.info("En vie : %d wallets suivis · %d notifications · %d tx analysées · %d alertes · file %d "
                         "· RPC temps réel %d / arrière-plan %d",
                         len(watcher.addresses), stats["notifs"], stats["tx"], stats["alertes"], queue.qsize(),
                         rpc.calls[0], rpc.calls[1])

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
                               "Reconnexion automatique en cours. Vérifie la connexion internet du PC.")
                    elif not down and signale[nom]:
                        signale[nom] = False
                        system(f"✅ {libelle} : reconnecté.")
                if rpc.auth_error and not signale["rpc"]:
                    signale["rpc"] = True
                    system(f"⛔ <b>Helius refuse les requêtes</b> ({esc(rpc.auth_error)})\n"
                           "Clé invalide ou crédits du mois épuisés : vérifie sur dashboard.helius.dev, "
                           "puis relance configurer.bat si la clé a changé.")
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
                       + f"\nVeille X : {'active' if xwatcher else 'inactive (connexion_x.bat)'}")

        async def discovery_loop() -> None:
            await asyncio.sleep(120)  # laisse le radar démarrer
            while True:
                try:
                    texte = discovery.report(await discovery.run_once(pipeline))
                    if texte:
                        tg.enqueue(texte, topic="devs")
                except Exception:
                    log.exception("Découverte automatique en échec")
                await asyncio.sleep(cfg.discovery_every_h * 3600)

        bot = Bot(cfg, db, tg, pipeline, agenda, watcher, xwatcher, stats)
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
            f"🐦 Veille X : active (rythme lent{pause})" if xwatcher else "⚠️ Veille X : inactive (connexion_x.bat)",
            f"🧭 Découverte auto de devs : toutes les {cfg.discovery_every_h} h" if cfg.discovery_enabled else None,
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
                 in_background(agenda.refresh_loop()), in_background(agenda.poll_dexscreener())]
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
