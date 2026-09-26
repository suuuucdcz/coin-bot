"""PumpPortal : flux temps réel des nouveaux tokens pump.fun (gratuit, sans clé).

Doc vérifiée le 26/09/2026 : wss://pumpportal.fun/api/data, méthode `subscribeNewToken`.
Règle : UNE seule connexion à la fois (sinon ban d'une heure). `subscribeAccountTrade` est
payant (clé + wallet financé) : on ne l'utilise pas, Helius couvre déjà les trades des wallets suivis.

Champs d'un message : signature, mint, traderPublicKey (créateur), txType="create", initialBuy,
solAmount, marketCapSol, name, symbol, uri, pool.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable

import websockets

log = logging.getLogger("pumpportal")

URL = "wss://pumpportal.fun/api/data"

# Santé (lue par main.py) : None = connecté, sinon heure de la coupure
state: dict = {"down_since": time.time(), "tokens": 0}


async def run(on_new_token: Callable[[dict], Awaitable[None]]) -> None:
    backoff = 2
    while True:
        try:
            async with websockets.connect(URL, ping_interval=20, ping_timeout=30, open_timeout=20) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("PumpPortal connecté (nouveaux tokens pump.fun)")
                backoff = 2
                state["down_since"] = None
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    if msg.get("txType") == "create" and msg.get("mint"):
                        state["tokens"] += 1
                        try:
                            await on_new_token(msg)
                        except Exception:
                            log.exception("Erreur sur un nouveau token PumpPortal")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("PumpPortal coupé (%s : %s), reconnexion dans %ss", type(e).__name__, e, backoff)
        if state["down_since"] is None:
            state["down_since"] = time.time()
        await asyncio.sleep(backoff)
        backoff = min(120, backoff * 2)  # jamais de reconnexions en rafale (risque de ban)
