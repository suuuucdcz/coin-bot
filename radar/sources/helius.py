"""Client RPC Solana (Helius) + websocket temps réel.

- SolanaRPC : limite de débit avec priorité (le temps réel passe devant le traçage), nouvel essai
  automatique (backoff) ;
- LogsWatcher : un abonnement `logsSubscribe` par wallet suivi, reconnexion automatique.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import Counter, deque
from contextvars import ContextVar
from typing import Any, Awaitable, Callable

import aiohttp
import websockets

log = logging.getLogger("rpc")

LAMPORTS = 1_000_000_000

# Programmes utiles
SYSTEM_PROGRAM = "11111111111111111111111111111111"
COMPUTE_BUDGET = "ComputeBudget111111111111111111111111111111"
MEMO_PROGRAMS = {"MemoSq4gqABAXKd96qnXCCdrNmYAW4VkqVdPDTqbDbC4", "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"}
SIMPLE_TRANSFER_PROGRAMS = {SYSTEM_PROGRAM, COMPUTE_BUDGET} | MEMO_PROGRAMS
TOKEN_PROGRAMS = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",   # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # comptes de token associés
}
PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
AMM_PROGRAMS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "Raydium CPMM",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj": "Raydium LaunchLab",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora",
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG": "Meteora DAMM v2",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca",
}
WSOL = "So11111111111111111111111111111111111111112"
IGNORED_MINTS = {
    WSOL,
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
}


class RpcError(RuntimeError):
    pass


# Priorité des appels RPC. Le temps réel (alertes) est prioritaire ; le traçage, la chasse au dev,
# l'agenda et la découverte passent en « arrière-plan » et cèdent la place dès qu'une alerte attend.
URGENT, BACKGROUND = 0, 1
_priority: ContextVar[int] = ContextVar("rpc_priority", default=URGENT)


async def in_background(coro):
    """Exécute une coroutine en priorité basse (ses appels RPC passent après le temps réel)."""
    token = _priority.set(BACKGROUND)
    try:
        return await coro
    finally:
        _priority.reset(token)


class SolanaRPC:
    def __init__(self, url: str, rps: float = 8.0):
        # Plan gratuit Helius : ~10 requêtes/s. On reste un peu en dessous.
        self.url = url
        self.min_interval = 1.0 / rps
        self._lock = asyncio.Lock()
        self._last = 0.0
        self._session: aiohttp.ClientSession | None = None
        self._id = 0
        self._urgent_waiting = 0
        self._calm = asyncio.Event()
        self._calm.set()
        # Santé (lue par main.py pour prévenir sur Telegram)
        self.auth_error: str | None = None      # clé refusée / quota épuisé
        self.failures: deque[float] = deque(maxlen=200)
        self.calls = {URGENT: 0, BACKGROUND: 0}
        self.by_method: Counter[str] = Counter()   # requêtes envoyées par méthode (= crédits Helius)

    async def __aenter__(self) -> "SolanaRPC":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _throttle(self) -> None:
        urgent = _priority.get() == URGENT
        self.calls[URGENT if urgent else BACKGROUND] += 1
        if urgent:
            self._urgent_waiting += 1
            self._calm.clear()
        try:
            while True:
                if not urgent:
                    await self._calm.wait()
                async with self._lock:
                    if not urgent and self._urgent_waiting:
                        continue  # une alerte est arrivée entre-temps : elle passe devant
                    attente = self._last + self.min_interval - time.monotonic()
                    if attente > 0:
                        await asyncio.sleep(attente)
                    self._last = time.monotonic()
                    return
        finally:
            if urgent:
                self._urgent_waiting -= 1
                if not self._urgent_waiting:
                    self._calm.set()

    def recent_failures(self, window_s: float = 600) -> int:
        now = time.time()
        return sum(1 for t in self.failures if now - t < window_s)

    async def call(self, method: str, params: list[Any]) -> Any:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        for tentative in range(7):
            await self._throttle()
            self.by_method[method if _priority.get() == URGENT else method + " (fond)"] += 1
            self._id += 1
            body = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}
            try:
                async with self._session.post(self.url, json=body) as r:
                    if r.status == 429 or r.status >= 500:
                        raise _Retry(f"HTTP {r.status}")
                    if r.status in (401, 403):
                        self.auth_error = f"HTTP {r.status} sur {method}"
                        self.failures.append(time.time())
                        raise RpcError(f"HTTP {r.status} : clé API refusée ou méthode interdite ({method})")
                    data = await r.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, _Retry) as e:
                attente = min(30, 0.5 * 2 ** tentative)
                log.debug("%s : %s, nouvel essai dans %.1fs", method, e, attente)
                await asyncio.sleep(attente)
                continue
            if "error" in data:
                err = data["error"]
                code = err.get("code")
                # -32429 / -32005 / -32004 : limite de débit ou nœud en retard -> on réessaie
                if code in (-32429, -32005, -32004, -32007, 429):
                    await asyncio.sleep(min(30, 0.5 * 2 ** tentative))
                    continue
                raise RpcError(f"{method} : {err.get('message')} (code {code})")
            self.auth_error = None
            return data.get("result")
        self.failures.append(time.time())
        raise RpcError(f"{method} : échec après plusieurs essais (rate limit ?)")

    # --- méthodes pratiques --------------------------------------------------
    async def signatures(self, address: str, before: str | None = None, until: str | None = None,
                         limit: int = 1000) -> list[dict]:
        opts: dict[str, Any] = {"limit": limit}
        if before:
            opts["before"] = before
        if until:
            opts["until"] = until
        return await self.call("getSignaturesForAddress", [address, opts]) or []

    async def all_signatures(self, address: str, max_pages: int = 10) -> tuple[list[dict], bool]:
        """Toutes les signatures (de la plus récente à la plus ancienne).

        Renvoie (signatures, tronqué). tronqué=True si l'adresse a plus de max_pages×1000 tx.
        """
        out: list[dict] = []
        before = None
        for _ in range(max_pages):
            page = await self.signatures(address, before=before)
            out.extend(page)
            if len(page) < 1000:
                return out, False
            before = page[-1]["signature"]
        return out, True

    async def transaction(self, signature: str) -> dict | None:
        # maxSupportedTransactionVersion = 1 (comme le dit CLAUDE.md) : des transactions v1 circulent
        # et sont refusées avec 0 (« Transaction version (1) is not supported »).
        return await self.call("getTransaction", [signature, {
            "encoding": "jsonParsed", "maxSupportedTransactionVersion": 1, "commitment": "confirmed"}])

    async def balance(self, address: str) -> float:
        res = await self.call("getBalance", [address])
        return (res or {}).get("value", 0) / LAMPORTS

    async def account_info(self, address: str) -> dict | None:
        res = await self.call("getAccountInfo", [address, {"encoding": "jsonParsed"}])
        return (res or {}).get("value")

    async def token_largest_accounts(self, mint: str) -> list[dict]:
        res = await self.call("getTokenLargestAccounts", [mint])
        return (res or {}).get("value", [])

    async def transaction_retry(self, signature: str, tries: int = 6) -> dict | None:
        """La tx peut ne pas être encore lisible juste après la notification : on réessaie."""
        for i in range(tries):
            tx = await self.transaction(signature)
            if tx:
                return tx
            await asyncio.sleep(0.5 + i * 0.5)
        return None

    async def mint_info(self, mint: str) -> dict | None:
        """supply, decimals, mintAuthority, freezeAuthority d'un token (None si pas un mint)."""
        v = await self.account_info(mint)
        try:
            info = v["data"]["parsed"]["info"]
            if v["data"]["parsed"]["type"] != "mint":
                return None
            info["program"] = v.get("owner")
            return info
        except (TypeError, KeyError):
            return None

    async def asset(self, mint: str) -> dict | None:
        """Métadonnées via l'API DAS de Helius (nom, ticker, lien JSON)."""
        try:
            return await self.call("getAsset", {"id": mint})
        except RpcError:
            return None


class _Retry(Exception):
    pass


OnSignature = Callable[[str, str, Any, list], Awaitable[None]]

# Ouverture d'un pool (Raydium « initialize2 » / CPMM « Initialize », PumpSwap « CreatePool », Meteora…)
_POOL_INIT_RE = re.compile(r"Instruction: (Initialize|InitializePool\w*|CreatePool\w*|InitializeLbPair\w*|"
                           r"InitializeCustomizablePermissionlessLbPair\w*)$|initialize2: ", re.I)


def notable_logs(logs: list[str] | None, strict: bool = False) -> bool:
    """Pour un wallet très actif : la transaction mérite-t-elle un getTransaction (1 crédit Helius) ?

    Toujours : création d'un token (InitializeMint), ouverture d'un pool, logs incomplets.
    Sauf en mode strict : simple virement de SOL (funding, retour de profits).
    Jamais : swaps, transferts de tokens, collecte de frais, NFT… (vu en vrai : 4 wallets à 80-200 tx/h
    mangeaient 80 % du quota gratuit sans jamais donner d'alerte).
    """
    if not logs or any("Log truncated" in l for l in logs):
        return True
    if any("Instruction: InitializeMint" in l for l in logs):
        return True
    if any(_POOL_INIT_RE.search(l) for l in logs):
        return True
    if strict:
        return False
    progs = {l.split()[1] for l in logs if l.startswith("Program ") and " invoke [" in l}
    return bool(progs) and progs <= SIMPLE_TRANSFER_PROGRAMS


class LogsWatcher:
    """Websocket Helius : notifie chaque transaction qui mentionne un wallet suivi."""

    def __init__(self, ws_url: str, on_signature: OnSignature,
                 on_connect: Callable[[], Awaitable[None]] | None = None):
        self.ws_url = ws_url
        self.on_signature = on_signature
        self.on_connect = on_connect
        self.addresses: set[str] = set()
        self._subs: dict[int, str] = {}       # id d'abonnement -> adresse
        self._by_addr: dict[str, int] = {}
        self._pending: dict[int, str] = {}    # id de requête -> adresse
        self._req = 0
        self._ws = None
        self.connected = asyncio.Event()
        self.down_since: float | None = time.time()   # None = connecté
        self.connections = 0                  # connexions réussies depuis le démarrage
        self.last_gap: float | None = None    # durée de la dernière coupure (None = premier démarrage)
        self.last_notification = 0.0

    async def add(self, address: str) -> None:
        if address in self.addresses:
            return
        self.addresses.add(address)
        if self._ws is not None:
            try:
                await self._subscribe(address)
            except Exception as e:  # connexion en train de tomber : l'abonnement sera refait à la reconnexion
                log.debug("Abonnement différé pour %s : %s", address[:6], e)

    async def remove(self, address: str) -> None:
        self.addresses.discard(address)
        sub = self._by_addr.pop(address, None)
        if sub is not None and self._ws is not None:
            self._subs.pop(sub, None)
            self._req += 1
            try:
                await self._ws.send(json.dumps({"jsonrpc": "2.0", "id": self._req,
                                                "method": "logsUnsubscribe", "params": [sub]}))
            except Exception as e:
                log.debug("Désabonnement de %s impossible : %s", address[:6], e)

    async def _subscribe(self, address: str) -> None:
        self._req += 1
        self._pending[self._req] = address
        await self._ws.send(json.dumps({
            "jsonrpc": "2.0", "id": self._req, "method": "logsSubscribe",
            "params": [{"mentions": [address]}, {"commitment": "confirmed"}],
        }))

    async def run(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=30,
                                              max_size=2 ** 23, open_timeout=20) as ws:
                    self._ws = ws
                    self._subs.clear(); self._by_addr.clear(); self._pending.clear()
                    for a in list(self.addresses):
                        await self._subscribe(a)
                    log.info("Websocket connecté, %d abonnements demandés", len(self.addresses))
                    backoff = 1
                    self.last_gap = time.time() - self.down_since if self.connections and self.down_since else None
                    self.connections += 1
                    self.down_since = None
                    self.connected.set()
                    if self.on_connect:
                        asyncio.create_task(self.on_connect())
                    async for raw in ws:
                        await self._handle(raw)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # coupure réseau, refus, fermeture côté serveur…
                log.warning("Websocket coupé (%s : %s), reconnexion dans %ss", type(e).__name__, e, backoff)
            finally:
                self._ws = None
                self.connected.clear()
                if self.down_since is None:
                    self.down_since = time.time()
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)

    async def _handle(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        if "id" in msg and msg["id"] in self._pending:
            addr = self._pending.pop(msg["id"])
            if "result" in msg:
                self._subs[msg["result"]] = addr
                self._by_addr[addr] = msg["result"]
            else:
                log.error("Abonnement refusé pour %s : %s", addr, msg.get("error"))
            return
        if msg.get("method") != "logsNotification":
            return
        params = msg.get("params", {})
        addr = self._subs.get(params.get("subscription"))
        value = params.get("result", {}).get("value", {})
        self.last_notification = time.time()
        if addr and value.get("signature"):
            try:
                await self.on_signature(addr, value["signature"], value.get("err"), value.get("logs") or [])
            except Exception:
                log.exception("Erreur dans le traitement d'une notification")


def account_keys(tx: dict) -> list[str]:
    msg = tx["transaction"]["message"]
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in msg["accountKeys"]]
    meta = tx.get("meta") or {}
    n = len(meta.get("preBalances", []))
    if len(keys) < n:  # tables d'adresses (v0) pas incluses : on les ajoute
        loaded = meta.get("loadedAddresses") or {}
        keys += loaded.get("writable", []) + loaded.get("readonly", [])
    return keys


def sol_deltas(tx: dict) -> dict[str, float]:
    """Variation du solde SOL de chaque compte dans la transaction (en SOL)."""
    meta = tx.get("meta") or {}
    pre, post = meta.get("preBalances", []), meta.get("postBalances", [])
    out: dict[str, float] = {}
    for key, a, b in zip(account_keys(tx), pre, post):
        if a != b:
            out[key] = out.get(key, 0.0) + (b - a) / LAMPORTS
    return out


def fee_payer(tx: dict) -> str:
    return account_keys(tx)[0]
