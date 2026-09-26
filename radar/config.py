"""Lecture de la configuration (.env) et mise en place des journaux."""
from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"

load_dotenv(ROOT / ".env")


def _env(name: str) -> str:
    """Valeur brute sans commentaire de fin de ligne (« 180   # secondes » -> « 180 »)."""
    return os.getenv(name, "").split("#")[0].strip()


def _int(name: str, default: int) -> int:
    try:
        return int(_env(name) or default)
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float((_env(name) or str(default)).replace(",", "."))
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").split("#")[0].strip().lower()
    return default if not v else v not in ("0", "false", "non", "no")


def _hours(name: str, default: str) -> tuple[int, int] | None:
    """« 3-8 » -> (3, 8) : plage horaire (heure de Paris). Vide ou « 0 » -> None (pas de pause)."""
    v = (os.getenv(name) if os.getenv(name) is not None else default).split("#")[0].strip()
    try:
        a, b = (int(x) for x in v.split("-"))
        return (a % 24, b % 24) if a != b else None
    except ValueError:
        return None


def _handles(v: str) -> list[str]:
    """« @zenkaixbt, https://x.com/1dev_zen » -> ["zenkaixbt", "1dev_zen"] (entrées invalides ignorées)."""
    out = []
    for a in v.replace(";", ",").split(","):
        a = re.sub(r"^(?:https?://)?(?:www\.)?(?:x|twitter)\.com/", "", a.strip()).strip("@/ ")
        if re.fullmatch(r"[A-Za-z0-9_]{1,15}", a) and a.lower() not in {h.lower() for h in out}:
            out.append(a)
    return out


@dataclass(frozen=True)
class Config:
    helius_api_key: str
    solana_rpc_url_fallback: str
    telegram_bot_token: str
    telegram_chat_id: str
    x_profile_dir: Path
    x_accounts: list[str]
    x_poll_seconds: int
    x_headless: bool
    x_browser: str
    x_enabled: bool
    trace_max_hops: int
    hot_wallet_tx_threshold: int
    db_path: Path
    watchlist_path: Path
    labels_path: Path
    funding_min_sol: float
    profit_min_sol: float
    rug_groups: set[str]
    watch_max: int
    watch_stale_days: int
    x_quiet_hours: tuple[int, int] | None
    discovery_enabled: bool
    discovery_min_ath: float
    discovery_every_h: int
    typesafe_api_key: str
    telegram_admins: list[int]

    @property
    def rpc_url(self) -> str:
        """URL RPC Solana : Helius si la clé existe, sinon le RPC de secours."""
        if self.helius_api_key:
            return f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        return self.solana_rpc_url_fallback or "https://solana-rpc.publicnode.com"

    @property
    def ws_url(self) -> str:
        if self.helius_api_key:
            return f"wss://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        return self.rpc_url.replace("https://", "wss://")


def load() -> Config:
    profile = os.getenv("X_PROFILE_DIR", "./data/x_profile")
    return Config(
        helius_api_key=os.getenv("HELIUS_API_KEY", "").strip(),
        solana_rpc_url_fallback=os.getenv("SOLANA_RPC_URL", "").strip(),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        x_profile_dir=(ROOT / profile).resolve(),
        x_accounts=_handles(_env("X_ACCOUNTS")),
        x_poll_seconds=_int("X_POLL_SECONDS", 180),
        x_headless=_bool("X_HEADLESS", True),
        x_browser=(_env("X_BROWSER") or "msedge").lower(),
        x_enabled=_bool("X_ENABLED", True),
        trace_max_hops=_int("TRACE_MAX_HOPS", 3),
        hot_wallet_tx_threshold=_int("HOT_WALLET_TX_THRESHOLD", 1000),
        db_path=DATA_DIR / "radar.db",
        watchlist_path=DATA_DIR / "watchlist.csv",
        labels_path=DATA_DIR / "labels_connus.csv",
        funding_min_sol=_float("FUNDING_MIN_SOL", 0.05),
        profit_min_sol=_float("PROFIT_MIN_SOL", 5.0),
        # « reserve-suspect » = devs au schéma des faux fonds souverains repérés par la découverte : toujours inclus
        rug_groups={g.strip() for g in (_env("RUG_GROUPS") or "reserve-cluster").split(",") if g.strip()}
        | {"reserve-suspect"},
        watch_max=_int("WATCH_MAX", 300),
        watch_stale_days=_int("WATCH_STALE_DAYS", 10),
        x_quiet_hours=_hours("X_QUIET_HOURS", "3-8"),
        discovery_enabled=_bool("DISCOVERY_ENABLED", True),
        discovery_min_ath=_float("DISCOVERY_MIN_ATH", 500_000),
        discovery_every_h=max(1, _int("DISCOVERY_EVERY_H", 6)),
        typesafe_api_key=_env("TYPESAFE_API_KEY"),
        telegram_admins=[int(x) for x in re.findall(r"-?\d+", _env("TELEGRAM_ADMINS"))],
    )


def set_env_value(key: str, value: str) -> None:
    """Modifie une seule ligne de .env (le reste du fichier est conservé)."""
    path = ROOT / ".env"
    lines = path.read_text(encoding="utf-8-sig").splitlines() if path.exists() else []
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}"
            break
    else:
        lines.append(f"{key}={value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def setup_logging(name: str = "radar", level: int = logging.INFO) -> None:
    """Journal dans la console + fichier tournant logs/<name>.log."""
    LOGS_DIR.mkdir(exist_ok=True)
    # La console Windows n'aime pas toujours les emojis : on force l'UTF-8.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s — %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(fmt)
    root.addHandler(console)
    fichier = RotatingFileHandler(LOGS_DIR / f"{name}.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fichier.setFormatter(fmt)
    root.addHandler(fichier)


def mask(secret: str) -> str:
    """Affiche un secret sans le révéler (pour les messages de diagnostic)."""
    if not secret:
        return "(vide)"
    return secret[:4] + "…" + secret[-2:] if len(secret) > 8 else "***"
