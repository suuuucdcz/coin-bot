"""Base SQLite : wallets suivis, tokens vus, alertes envoyées, liens de cluster."""
from __future__ import annotations

import csv
import sqlite3
import time
from pathlib import Path

from .analysis.xparse import is_solana_address

SCHEMA = """
CREATE TABLE IF NOT EXISTS wallets (
    address   TEXT PRIMARY KEY,
    label     TEXT,
    grp       TEXT,
    role      TEXT,
    notes     TEXT,
    depth     INTEGER DEFAULT 0,      -- 0 = watchlist de départ, 1..3 = ajouté par le traçage
    parent    TEXT,                   -- wallet qui l'a financé (si ajouté automatiquement)
    active    INTEGER DEFAULT 1,
    added_at  INTEGER
);
CREATE TABLE IF NOT EXISTS tokens (
    mint        TEXT PRIMARY KEY,
    symbol      TEXT,
    name        TEXT,
    creator     TEXT,
    created_at  INTEGER,
    first_seen  INTEGER,
    meta_json   TEXT
);
CREATE TABLE IF NOT EXISTS alerts (
    key      TEXT PRIMARY KEY,        -- clé de dédoublonnage (ex. "create:<mint>")
    kind     TEXT,
    sent_at  INTEGER
);
CREATE TABLE IF NOT EXISTS links (
    src       TEXT,
    dst       TEXT,
    kind      TEXT,                   -- funding, relais, frère, profits…
    amount    REAL,
    signature TEXT,
    ts        INTEGER,
    PRIMARY KEY (src, dst, signature)
);
CREATE TABLE IF NOT EXISTS labels (
    address TEXT PRIMARY KEY,
    label   TEXT,                     -- ex. "hot wallet / service"
    at      INTEGER
);
CREATE TABLE IF NOT EXISTS wallet_state (
    address  TEXT PRIMARY KEY,
    last_sig TEXT,                    -- dernière tx traitée (rattrapage après coupure)
    last_ts  INTEGER
);
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,           -- ex. "topic:agenda", "agenda_msg:2026-09-26"
    value TEXT
);
CREATE TABLE IF NOT EXISTS announcements (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT,                 -- en majuscules, sans $
    name        TEXT,
    handle      TEXT,                 -- compte X principal
    sources     TEXT,                 -- JSON : [{handle, url}] (tous les tweets qui en parlent)
    tweet_url   TEXT,
    tweet_text  TEXT,
    launch_ts   INTEGER,              -- heure de lancement (UTC) si trouvée
    launch_txt  TEXT,                 -- texte brut (« 18:00 UTC »)
    platform    TEXT,
    ca          TEXT,
    dev         TEXT,
    satellites  TEXT,                 -- JSON : [{address, role}]
    status      TEXT DEFAULT 'annoncé',
    flags       TEXT,                 -- JSON : signaux d'arnaque
    account     TEXT,                 -- JSON : profil du compte X
    msg_id      INTEGER,
    first_seen  INTEGER,
    updated     INTEGER
);
CREATE TABLE IF NOT EXISTS tweets_seen (
    url TEXT PRIMARY KEY,
    at  INTEGER
);
CREATE TABLE IF NOT EXISTS networks (
    seed  TEXT PRIMARY KEY,           -- wallet de départ
    data  TEXT,                       -- JSON : wallets, liens, projets (radar/analysis/network.py)
    at    INTEGER
);
CREATE TABLE IF NOT EXISTS x_accounts (
    handle     TEXT PRIMARY KEY,
    data       TEXT,                  -- JSON : abonnés, date de création, certifié
    checked_at INTEGER
);
CREATE TABLE IF NOT EXISTS results (   -- suivi des alertes sur 24 h (radar/results.py)
    key        TEXT PRIMARY KEY,      -- clé de l'alerte suivie
    mint       TEXT NOT NULL,
    symbol     TEXT,
    kind       TEXT,                  -- create, buy, cluster, lp_add, top, annonce…
    grp        TEXT,
    trust      TEXT,
    sent_at    INTEGER,
    mc0        REAL,                  -- market cap au moment de l'alerte (ou 1re mesure)
    mc_max     REAL,                  -- plus haut vu ensuite
    mc_1h      REAL,
    mc_24h     REAL,
    last_mc    REAL,
    last_check INTEGER,
    done       INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS results_open ON results(done, sent_at);
"""


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # timeout : si un autre programme écrit dans la base (script, outil), on attend au lieu de planter
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.import_warnings: list[str] = []
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        # Colonnes ajoutées après coup (bases déjà créées)
        for ddl in ("ALTER TABLE announcements ADD COLUMN details TEXT",):
            try:
                self.conn.execute(ddl)
            except sqlite3.OperationalError:
                pass
        self.conn.commit()

    # --- wallets -----------------------------------------------------------
    def import_watchlist(self, csv_path: Path) -> int:
        """Charge data/watchlist.csv : c'est la référence pour les wallets de départ.

        - adresse invalide ou en double : ligne ignorée (voir self.import_warnings) ;
        - label / groupe / rôle modifiés dans le CSV : mis à jour en base ;
        - wallet de départ retiré du CSV : mis en veille (il n'est plus surveillé).
        Renvoie le nombre de nouveaux wallets.
        """
        n = 0
        vus: set[str] = set()
        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            for i, row in enumerate(csv.DictReader(f), start=2):
                addr = (row.get("address") or "").strip()
                if not addr:
                    continue
                if not is_solana_address(addr):
                    self.import_warnings.append(f"{csv_path.name} ligne {i} : adresse invalide « {addr[:50]} »")
                    continue
                if addr in vus:
                    self.import_warnings.append(f"{csv_path.name} ligne {i} : adresse en double {addr}")
                    continue
                vus.add(addr)
                label = (row.get("label") or "").strip() or f"W_{addr[:4]}"
                grp, role, notes = ((row.get(k) or "").strip() for k in ("group", "role", "notes"))
                if not grp:
                    self.import_warnings.append(f"{csv_path.name} ligne {i} : pas de groupe pour {label}")
                existe = self.wallet(addr) is not None
                self.conn.execute(
                    "INSERT INTO wallets(address,label,grp,role,notes,depth,added_at) VALUES(?,?,?,?,?,0,?) "
                    "ON CONFLICT(address) DO UPDATE SET label=excluded.label, grp=excluded.grp, role=excluded.role, "
                    "notes=excluded.notes, depth=0, active=1",
                    (addr, label, grp, role, notes, int(time.time())))
                n += not existe
        if vus:
            retires = [r["address"] for r in self.conn.execute(
                "SELECT address FROM wallets WHERE depth=0 AND active=1 AND COALESCE(grp,'') != 'découverte'")
                if r["address"] not in vus]
            if retires:
                self.conn.executemany("UPDATE wallets SET active=0 WHERE address=?", [(a,) for a in retires])
                self.import_warnings.append(f"{len(retires)} wallet(s) retiré(s) de {csv_path.name} : mis en veille")
        self.conn.commit()
        return n

    def add_wallet(self, address: str, label: str, grp: str, role: str, depth: int, parent: str | None, notes: str = "") -> bool:
        """Ajoute un wallet (ou réactive un wallet mis en veille). True s'il est désormais suivi."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO wallets(address,label,grp,role,notes,depth,parent,added_at) VALUES(?,?,?,?,?,?,?,?)",
            (address, label, grp, role, notes, depth, parent, int(time.time())),
        )
        if not cur.rowcount:
            cur = self.conn.execute("UPDATE wallets SET active=1, added_at=? WHERE address=? AND active=0",
                                    (int(time.time()), address))
        self.conn.commit()
        return cur.rowcount > 0

    def stale_wallets(self, days: int) -> list[str]:
        """Wallets ajoutés automatiquement, sans aucune activité depuis `days` jours.

        La watchlist de départ (profondeur 0, data/watchlist.csv) n'est jamais concernée,
        sauf les devs trouvés par la découverte automatique.
        """
        cutoff = int(time.time()) - days * 86400
        rows = self.conn.execute(
            "SELECT w.address FROM wallets w LEFT JOIN wallet_state s ON s.address = w.address "
            "WHERE w.active = 1 AND (w.depth > 0 OR w.grp = 'découverte') "
            "AND COALESCE(w.added_at, 0) < ? AND COALESCE(s.last_ts, 0) < ?", (cutoff, cutoff)).fetchall()
        return [r["address"] for r in rows]

    def least_active(self, limit: int = 300) -> list[sqlite3.Row]:
        """Wallets ajoutés automatiquement, du moins actif au plus actif (candidats à la mise en veille)."""
        return self.conn.execute(
            "SELECT w.* FROM wallets w LEFT JOIN wallet_state s ON s.address = w.address "
            "WHERE w.active = 1 AND w.depth > 0 ORDER BY COALESCE(s.last_ts, 0), w.added_at LIMIT ?",
            (limit,)).fetchall()

    def deactivate(self, addresses: list[str]) -> None:
        self.conn.executemany("UPDATE wallets SET active=0 WHERE address=?", [(a,) for a in addresses])
        self.conn.commit()

    def wallet(self, address: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM wallets WHERE address=?", (address,)).fetchone()

    def active_wallets(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM wallets WHERE active=1").fetchall()

    # --- alertes (dédoublonnage) --------------------------------------------
    def alert_already_sent(self, key: str) -> bool:
        return self.conn.execute("SELECT 1 FROM alerts WHERE key=?", (key,)).fetchone() is not None

    def mark_alert_sent(self, key: str, kind: str) -> None:
        self.conn.execute("INSERT OR IGNORE INTO alerts(key,kind,sent_at) VALUES(?,?,?)", (key, kind, int(time.time())))
        self.conn.commit()

    def forget_alert(self, key: str) -> None:
        """L'envoi a définitivement échoué : la même alerte pourra repartir si l'événement se reproduit."""
        self.conn.execute("DELETE FROM alerts WHERE key=?", (key,))
        self.conn.commit()

    def alerts_since(self, since: int) -> dict[str, int]:
        rows = self.conn.execute("SELECT kind, COUNT(*) n FROM alerts WHERE sent_at>=? GROUP BY kind", (since,))
        return {r["kind"] or "?": r["n"] for r in rows}

    # --- liens / étiquettes ------------------------------------------------
    def add_link(self, src: str, dst: str, kind: str, amount: float, signature: str, ts: int) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO links(src,dst,kind,amount,signature,ts) VALUES(?,?,?,?,?,?)",
            (src, dst, kind, amount, signature, ts),
        )
        self.conn.commit()

    def set_label(self, address: str, label: str) -> None:
        self.conn.execute("INSERT OR REPLACE INTO labels(address,label,at) VALUES(?,?,?)", (address, label, int(time.time())))
        self.conn.commit()

    def get_label(self, address: str) -> str | None:
        row = self.conn.execute("SELECT label FROM labels WHERE address=?", (address,)).fetchone()
        return row["label"] if row else None

    def import_labels(self, csv_path: Path) -> None:
        """Exchanges connus (data/labels_connus.csv). Les adresses invalides sont signalées et ignorées."""
        if not csv_path.exists():
            return
        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            for i, row in enumerate(csv.DictReader(f), start=2):
                addr, label = (row.get("address") or "").strip(), (row.get("label") or "").strip()
                if not addr:
                    continue
                if not is_solana_address(addr) or not label:
                    self.import_warnings.append(f"{csv_path.name} ligne {i} : adresse ou étiquette invalide")
                    continue
                self.set_label(addr, label)

    def labels(self) -> dict[str, str]:
        return {r["address"]: r["label"] for r in self.conn.execute("SELECT address,label FROM labels")}

    # --- tokens ---------------------------------------------------------------
    def upsert_token(self, mint: str, symbol: str | None, name: str | None, creator: str | None,
                     created_at: int | None) -> None:
        self.conn.execute(
            """INSERT INTO tokens(mint,symbol,name,creator,created_at,first_seen) VALUES(?,?,?,?,?,?)
               ON CONFLICT(mint) DO UPDATE SET
                 symbol=COALESCE(excluded.symbol,symbol), name=COALESCE(excluded.name,name),
                 creator=COALESCE(creator,excluded.creator), created_at=COALESCE(created_at,excluded.created_at)""",
            (mint, symbol, name, creator, created_at, int(time.time())),
        )
        self.conn.commit()

    def token(self, mint: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM tokens WHERE mint=?", (mint,)).fetchone()

    def tokens_created_by(self, creator: str) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM tokens WHERE creator=?", (creator,)).fetchall()

    # --- état du suivi ---------------------------------------------------------
    def last_sig(self, address: str) -> str | None:
        row = self.conn.execute("SELECT last_sig FROM wallet_state WHERE address=?", (address,)).fetchone()
        return row["last_sig"] if row else None

    def set_last_sig(self, address: str, sig: str, ts: int | None = None) -> None:
        """Point de reprise ; `ts` = heure (bloc) de la tx, pour savoir depuis quand le wallet dort."""
        self.conn.execute("INSERT OR REPLACE INTO wallet_state(address,last_sig,last_ts) VALUES(?,?,?)",
                          (address, sig, int(ts or time.time())))
        self.conn.commit()

    def last_activity(self, address: str) -> int:
        row = self.conn.execute("SELECT last_ts FROM wallet_state WHERE address=?", (address,)).fetchone()
        return int(row["last_ts"] or 0) if row else 0

    # --- réglages (sujets Telegram, message épinglé…) ------------------------
    def get(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    # --- suivi des résultats -----------------------------------------------------
    def add_result(self, key: str, mint: str, kind: str, symbol: str | None, grp: str | None, trust: str | None,
                   mc0: float | None, sent_at: int | None = None) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO results(key,mint,symbol,kind,grp,trust,sent_at,mc0) VALUES(?,?,?,?,?,?,?,?)",
            (key, mint, symbol, kind, grp, trust, int(sent_at or time.time()), mc0 if mc0 and mc0 > 0 else None))
        self.conn.commit()

    def results_open(self, since: int) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM results WHERE done=0 AND sent_at>=?", (since,)).fetchall()

    def update_result(self, key: str, mc: float | None, peak: float | None, now: int) -> None:
        """Nouvelle mesure : plus haut, valeur à +1 h (1re mesure après 1 h), à +24 h (et fin du suivi)."""
        r = self.conn.execute("SELECT * FROM results WHERE key=?", (key,)).fetchone()
        if r is None:
            return
        age = now - (r["sent_at"] or now)
        haut = max(x for x in (r["mc_max"] or 0, mc or 0, peak or 0))
        mc0 = r["mc0"] or mc
        self.conn.execute(
            "UPDATE results SET mc0=?, mc_max=?, last_mc=COALESCE(?, last_mc), last_check=?, "
            "mc_1h=CASE WHEN mc_1h IS NULL AND ?>=3600 THEN ? ELSE mc_1h END, "
            "mc_24h=CASE WHEN ?>=86400 THEN ? ELSE mc_24h END, done=CASE WHEN ?>=86400 THEN 1 ELSE 0 END WHERE key=?",
            (mc0, haut or None, mc, now, age, mc, age, mc, age, key))
        self.conn.commit()

    def results_since(self, since: int) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM results WHERE sent_at>=? ORDER BY sent_at", (since,)).fetchall()

    # --- remise à zéro ----------------------------------------------------------------
    # Toujours effacés : l'historique et les compteurs (alertes, agenda, tokens, résultats, compteurs d'activité).
    # Toujours gardés : configuration Telegram (sections, messages épinglés), réglages (son, sourdines), compteurs
    # d'API (crédits Helius du mois, quota Gemini du jour).
    RESET_TABLES = ("alerts", "results", "announcements", "tokens", "tweets_seen", "networks")
    RESET_KEYS = ("ann_seen:", "buys:", "creates:", "grp_tokens:", "daily_report", "discovery_last")
    # Avec tout=True, en plus : ce que le radar a APPRIS (wallets ajoutés, liens, étiquettes, classements)
    LEARNED_TABLES = ("links", "labels", "wallet_state", "x_accounts")
    LEARNED_KEYS = ("farm:", "sniper:", "factory:", "noisy:", "disc:", "disc_up:", "lance:")

    def remise_a_zero(self, tout: bool = False) -> dict[str, int]:
        """Efface l'historique et les compteurs (voir RESET_*). tout=True : repart aussi de la watchlist de départ."""
        n: dict[str, int] = {}
        for t in self.RESET_TABLES + (self.LEARNED_TABLES if tout else ()):
            n[t] = self.conn.execute(f"DELETE FROM {t}").rowcount
        prefixes = self.RESET_KEYS + (self.LEARNED_KEYS if tout else ())
        cles = [k for (k,) in self.conn.execute("SELECT key FROM settings") if k.startswith(prefixes)]
        self.conn.executemany("DELETE FROM settings WHERE key=?", [(k,) for k in cles])
        n["compteurs"] = len(cles)
        if tout:
            # Ne restent que les wallets de data/watchlist.csv (réimportée à chaque démarrage)
            n["wallets"] = self.conn.execute(
                "DELETE FROM wallets WHERE NOT (depth=0 AND COALESCE(grp,'') != 'découverte')").rowcount
        self.conn.commit()
        self.conn.execute("VACUUM")
        return n

    def settings_like(self, prefix: str) -> list[tuple[str, str]]:
        """Réglages dont la clé commence par ce préfixe (ex. « noisy: »)."""
        return [(r["key"], r["value"]) for r in
                self.conn.execute("SELECT key, value FROM settings WHERE key LIKE ? || '%'", (prefix,))]

    def put(self, key: str, value: str | int | None) -> None:
        if value is None:
            self.conn.execute("DELETE FROM settings WHERE key=?", (key,))
        else:
            self.conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, str(value)))
        self.conn.commit()

    # --- annonces X ---------------------------------------------------------------
    def unsee_tweet(self, url: str) -> None:
        """Remet un tweet à lire plus tard (l'IA n'a pas eu le temps de le lire)."""
        self.conn.execute("DELETE FROM tweets_seen WHERE url=?", (url,))
        self.conn.commit()

    def tweet_seen(self, url: str) -> bool:
        cur = self.conn.execute("INSERT OR IGNORE INTO tweets_seen(url,at) VALUES(?,?)", (url, int(time.time())))
        self.conn.commit()
        return cur.rowcount == 0

    def find_announcement(self, ticker: str | None, ca: str | None, since: int) -> sqlite3.Row | None:
        if ca:
            row = self.conn.execute("SELECT * FROM announcements WHERE ca=? ORDER BY id DESC LIMIT 1", (ca,)).fetchone()
            if row:
                return row
        if ticker:
            return self.conn.execute(
                "SELECT * FROM announcements WHERE ticker=? AND first_seen>=? "
                "AND COALESCE(status, '') NOT LIKE 'écarté%' ORDER BY id DESC LIMIT 1",
                (ticker, since)).fetchone()
        return None

    def insert_announcement(self, **fields) -> int:
        fields.setdefault("first_seen", int(time.time()))
        fields["updated"] = int(time.time())
        cols = ",".join(fields)
        cur = self.conn.execute(f"INSERT INTO announcements({cols}) VALUES({','.join('?' * len(fields))})",
                                tuple(fields.values()))
        self.conn.commit()
        return cur.lastrowid

    def update_announcement(self, ann_id: int, **fields) -> None:
        if not fields:
            return
        fields["updated"] = int(time.time())
        sets = ",".join(f"{k}=?" for k in fields)
        self.conn.execute(f"UPDATE announcements SET {sets} WHERE id=?", (*fields.values(), ann_id))
        self.conn.commit()

    def announcement(self, ann_id: int) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM announcements WHERE id=?", (ann_id,)).fetchone()

    def announcements_since(self, since: int) -> list[sqlite3.Row]:
        """Annonces vues depuis `since` ou dont le lancement est après `since`."""
        return self.conn.execute(
            "SELECT * FROM announcements WHERE (first_seen>=? OR launch_ts>=?) "
            "AND COALESCE(status, '') NOT LIKE 'écarté%' ORDER BY COALESCE(launch_ts, 9e18), id",
            (since, since)).fetchall()

    # --- profils X ------------------------------------------------------------------
    def x_account(self, handle: str) -> tuple[str, int] | None:
        row = self.conn.execute("SELECT data, checked_at FROM x_accounts WHERE handle=?", (handle.lower(),)).fetchone()
        return (row["data"], row["checked_at"]) if row else None

    def set_x_account(self, handle: str, data: str) -> None:
        self.conn.execute("INSERT OR REPLACE INTO x_accounts(handle,data,checked_at) VALUES(?,?,?)",
                          (handle.lower(), data, int(time.time())))
        self.conn.commit()

    def get_network(self, seed: str) -> tuple[str, int] | None:
        row = self.conn.execute("SELECT data, at FROM networks WHERE seed=?", (seed,)).fetchone()
        return (row["data"], row["at"]) if row else None

    def put_network(self, seed: str, data: str) -> None:
        self.conn.execute("INSERT OR REPLACE INTO networks(seed,data,at) VALUES(?,?,?)", (seed, data, int(time.time())))
        self.conn.commit()

    def stats(self) -> dict[str, int]:
        q = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "wallets suivis": q("SELECT COUNT(*) FROM wallets WHERE active=1"),
            "wallets en veille": q("SELECT COUNT(*) FROM wallets WHERE active=0"),
            "tokens vus": q("SELECT COUNT(*) FROM tokens"),
            "alertes envoyées": q("SELECT COUNT(*) FROM alerts"),
            "liens de cluster": q("SELECT COUNT(*) FROM links"),
            "étiquettes (exchanges…)": q("SELECT COUNT(*) FROM labels"),
            "annonces X": q("SELECT COUNT(*) FROM announcements"),
            "tweets lus": q("SELECT COUNT(*) FROM tweets_seen"),
        }

    def close(self) -> None:
        self.conn.close()


# ---------------------------------------------------------------------------
# python -m radar.db            -> résumé de la base
# python -m radar.db wallets    -> liste des wallets suivis
def main() -> int:
    import sys

    from . import config as cfgmod
    cfg = cfgmod.load()
    if not cfg.db_path.exists():
        print("La base n'existe pas encore : elle sera créée au premier lancement du radar.")
        return 0
    db = DB(cfg.db_path)
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "remise-a-zero":
            # A lancer radar ARRÊTÉ (sinon il réécrit aussitôt ce qu'il a en mémoire)
            tout = "--tout" in sys.argv
            copie = cfg.db_path.with_name(f"{cfg.db_path.name}.avant-remise-{time.strftime('%Y%m%d-%H%M%S')}")
            with sqlite3.connect(copie) as dest:
                db.conn.backup(dest)
            n = db.remise_a_zero(tout)
            print(f"Sauvegarde : {copie.name}")
            print("Effacé : " + ", ".join(f"{k} {v}" for k, v in n.items()))
            print("Gardé : configuration Telegram, réglages, crédits Helius du mois, quota Gemini du jour"
                  + ("" if tout else ", watchlist apprise, liens, étiquettes, classements (snipers, usines…)"))
            return 0
        if len(sys.argv) > 1 and sys.argv[1] == "wallets":
            for w in db.conn.execute("SELECT * FROM wallets WHERE active=1 ORDER BY grp, depth, label"):
                print(f"{(w['grp'] or '-')[:18]:18} {(w['label'] or '')[:22]:22} p{w['depth']}  {w['address']}  "
                      f"{(w['role'] or '')[:50]}")
            return 0
        print(f"Base : {cfg.db_path}")
        for k, v in db.stats().items():
            print(f"  {k:26} {v}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
