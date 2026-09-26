"""Analyse du texte d'un tweet : ticker, CA, heure de lancement, plateforme, signaux d'arnaque."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
CA_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
TICKER_RE = re.compile(r"(?<![\w$])\$([A-Za-z][A-Za-z0-9]{1,9})\b")
# Tickers qui ne sont jamais « le coin annoncé »
COMMON_TICKERS = {"SOL", "BTC", "ETH", "USD", "USDC", "USDT", "BNB", "XRP", "JUP", "BONK", "WIF", "PUMP",
                  "RAY", "TRX", "DOGE", "SUI", "HYPE", "TRUMP", "ADA", "AVAX", "TON", "LINK", "PEPE",
                  # faux tickers (« $TICKER launching soon », modèles de tweets)
                  "TICKER", "CA", "TOKEN", "COIN", "MEME", "CONTRACT", "XXX", "NEWCOIN", "YOURCOIN"}

ZONES = {
    "UTC": "UTC", "GMT": "UTC", "Z": "UTC",
    "ET": "America/New_York", "EST": "America/New_York", "EDT": "America/New_York",
    "CT": "America/Chicago", "CST": "America/Chicago", "CDT": "America/Chicago",
    "PT": "America/Los_Angeles", "PST": "America/Los_Angeles", "PDT": "America/Los_Angeles",
    "CET": "Europe/Paris", "CEST": "Europe/Paris", "BST": "Europe/London",
    "SGT": "Asia/Singapore", "HKT": "Asia/Hong_Kong", "KST": "Asia/Seoul", "JST": "Asia/Tokyo",
    "IST": "Asia/Kolkata", "WIB": "Asia/Jakarta", "UTC+8": "Asia/Singapore",
}
_ZONE_ALT = "|".join(sorted((re.escape(z) for z in ZONES if z != "Z"), key=len, reverse=True))
TIME_RE = re.compile(
    rf"(?<![\d:])(\d{{1,2}})(?:[:h.](\d{{2}}))?\s*(am|pm|a\.m\.|p\.m\.)?\s*\(?({_ZONE_ALT})\b\)?", re.I)
TIME_AMPM_RE = re.compile(r"(?<![\d:])(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.I)
RELATIVE_RE = re.compile(r"\bin\s+(\d{1,3})\s*(h|hrs?|hours?|m|mins?|minutes?)\b", re.I)
TOMORROW_RE = re.compile(r"\b(tomorrow|tmrw|tmr|demain)\b", re.I)

LAUNCH_RE = re.compile(
    r"\b(launch(?:ing|es|ed)?|goes? live|going live|live (?:at|in|on)|stealth|fair ?launch|countdown|"
    r"ca drops?|ca (?:at|in|reveal)|contract (?:at|drops?|reveal)|deploy(?:ing|s)?|dropping|"
    r"official (?:ca|contract)|t-?minus|lancement)\b", re.I)
PLATFORMS = [
    (re.compile(r"pump\.?fun|pumpfun|\bpump\b", re.I), "pump.fun"),
    (re.compile(r"raydium|launchlab", re.I), "Raydium"),
    (re.compile(r"meteora|\bdbc\b", re.I), "Meteora"),
    (re.compile(r"letsbonk|bonk\.fun", re.I), "LetsBonk"),
    (re.compile(r"believe(?:app)?", re.I), "Believe"),
    (re.compile(r"moonshot", re.I), "Moonshot"),
    (re.compile(r"jup(?:iter)? (?:launchpad|studio)", re.I), "Jupiter"),
]
SCAM_PATTERNS = [
    (re.compile(r"drop (?:your|ur) (?:sol|solana)?\s*(?:address|wallet|addy)", re.I), "« drop your SOL address »"),
    (re.compile(r"airdrop (?:to|for) (?:the )?first", re.I), "airdrop aux premiers"),
    (re.compile(r"\b\d{3,5}x\b", re.I), "promesse de gains (« 1000x »)"),
    (re.compile(r"guarantee|risk[- ]?free|can'?t lose", re.I), "gains « garantis »"),
    (re.compile(r"presale|pre-sale|send (?:sol|solana) to", re.I), "presale / envoi de SOL demandé"),
    (re.compile(r"(?:ca|contract).{0,25}(?:in|on|via) (?:our |the )?(?:tg|telegram|discord)", re.I),
     "CA donné seulement sur Telegram/Discord"),
    (re.compile(r"\bdm (?:me )?for (?:the )?(?:ca|contract|wl|whitelist)", re.I), "CA / whitelist en DM"),
]


def is_solana_address(s: str) -> bool:
    n = 0
    for ch in s:
        i = B58.find(ch)
        if i < 0:
            return False
        n = n * 58 + i
    size = (n.bit_length() + 7) // 8 + (len(s) - len(s.lstrip("1")))
    return size == 32


@dataclass
class TweetInfo:
    tickers: list[str] = field(default_factory=list)
    cas: list[str] = field(default_factory=list)
    launch_ts: int | None = None
    launch_txt: str | None = None
    platform: str | None = None
    launch_words: bool = False
    scam: list[str] = field(default_factory=list)
    launch_alts: list[int] = field(default_factory=list)   # heure sans fuseau : autres fuseaux possibles

    @property
    def is_candidate(self) -> bool:
        """Annonce de lancement plausible : un ticker ou un CA + un mot de lancement ou une heure."""
        return bool((self.tickers or self.cas) and (self.launch_words or self.launch_ts))


def _at(ref_utc: datetime, zone: str, hour: int, minute: int, tomorrow: bool) -> datetime:
    tz = ZoneInfo(zone)
    local_ref = ref_utc.astimezone(tz)
    cand = local_ref.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if tomorrow:
        cand += timedelta(days=1)
    elif cand < local_ref - timedelta(hours=1):
        cand += timedelta(days=1)  # « 18:00 UTC » écrit après 18 h -> le lendemain
    return cand.astimezone(timezone.utc)


def parse_launch_time(text: str, tweet_time: datetime) -> tuple[int | None, str | None]:
    tomorrow = bool(TOMORROW_RE.search(text))
    m = TIME_RE.search(text)
    if m:
        h, mi = int(m.group(1)), int(m.group(2) or 0)
        ampm = (m.group(3) or "").lower().replace(".", "")
        if ampm == "pm" and h < 12:
            h += 12
        elif ampm == "am" and h == 12:
            h = 0
        if 0 <= h <= 23 and 0 <= mi <= 59:
            zone = ZONES[m.group(4).upper()]
            return int(_at(tweet_time, zone, h, mi, tomorrow).timestamp()), m.group(0).strip()
    m = RELATIVE_RE.search(text)
    if m:
        n, unit = int(m.group(1)), m.group(2).lower()
        delta = timedelta(hours=n) if unit.startswith("h") else timedelta(minutes=n)
        return int((tweet_time + delta).timestamp()), m.group(0).strip()
    m = TIME_AMPM_RE.search(text)
    if m:  # « 6pm » sans fuseau : UTC par défaut, les autres fuseaux sont dans ambiguous_times()
        h, mi = int(m.group(1)), int(m.group(2) or 0)
        if m.group(3).lower() == "pm" and h < 12:
            h += 12
        if 0 <= h <= 23:
            return int(_at(tweet_time, "UTC", h, mi, tomorrow).timestamp()), m.group(0).strip() + " (fuseau ?)"
    return None, None


# Fuseaux les plus probables pour une heure donnée sans fuseau (« launch at 6pm »)
AMBIGUOUS_ZONES = ("UTC", "America/New_York", "America/Los_Angeles", "Europe/Paris", "Asia/Singapore")


def ambiguous_times(text: str, tweet_time: datetime) -> list[int]:
    """Toutes les heures possibles d'un « 6pm » sans fuseau (le compte peut être à New York, Paris…)."""
    m = TIME_AMPM_RE.search(text)
    if not m or TIME_RE.search(text):
        return []
    h, mi = int(m.group(1)), int(m.group(2) or 0)
    if m.group(3).lower() == "pm" and h < 12:
        h += 12
    if not 0 <= h <= 23:
        return []
    tomorrow = bool(TOMORROW_RE.search(text))
    return sorted({int(_at(tweet_time, z, h, mi, tomorrow).timestamp()) for z in AMBIGUOUS_ZONES})


def parse_tweet(text: str, tweet_time: datetime | None = None, links: list[str] | None = None) -> TweetInfo:
    tweet_time = tweet_time or datetime.now(timezone.utc)
    blob = text + " " + " ".join(links or [])
    info = TweetInfo()
    for t in TICKER_RE.findall(text):
        t = t.upper()
        if t not in COMMON_TICKERS and t not in info.tickers:
            info.tickers.append(t)
    for c in CA_RE.findall(blob):
        if c not in info.cas and is_solana_address(c):
            info.cas.append(c)
    info.launch_ts, info.launch_txt = parse_launch_time(text, tweet_time)
    info.launch_alts = ambiguous_times(text, tweet_time)
    info.launch_words = bool(LAUNCH_RE.search(text))
    for rx, name in PLATFORMS:
        if rx.search(blob):
            info.platform = name
            break
    info.scam = [label for rx, label in SCAM_PATTERNS if rx.search(text)]
    return info


if __name__ == "__main__":  # petit auto-test
    ref = datetime(2026, 9, 26, 9, 0, tzinfo=timezone.utc)
    for t in ["$ASH launching today 18:00 UTC on Raydium, CA: DU5L11pfQ1EyDWvBhM5sp2piGeHEYDy9sdTvDkfcKrNs",
              "Stealth launch of $MOON on pump.fun at 2pm EST 🚀 1000x guaranteed, drop your SOL address",
              "$CAT goes live in 3 hours! CA drops on our telegram",
              "gm frens, $SOL looking strong"]:
        i = parse_tweet(t, ref)
        when = datetime.fromtimestamp(i.launch_ts, timezone.utc).strftime("%d/%m %H:%M UTC") if i.launch_ts else None
        print(i.is_candidate, i.tickers, i.cas, when, i.launch_txt, i.platform, i.scam)
