"""Veille X via Playwright (profil navigateur persistant, rythme lent pour ne pas faire bannir le compte).

Commandes :
    python -m radar.sources.x_watch login   -> ouvre X pour te connecter une fois (connexion_x.bat)
    python -m radar.sources.x_watch test    -> fait une recherche et affiche les annonces trouvées
    python -m radar.sources.x_watch profil <compte> -> fiche lue par le radar + note de fiabilité
    python -m radar.sources.x_watch exporter -> (sur le PC) écrit la session X dans data/session_x_export.json
    python -m radar.sources.x_watch importer -> (sur le serveur) reprend cette session, sans fenêtre à ouvrir
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import quote
from zoneinfo import ZoneInfo

from .. import config as cfgmod

log = logging.getLogger("x_watch")

PARIS = ZoneInfo("Europe/Paris")
SESSION_MARKER = "session_x.json"   # écrit après une connexion réussie (cookie auth_token présent)
LOGIN_TIMEOUT_S = 15 * 60
EXPORT_NAME = "session_x_export.json"   # cookies X : aussi secret qu'un mot de passe (jamais dans git)


def has_session(profile_dir: Path) -> bool:
    """Une connexion X a-t-elle réussi avec ce profil ?"""
    return (profile_dir / SESSION_MARKER).exists()


def quiet_until(hours: tuple[int, int] | None, now: datetime | None = None) -> datetime | None:
    """Pendant la pause de nuit (heure de Paris), renvoie l'heure de reprise ; sinon None."""
    if not hours:
        return None
    now = now or datetime.now(PARIS)
    debut, fin = hours
    h = now.hour
    dedans = debut <= h < fin if debut < fin else (h >= debut or h < fin)
    if not dedans:
        return None
    reprise = now.replace(hour=fin, minute=0, second=0, microsecond=0)
    return reprise if reprise > now else reprise + timedelta(days=1)

QUERIES = [
    '"launching today" solana', '"launches tomorrow" solana', '"CA drops"', '"stealth launch" pump',
    '"fair launch" pump.fun', '"official contract" solana', 'countdown token launch solana', 'UsePaid',
]

EXTRACT_JS = """
() => Array.from(document.querySelectorAll('article[data-testid="tweet"]')).map(a => {
  const t = a.querySelector('time');
  const link = t ? t.closest('a') : null;
  const un = a.querySelector('[data-testid="User-Name"]');
  const txt = a.querySelector('[data-testid="tweetText"]');
  const grp = a.querySelector('[role="group"]');
  const links = Array.from(a.querySelectorAll('[data-testid="tweetText"] a, [data-testid="card.wrapper"] a'))
      .map(x => (x.innerText || '') + ' ' + (x.href || ''));
  const images = Array.from(a.querySelectorAll('[data-testid="tweetPhoto"] img'))
      .map(i => i.src).filter(s => s && s.startsWith('https://pbs.twimg.com/media/'));
  return {
    url: link ? link.href : null,
    time: t ? t.getAttribute('datetime') : null,
    user: un ? un.innerText : '',
    verified: !!(un && un.querySelector('[data-testid="icon-verified"]')),
    text: txt ? txt.innerText : '',
    stats: grp ? (grp.getAttribute('aria-label') || '') : '',
    links: links,
    images: images,
  };
})
"""

PROFILE_JS = """
() => {
  const q = s => document.querySelector(s);
  const f = q('a[href$="/verified_followers"]') || q('a[href$="/followers"]');
  const fo = q('a[href$="/following"]');
  const j = q('[data-testid="UserJoinDate"]');
  const n = q('[data-testid="UserName"]');
  const d = q('[data-testid="UserDescription"]');
  const icon = n ? n.querySelector('[data-testid="icon-verified"]') : null;
  // Badge : bleu = abonnement payant ; or (dégradé) = organisation ; gris = institution
  let vtype = null;
  if (icon) {
    const html = icon.outerHTML;
    const col = getComputedStyle(icon).color || '';
    vtype = (html.includes('linearGradient') || html.includes('url(#')) ? 'gold'
          : /130,\\s*154,\\s*171/.test(col) ? 'grey' : 'blue';
  }
  return {
    followers: f ? f.innerText : '',
    following: fo ? fo.innerText : '',
    joined: j ? j.innerText : '',
    verified: !!icon,
    verified_type: vtype,
    bio: d ? d.innerText : '',
  };
}
"""

MONTHS = {m: i + 1 for i, m in enumerate(
    "january february march april may june july august september october november december".split())}
MONTHS.update({m: i + 1 for i, m in enumerate(
    "janvier février mars avril mai juin juillet août septembre octobre novembre décembre".split())})


def parse_count(txt: str) -> int | None:
    """« 1,234 Followers » / « 1.2K » / « 3,4 k abonnés » -> nombre."""
    m = re.search(r"([\d.,\s ]+)\s*([KkMm]|k|M)?", txt or "")
    if not m or not m.group(1).strip():
        return None
    # X en français sépare les milliers par une espace fine insécable ( ) : on retire tout espacement
    num, unit = re.sub(r"\s", "", m.group(1)), (m.group(2) or "").lower()
    if unit:
        num = num.replace(",", ".")
        try:
            return int(float(num) * (1000 if unit == "k" else 1_000_000))
        except ValueError:
            return None
    digits = re.sub(r"[.,]", "", num)
    return int(digits) if digits.isdigit() else None


def parse_joined(txt: str) -> str | None:
    """« Joined September 2026 » / « A rejoint X en septembre 2026 » -> « 09/2026 »."""
    low = (txt or "").lower()
    y = re.search(r"(20\d{2})", low)
    mo = next((v for k, v in MONTHS.items() if k in low), None)
    if y and mo:
        return f"{mo:02d}/{y.group(1)}"
    return y.group(1) if y else None


# Page « À propos de ce compte » (x.com/<compte>/about) : anglais et français
ABOUT_JOINED_RE = re.compile(r"(?:date joined|joined|date d.inscription|a rejoint x|inscrit)\D{0,20}?"
                             r"([A-Za-zéûÉ]+\.?\s+\d{4})", re.I)
ABOUT_BASED_RE = re.compile(r"(?:account based in|based in|compte basé|basé)\s*(?:en|au|aux|à|in)?\s*[:\n]?\s*"
                            r"([^\n]{2,40})", re.I)
ABOUT_RENAMES_RE = [
    re.compile(r"(\d+)\s+(?:username changes?|changements? (?:de|du) nom d.utilisateur)", re.I),
    re.compile(r"(?:username changes?|changements? (?:de|du) nom d.utilisateur)\s*[:\n]?\s*(\d+)", re.I),
]
ABOUT_NEVER_RE = re.compile(r"(?:never changed|jamais modifié|aucun changement)", re.I)


def parse_about(text: str) -> dict:
    """Lecture souple de la page « À propos » : les libellés exacts peuvent changer côté X."""
    out: dict = {}
    m = ABOUT_JOINED_RE.search(text)
    if m:
        out["joined"] = parse_joined(m.group(1))
    m = ABOUT_BASED_RE.search(text)
    if m:
        out["based_in"] = m.group(1).strip()
    for rx in ABOUT_RENAMES_RE:
        m = rx.search(text)
        if m:
            out["username_changes"] = int(m.group(1))
            break
    else:
        if ABOUT_NEVER_RE.search(text):
            out["username_changes"] = 0
    return out


def handle_from_url(url: str | None) -> str | None:
    m = re.search(r"x\.com/([A-Za-z0-9_]{1,15})/status/", url or "")
    return m.group(1) if m else None


class XWatcher:
    def __init__(self, cfg: cfgmod.Config, on_tweets: Callable[[list[dict]], Awaitable[None]],
                 on_problem: Callable[[str], None] | None = None):
        self.cfg = cfg
        self.on_tweets = on_tweets
        self.on_problem = on_problem
        self.profile_requests: asyncio.Queue[tuple[str, asyncio.Future]] = asyncio.Queue()
        self._warned = False
        # Actions choisies par l'IA locale (agenda._plan_x) : elles REMPLACENT des recherches fixes,
        # le nombre de pages lues par tour ne change pas
        self.extra_jobs: list[tuple[str, str]] = []

    async def _request(self, kind: str, handle: str):
        fut = asyncio.get_running_loop().create_future()
        self.profile_requests.put_nowait((f"{kind}:{handle}", fut))
        try:
            return await asyncio.wait_for(fut, 1800)
        except asyncio.TimeoutError:
            return None

    async def profile(self, handle: str) -> dict | None:
        """Demande le profil d'un compte (traité au prochain passage, rythme lent)."""
        return await self._request("profile", handle)

    async def user_tweets(self, handle: str) -> list[dict] | None:
        """Demande les derniers tweets d'un compte (chasse au dev : anciens CA publiés)."""
        return await self._request("timeline", handle)

    async def deep_search(self, query: str) -> list[dict] | None:
        """Recherche X sans limite de date (ex. « from:compte (pump OR CA) »)."""
        return await self._request("deepsearch", query)

    async def _open(self, pw, headless: bool):
        self.cfg.x_profile_dir.mkdir(parents=True, exist_ok=True)
        # Edge (signé Microsoft, déjà installé) : le Chromium de Playwright est bloqué par
        # Windows 11 (« spawn UNKNOWN », Smart App Control) sur le PC de Maxence.
        channel = self.cfg.x_browser if self.cfg.x_browser in ("msedge", "chrome") else None
        args = ["--disable-blink-features=AutomationControlled"]
        if sys.platform.startswith("linux"):
            args.append("--disable-dev-shm-usage")   # petits serveurs : /dev/shm trop petit pour Chromium
        if getattr(self.cfg, "x_light", False):
            # 1 Go de mémoire : un seul processus de rendu, pas d'isolation par site ni de services annexes
            # (vu en vrai : 0 tweet sur le serveur, la page n'avait pas fini de s'afficher en 20 s)
            args += ["--disable-gpu", "--renderer-process-limit=1", "--disable-extensions",
                     "--disable-background-networking", "--disable-features=site-per-process,IsolateOrigins,"
                     "Translate,MediaRouter,OptimizationHints"]
        taille = {"width": 1024, "height": 768} if getattr(self.cfg, "x_light", False) else {"width": 1280, "height": 900}
        ctx = await pw.chromium.launch_persistent_context(
            str(self.cfg.x_profile_dir), channel=channel, headless=headless, viewport=taille, args=args)
        if getattr(self.cfg, "x_light", False):
            # Serveur à 1 Go (Google Cloud e2-micro) : ni images, ni vidéos, ni polices. Le texte des tweets et
            # les adresses des images (lues dans la page) restent disponibles ; mémoire et trafic divisés.
            async def _leger(route):
                if route.request.resource_type in ("image", "media", "font"):
                    await route.abort()
                else:
                    await route.continue_()
            await ctx.route("**/*", _leger)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        return ctx, page

    async def run(self) -> None:
        from playwright.async_api import async_playwright
        while True:
            try:
                async with async_playwright() as pw:
                    ctx, page = await self._open(pw, self.cfg.x_headless)
                    try:
                        while True:
                            reprise = quiet_until(self.cfg.x_quiet_hours)
                            if reprise:
                                # Pause de nuit : un compte actif 24 h/24 ressemble à un bot
                                log.info("Veille X en pause jusqu'à %s (heure de Paris)", reprise.strftime("%H:%M"))
                                await asyncio.sleep((reprise - datetime.now(PARIS)).total_seconds()
                                                    + random.uniform(0, 900))
                            await self.cycle(page)
                            pause = self.cfg.x_poll_seconds * random.uniform(0.8, 1.5)
                            if getattr(self.cfg, "x_light", False):
                                # Petit serveur : navigateur fermé pendant la pause (~400 Mo rendus au système).
                                # Vu en vrai sur 1 Go : swap sur disque lent, radar figé, PumpPortal et Telegram
                                # coupés toutes les 5 min.
                                await ctx.close()
                                await asyncio.sleep(pause)
                                ctx, page = await self._open(pw, self.cfg.x_headless)
                            else:
                                await asyncio.sleep(pause)
                    finally:
                        try:
                            await ctx.close()
                        except Exception:
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Veille X : navigateur relancé après erreur (%s : %s)", type(e).__name__, e)
                await asyncio.sleep(90)

    async def _goto(self, page, url: str) -> bool:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        if any(k in page.url for k in ("/login", "/i/flow", "/i/jf/", "onboarding", "/logout")):
            if not self._warned and self.on_problem:
                self.on_problem("⚠️ Veille X : la session X est déconnectée. Sur le PC, double-clique sur "
                                "<code>connexion_x.bat</code>. Si le radar tourne sur un serveur : exporte ensuite la "
                                "session et reprends-la sur le serveur (HEBERGEMENT.md, « Veille X refusée »).")
            self._warned = True
            return False
        self._warned = False
        return True

    async def _tweets(self, page) -> list[dict]:
        try:
            # Petit serveur : la page peut mettre bien plus de 20 s à s'afficher
            await page.wait_for_selector('article[data-testid="tweet"]',
                                         timeout=45000 if getattr(self.cfg, "x_light", False) else 20000)
        except Exception:
            return []
        seen: dict[str, dict] = {}
        for _ in range(3):
            for t in await page.evaluate(EXTRACT_JS):
                if t.get("url") and "/status/" in t["url"]:
                    t["handle"] = handle_from_url(t["url"])
                    seen[t["url"]] = t
            await page.mouse.wheel(0, random.randint(1800, 2600))
            await asyncio.sleep(random.uniform(1.5, 3.0))
        return list(seen.values())

    async def search(self, page, query: str, since: bool = True, latest: bool = True) -> list[dict]:
        if since:
            query += " since:" + (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        url = f"https://x.com/search?q={quote(query)}&src=typed_query" + ("&f=live" if latest else "")
        if not await self._goto(page, url):
            return []
        return await self._tweets(page)

    async def timeline(self, page, handle: str) -> list[dict]:
        if not await self._goto(page, f"https://x.com/{handle}"):
            return []
        return await self._tweets(page)

    async def read_profile(self, page, handle: str) -> dict | None:
        if not await self._goto(page, f"https://x.com/{handle}"):
            return None
        try:
            await page.wait_for_selector('[data-testid="UserName"]', timeout=35000 if getattr(self.cfg, "x_light", False) else 15000)
        except Exception:
            return None
        await asyncio.sleep(random.uniform(1, 2))
        raw = await page.evaluate(PROFILE_JS)
        links = await page.evaluate(
            "() => Array.from(document.querySelectorAll('[data-testid=\"UserUrl\"], [data-testid=\"UserDescription\"] a'))"
            ".map(a => (a.innerText || '') + ' ' + (a.href || ''))")
        prof = {"handle": handle, "followers": parse_count(raw.get("followers", "")),
                "following": parse_count(raw.get("following", "")),
                "joined": parse_joined(raw.get("joined", "")), "verified": raw.get("verified"),
                "verified_type": raw.get("verified_type"),
                "bio": raw.get("bio", "")[:500], "links": links[:10]}
        prof.update({k: v for k, v in (await self.read_about(page, handle)).items() if v is not None})
        return prof

    async def read_about(self, page, handle: str) -> dict:
        """« À propos de ce compte » : changements de nom d'utilisateur (compte racheté ?), pays, inscription."""
        await asyncio.sleep(random.uniform(2, 4))
        try:
            if not await self._goto(page, f"https://x.com/{handle}/about"):
                return {}
            await asyncio.sleep(random.uniform(2, 3))
            text = await page.evaluate("() => (document.querySelector('main') || document.body).innerText")
        except Exception as e:
            log.debug("Page À propos de @%s illisible : %s", handle, e)
            return {}
        out = parse_about(text or "")
        if not out:
            log.info("Page À propos de @%s : rien reconnu (libellés X changés ?) : %r", handle, (text or "")[:300])
        return out

    def _jobs(self) -> list[tuple[str, str]]:
        """Pages à lire ce tour-ci. Les actions choisies par l'IA remplacent des recherches fixes :
        le nombre de pages lues par tour reste le même (rythme anti-ban inchangé)."""
        extra = self.extra_jobs[:2]
        self.extra_jobs = []
        jobs = [("search", q) for q in random.sample(QUERIES, len(QUERIES) - len(extra))] + extra
        random.shuffle(jobs)
        return jobs + [("timeline", a) for a in self.cfg.x_accounts]

    async def cycle(self, page) -> None:
        for kind, arg in self._jobs():
            try:
                tweets = await (self.search(page, arg) if kind == "search" else self.timeline(page, arg))
                log.info("X %s « %s » : %d tweets", kind, arg, len(tweets))
                if tweets:
                    await self.on_tweets(tweets)
            except Exception as e:
                log.warning("X %s « %s » : %s", kind, arg, e)
            # Demandes de la chasse au dev (4 max par pause, pour rester discret)
            for _ in range(4):
                if self.profile_requests.empty():
                    break
                req, fut = self.profile_requests.get_nowait()
                if fut.done():
                    continue
                kind, handle = req.split(":", 1)
                try:
                    if kind == "timeline":
                        fut.set_result(await self.timeline(page, handle))
                    elif kind == "deepsearch":
                        fut.set_result(await self.search(page, handle, since=False, latest=False))
                    else:
                        fut.set_result(await self.read_profile(page, handle))
                except Exception as e:
                    fut.set_result(None)
                    log.warning("X %s @%s : %s", kind, handle, e)
                await asyncio.sleep(random.uniform(8, 15))
            await asyncio.sleep(random.uniform(15, 40))


# ---------------------------------------------------------------------------
async def login(cfg: cfgmod.Config) -> bool:
    """Ouvre X ; l'utilisateur se connecte lui-même. Détecte la connexion et ferme la fenêtre."""
    from playwright.async_api import async_playwright
    w = XWatcher(cfg, None)  # type: ignore[arg-type]
    marker = cfg.x_profile_dir / SESSION_MARKER
    if cfg.x_profile_dir.exists() and not marker.exists() and any(cfg.x_profile_dir.iterdir()):
        # Profil copié d'un autre PC ou connexion ratée : on repart d'un profil propre (l'ancien est gardé à côté)
        ancien = cfg.x_profile_dir.with_name(f"{cfg.x_profile_dir.name}_ancien_{datetime.now():%Y%m%d_%H%M%S}")
        cfg.x_profile_dir.rename(ancien)
        print(f"(ancien profil navigateur mis de côté : {ancien.name} — tu peux le supprimer)")
    async with async_playwright() as pw:
        ctx, page = await w._open(pw, headless=False)
        await page.goto("https://x.com/login")
        print("➡️  Connecte-toi à X dans la fenêtre qui vient de s'ouvrir (compte secondaire conseillé).")
        print("    Le programme détecte la connexion tout seul et ferme la fenêtre.")
        ok = False
        for _ in range(LOGIN_TIMEOUT_S // 2):
            try:
                cookies = await ctx.cookies("https://x.com")
            except Exception:
                break  # fenêtre fermée à la main
            if any(c["name"] == "auth_token" and c["value"] for c in cookies):
                ok = True
                break
            await asyncio.sleep(2)
        if ok:
            await asyncio.sleep(5)  # laisse le navigateur enregistrer la session sur le disque
            marker.write_text(json.dumps({"connecte_le": datetime.now(PARIS).isoformat(timespec="seconds")}),
                              encoding="utf-8")
        try:
            await ctx.close()
        except Exception:
            pass
    if ok:
        print("✅ Session X enregistrée. La veille X démarrera avec le radar.")
    else:
        print("❌ Connexion X non détectée (fenêtre fermée trop tôt ?). Relance connexion_x.bat.")
    return ok


async def _login() -> int:
    return 0 if await login(cfgmod.load()) else 1


def _export_path(cfg: cfgmod.Config) -> Path:
    return cfg.x_profile_dir.parent / EXPORT_NAME


async def export_session(cfg: cfgmod.Config) -> bool:
    """Sur le PC : copie les cookies X du profil connecté dans un fichier (pour un serveur Linux, où les cookies
    chiffrés par Windows du profil Edge sont illisibles)."""
    from playwright.async_api import async_playwright
    if not has_session(cfg.x_profile_dir):
        print("❌ Pas de session X sur ce PC : lance d'abord connexion_x.bat.")
        return False
    w = XWatcher(cfg, None)  # type: ignore[arg-type]
    async with async_playwright() as pw:
        ctx, _page = await w._open(pw, headless=True)
        cookies = await ctx.cookies(["https://x.com", "https://twitter.com"])
        await ctx.close()
    if not any(c["name"] == "auth_token" and c["value"] for c in cookies):
        print("❌ Session X expirée : relance connexion_x.bat puis recommence.")
        return False
    out = _export_path(cfg)
    out.write_text(json.dumps({"exporte_le": datetime.now(PARIS).isoformat(timespec="seconds"), "cookies": cookies}),
                   encoding="utf-8")
    print(f"✅ Session X exportée : {out}")
    print("⚠️  Ce fichier vaut un mot de passe : copie-le sur TON serveur (scp), puis supprime-le des deux côtés.")
    return True


async def import_session(cfg: cfgmod.Config) -> bool:
    """Sur le serveur : charge les cookies exportés dans le profil du navigateur et vérifie la connexion."""
    from playwright.async_api import async_playwright
    src = _export_path(cfg)
    if not src.exists():
        print(f"❌ {src} introuvable : exporte la session sur le PC puis copie le fichier ici.")
        return False
    cookies = json.loads(src.read_text(encoding="utf-8")).get("cookies") or []
    w = XWatcher(cfg, None)  # type: ignore[arg-type]
    async with async_playwright() as pw:
        ctx, page = await w._open(pw, headless=True)
        await ctx.add_cookies(cookies)
        await page.goto("https://x.com/home", wait_until="domcontentloaded")
        await asyncio.sleep(6)
        ok = any(c["name"] == "auth_token" and c["value"] for c in await ctx.cookies("https://x.com")) \
            and "/login" not in page.url and "/i/flow" not in page.url
        await ctx.close()
    if not ok:
        print("❌ X refuse la session sur ce serveur (déconnexion ou vérification demandée). Reconnecte-toi sur le "
              "PC, réexporte, et réessaie ; sinon garde la veille X sur le PC (X_ENABLED=0 ici).")
        return False
    (cfg.x_profile_dir / SESSION_MARKER).write_text(
        json.dumps({"connecte_le": datetime.now(PARIS).isoformat(timespec="seconds"), "importe": True}),
        encoding="utf-8")
    print("✅ Session X reprise sur le serveur. Supprime maintenant le fichier exporté :", src)
    return True


async def _test() -> int:
    from playwright.async_api import async_playwright

    from ..analysis.xparse import parse_tweet
    cfg = cfgmod.load()
    found: list[dict] = []

    async def collect(tweets: list[dict]) -> None:
        found.extend(tweets)

    w = XWatcher(cfg, collect, lambda m: print(re.sub("<[^>]+>", "", m)))
    async with async_playwright() as pw:
        ctx, page = await w._open(pw, cfg.x_headless)
        for q in QUERIES[:2]:
            await collect(await w.search(page, q))
        prof = await w.read_profile(page, cfg.x_accounts[0]) if cfg.x_accounts else None
        await ctx.close()
    print(f"\n{len(found)} tweets lus.")
    for t in found:
        dt = datetime.fromisoformat(t["time"].replace("Z", "+00:00")) if t.get("time") else None
        info = parse_tweet(t["text"], dt, t.get("links"))
        if info.is_candidate:
            when = datetime.fromtimestamp(info.launch_ts, timezone.utc).strftime("%d/%m %H:%M UTC") if info.launch_ts else "?"
            print(f"• @{t['handle']} {info.tickers} CA={info.cas[:1]} heure={when} {info.platform or ''} "
                  f"{'🚩' + ','.join(info.scam) if info.scam else ''}\n  {t['url']}")
    if prof:
        print("\nProfil test :", prof)
    return 0


async def _profil(handle: str) -> int:
    """Affiche ce que le radar lit d'un compte X et sa note de fiabilité."""
    from playwright.async_api import async_playwright

    from ..analysis.xlinks import account_trust
    cfg = cfgmod.load()
    w = XWatcher(cfg, None, lambda m: print(re.sub("<[^>]+>", "", m)))  # type: ignore[arg-type]
    async with async_playwright() as pw:
        ctx, page = await w._open(pw, cfg.x_headless)
        prof = await w.read_profile(page, handle.lstrip("@"))
        await ctx.close()
    if not prof:
        print("Profil illisible (session X déconnectée ?).")
        return 1
    for k, v in prof.items():
        print(f"  {k:18} {v}")
    t = account_trust(prof)
    print(f"\nFiabilité : {t.icon} {t.level} ({t.score}/100)")
    for p in t.plus:
        print("  +", p)
    for f in t.flags:
        print("  🚩", f)
    return 0


def main() -> int:
    cfgmod.setup_logging("x_watch")
    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"
    if cmd == "profil" and len(sys.argv) > 2:
        return asyncio.run(_profil(sys.argv[2]))
    if cmd in ("exporter", "importer"):
        fn = export_session if cmd == "exporter" else import_session
        return 0 if asyncio.run(fn(cfgmod.load())) else 1
    return asyncio.run(_login() if cmd == "login" else _test())


if __name__ == "__main__":
    sys.exit(main())
