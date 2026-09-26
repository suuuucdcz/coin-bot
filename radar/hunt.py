"""🕵️ Chasse au dev : trouver le wallet du dev AVANT que le CA soit publié.

Idée : un dev crée souvent son coin quelques secondes/minutes avant de poster le CA.
Si son wallet est déjà sous surveillance, l'alerte « création » part avant le call public.

Sources d'indices (du plus fiable au moins fiable) :
  1. anciens coins du compte X : CA publiés dans ses tweets -> créateur de ces coins ;
  2. DexScreener : tokens au même ticker dont le compte X est celui de l'annonce -> créateur ;
  3. adresses de wallet publiées dans la bio X, les tweets ou le canal Telegram public du projet.
"""
from __future__ import annotations

import asyncio
import html
import logging
import re
from dataclasses import dataclass
from urllib.parse import quote, urlparse

import aiohttp

from .analysis.enrich import token_info, x_handle
from .analysis.xparse import CA_RE, is_solana_address
from .devs import find_deployer
from .sources.helius import SYSTEM_PROGRAM

log = logging.getLogger("hunt")

TG_RE = re.compile(r"(?:t\.me|telegram\.me)/(?!joinchat|\+|s/)([A-Za-z0-9_]{4,32})", re.I)
MENTION_RE = re.compile(r"(?<![\w@])@([A-Za-z0-9_]{3,15})")
URL_RE = re.compile(r"https?://[^\s\"'<>)]+")
# Comptes cités partout, jamais « le dev »
BIG_ACCOUNTS = {"solana", "pumpdotfun", "pumpfun", "raydiumprotocol", "meteoraag", "jupiterexchange",
                "dexscreener", "phantom", "binance", "coinbase", "elonmusk", "letsbonkfun", "believeapp"}
MAX_ADDRESSES = 25


SOCIAL_HOSTS = ("x.com", "twitter.com", "t.me", "telegram.me", "pump.fun", "dexscreener.com", "solscan.io",
                "birdeye.so", "discord.gg", "discord.com", "youtube.com", "instagram.com", "tiktok.com")


def _is_site(url: str, allow_tco: bool = False) -> bool:
    host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    if host == "t.co":
        return allow_tco
    return bool(host) and not any(host == h or host.endswith("." + h) for h in SOCIAL_HOSTS)


async def _fetch(http: aiohttp.ClientSession, url: str) -> str:
    """Texte brut d'une page web (site du projet), 300 ko max."""
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=10),
                            headers={"User-Agent": "Mozilla/5.0"}) as r:
            if r.status != 200 or "html" not in r.headers.get("Content-Type", "html"):
                return ""
            raw = (await r.content.read(300_000)).decode("utf-8", "ignore")
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return ""
    return html.unescape(re.sub(r"<[^>]+>", " ", raw)) + " " + " ".join(re.findall(r'href="([^"]+)"', raw))


@dataclass
class Candidate:
    address: str
    reason: str
    score: int          # 3 = fort (créateur d'un ancien coin du même compte), 2 = adresse publiée


def _addresses(texts: list[str]) -> list[str]:
    out: list[str] = []
    for t in texts:
        for a in CA_RE.findall(t or ""):
            if a not in out and is_solana_address(a):
                out.append(a)
    return out[:MAX_ADDRESSES]


async def _telegram_texts(http: aiohttp.ClientSession, channel: str) -> list[str]:
    """Aperçu web public d'un canal Telegram (t.me/s/<canal>), sans compte."""
    try:
        async with http.get(f"https://t.me/s/{channel}", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return []
            page = await r.text()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return []
    msgs = re.findall(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', page, re.S)
    return [html.unescape(re.sub(r"<[^>]+>", " ", m)) for m in msgs[-40:]]


async def hunt(agenda, ann_id: int) -> list[Candidate]:
    """Pistes de dev pour un coin annoncé sans CA.

    Règle des liens : seules les sources contrôlées par le compte officiel comptent (ses tweets, sa bio,
    le site et le Telegram liés dans SA bio). Les tweets des autres comptes (callers, copieurs) peuvent
    citer n'importe quel CA : ils ne produisent jamais de piste. Un ancien token n'est attribué au compte
    que si le lien existe dans les deux sens (le compte publie le CA ET le token renvoie vers le compte).
    """
    p = agenda.p
    row = agenda.db.announcement(ann_id)
    if not row:
        return []
    handle = (agenda._official(row)[0] or row["handle"] or "").lstrip("@")
    ticker = row["ticker"]
    own: list[str] = [row["tweet_text"] or ""] if (row["handle"] or "").lower() == handle.lower() else []
    found: dict[str, Candidate] = {}

    def add(addr: str, reason: str, score: int) -> None:
        if addr and (addr not in found or found[addr].score < score):
            found[addr] = Candidate(addr, reason, score)

    # 1. Compte officiel : bio, liens, derniers tweets, historique ; comptes désignés dans sa bio
    tg_channels: set[str] = set(TG_RE.findall(" ".join(own)))
    websites: set[str] = set()
    if agenda.xw and handle and handle != "?":
        accounts = [handle]
        prof = await agenda.xw.profile(handle) or {}
        bio_texts = [prof.get("bio", "")] + prof.get("links", [])
        own += bio_texts
        # Comptes cités dans la bio (« dev: @xxx », « founder @yyy ») : souvent le compte perso du dev
        for m in MENTION_RE.findall(" ".join(bio_texts)):
            if m.lower() not in BIG_ACCOUNTS and m.lower() != handle.lower() and m not in accounts:
                accounts.append(m)
        for acc in accounts[:3]:
            batches = [await agenda.xw.user_tweets(acc) or [],
                       await agenda.xw.deep_search(f"from:{acc} (pump OR CA OR contract OR solscan OR dexscreener OR wallet)") or []]
            for t in (t for b in batches for t in b):
                if (t.get("handle") or acc).lower() != acc.lower():
                    continue  # retweet / citation d'un autre compte
                own.append(t.get("text", ""))
                own += t.get("links", [])
        tg_channels |= set(TG_RE.findall(" ".join(own)))
        # Site du projet : lien du profil X (souvent raccourci en t.co, suivi automatiquement)
        websites |= {u for u in URL_RE.findall(" ".join(bio_texts)) if _is_site(u, allow_tco=True)}

    # 2. Canal Telegram public lié par le compte officiel
    for ch in list(tg_channels)[:2]:
        own += await _telegram_texts(p.http, ch)

    # 3. Site web lié dans la bio (adresses, liens Telegram)
    for url in list(websites)[:2]:
        page = await _fetch(p.http, url)
        own.append(page)
        for ch in TG_RE.findall(page):
            if ch not in tg_channels:
                tg_channels.add(ch)
                own += await _telegram_texts(p.http, ch)

    # Adresses publiées par le compte officiel : ancien coin (-> son créateur) ou wallet publié
    for addr in _addresses(own):
        if addr == row["ca"]:
            continue
        try:
            mi = await p.rpc.mint_info(addr)
            if mi:
                info = await token_info(p.rpc, p.http, addr, with_dev_history=False)
                creator = info.creator or await find_deployer(p.rpc, addr)
                if not creator:
                    continue
                if (x_handle(info.twitter) or "").lower() == handle.lower():
                    add(creator, f"créateur de ${info.symbol or '?'} : CA publié par @{handle} "
                                 f"et son token renvoie vers @{handle}", 3)
                else:
                    add(creator, f"créateur de ${info.symbol or '?'} (CA cité par @{handle}, "
                                 "mais le token ne renvoie pas vers lui : peut-être un coin qu'il a seulement relayé)", 1)
            else:
                acc = await p.rpc.account_info(addr)
                if acc is None or acc.get("owner") == SYSTEM_PROGRAM:
                    add(addr, f"adresse publiée par @{handle} (bio / tweet / Telegram / site)", 2)
        except Exception as e:
            log.debug("adresse %s : %s", addr[:6], e)

    # 4. DexScreener : tokens au même ticker dont la fiche renvoie vers le PROFIL du compte officiel
    #    (un lien vers un tweet ne compte pas ; et même un profil se copie : piste moyenne)
    if ticker and handle:
        try:
            async with p.http.get(f"https://api.dexscreener.com/latest/dex/search?q={quote(ticker)}",
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                pairs = (await r.json(content_type=None)).get("pairs") or []
        except Exception:
            pairs = []
        seen: set[str] = set()
        for pr in pairs:
            base = pr.get("baseToken") or {}
            mint = base.get("address")
            socials = {s.get("type"): s.get("url") for s in (pr.get("info") or {}).get("socials") or []}
            if pr.get("chainId") != "solana" or mint in seen or (x_handle(socials.get("twitter")) or "").lower() != handle.lower():
                continue
            seen.add(mint)
            info = await token_info(p.rpc, p.http, mint, with_dev_history=False)
            creator = info.creator or await find_deployer(p.rpc, mint)
            if creator:
                add(creator, f"créateur de ${base.get('symbol')} (sa fiche DexScreener renvoie vers @{handle})", 2)

    # Les pistes faibles (score 1) ne sont pas surveillées comme « dev » : trop de faux positifs
    cands = sorted((c for c in found.values() if c.score >= 2), key=lambda c: -c.score)
    log.info("Chasse au dev $%s (@%s) : %d piste(s), %d écartée(s) car trop faibles",
             ticker, handle, len(cands), len(found) - len(cands))
    return cands
