"""Jev (TypeSafe AI, modèle « System One ») : décisions rapides avec probabilité calibrée. OPTIONNEL.

https://docs.typesafe.ai — POST https://api.typesafe.ai/v1/systemone, clé TYPESAFE_API_KEY (accès anticipé).
Prix : ~0,04 $ par million de tokens en entrée (un tweet ≈ 100 tokens) : quasi gratuit.

Rôle dans le radar : un AVIS EN PLUS des règles, jamais le seul juge.
  - Jev n'est pas conçu pour du contenu hostile (doc TypeSafe « jaggedness ») : un tweet d'arnaque
    est justement écrit pour tromper. Ses réponses ajoutent des drapeaux ou départagent des comptes,
    elles ne confirment jamais seules un token.
  - Les dates, heures, montants et comptages restent calculés par le code (Jev n'est pas fiable là-dessus).
Sans clé, tout est désactivé et le radar fonctionne exactement pareil.
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

log = logging.getLogger("jev")

API = "https://api.typesafe.ai/v1/systemone"
MODEL = "jev-latest"
TIMEOUT_S = 4
PAUSE_AFTER_AUTH_ERROR_S = 3600

TWEET_QUESTIONS = {
    "type": {
        "type": "choice",
        "instructions": "What is this tweet, written by the author shown in the state, doing about a crypto token?",
        "criteria": {
            "annonce_projet": {
                "what": "The token's own team announces its launch, launch time or contract address (we / our / official)",
                "not_for": "Someone promoting a token that is not theirs",
            },
            "promo_tiers": {
                "what": "A third party (caller, influencer, KOL, alpha group, bot) promotes, shills or reposts a token",
                "not_for": "The project team talking about its own token",
            },
            "arnaque": {
                "what": "Giveaway or airdrop bait, presale, 'send SOL', 'drop your wallet', guaranteed gains, "
                        "phishing link, contract only given in DM or Telegram",
            },
            "autre": "Market commentary, memes, news or anything that is not a token launch",
        },
    },
    "officiel": {
        "type": "noul",
        "instructions": "Is the author the official account of the token being announced?",
        "criteria": {"true": "Speaks as the project itself, account named after the token",
                     "false": "Speaks about someone else's project, or is a general crypto account"},
    },
}

ACCOUNT_QUESTIONS = {
    "role": {
        "type": "choice",
        "instructions": "Based only on this X profile, what kind of account is it?",
        "criteria": {
            "projet": "Official account of one crypto project or token",
            "dev": "Personal account of a developer or founder",
            "caller": "Crypto caller, influencer, KOL or alpha group promoting many different tokens",
            "celebrite": "Celebrity, media, big company or brand, not about a single memecoin",
            "bot": "Automated, spam or giveaway account",
            "autre": "Anything else",
        },
    },
}


class Jev:
    def __init__(self, api_key: str, http: aiohttp.ClientSession | None):
        self.api_key = api_key
        self.http = http
        self._paused_until = 0.0
        self.calls = 0

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.http) and time.time() >= self._paused_until

    async def ask(self, state: dict | str, questions: dict) -> dict | None:
        """Réponses brutes de Jev (None si désactivé, lent ou en erreur : le radar continue sans)."""
        if not self.enabled:
            return None
        body = {"model": MODEL, "state": state, "questions": questions}
        headers = {"Authorization": f"Bearer {self.api_key}"}
        for essai in range(2):
            try:
                async with self.http.post(API, json=body, headers=headers,
                                          timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)) as r:
                    if r.status == 401:
                        log.error("Jev : clé TYPESAFE_API_KEY refusée, IA en pause 1 h")
                        self._paused_until = time.time() + PAUSE_AFTER_AUTH_ERROR_S
                        return None
                    if r.status in (429, 529) and essai == 0:
                        await asyncio.sleep(1.5)
                        continue
                    if r.status != 200:
                        log.warning("Jev : HTTP %s", r.status)
                        return None
                    data = await r.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                log.debug("Jev indisponible : %s", e)
                return None
            self.calls += 1
            return data.get("answers") or None
        return None

    async def classify_tweet(self, text: str, handle: str | None) -> dict | None:
        """{type, p_type, confiance, p_officiel} ou None."""
        ans = await self.ask({"author": f"@{handle or '?'}", "tweet": (text or "")[:1200]}, TWEET_QUESTIONS)
        if not ans or "type" not in ans:
            return None
        t = ans["type"]
        choix = t.get("choice")
        return {"type": choix, "p_type": float((t.get("probabilities") or {}).get(choix, 0)),
                "confiance": float(t.get("confidence") or 0),
                "p_officiel": float((ans.get("officiel") or {}).get("noul", 0.5))}

    async def classify_account(self, prof: dict) -> dict | None:
        """{role, p_role, confiance} à partir de la bio et des chiffres du profil, ou None."""
        state = {k: prof.get(k) for k in ("handle", "bio", "followers", "following", "joined", "links")}
        ans = await self.ask(state, ACCOUNT_QUESTIONS)
        if not ans or "role" not in ans:
            return None
        r = ans["role"]
        choix = r.get("choice")
        return {"role": choix, "p_role": float((r.get("probabilities") or {}).get(choix, 0)),
                "confiance": float(r.get("confidence") or 0)}


ROLE_LABELS = {"projet": "compte d'un projet", "dev": "compte perso d'un dev", "caller": "caller / influenceur",
               "celebrite": "célébrité / marque", "bot": "bot / spam", "autre": "autre"}
