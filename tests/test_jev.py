"""Jev (TypeSafe) : lecture des réponses au format de la doc, et radar inchangé sans clé ou en cas de panne."""
import asyncio

from radar.analysis.jev import Jev


class FakeResponse:
    def __init__(self, status, data):
        self.status, self._data = status, data

    async def json(self, content_type=None):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeHTTP:
    def __init__(self, status, data):
        self.status, self.data, self.bodies = status, data, []

    def post(self, url, json=None, headers=None, timeout=None):
        self.bodies.append((url, json, headers))
        return FakeResponse(self.status, self.data)


REPONSE = {  # format de https://docs.typesafe.ai/api.md
    "model": "jev-1.13.0",
    "answers": {
        "type": {"type": "choice", "choice": "arnaque", "confidence": 0.9,
                 "probabilities": {"annonce_projet": 0.05, "promo_tiers": 0.03, "arnaque": 0.9, "autre": 0.02}},
        "officiel": {"type": "noul", "noul": 0.1},
    },
    "usage": {"input_tokens": 120, "output_tokens": 9},
}


def test_lecture_d_une_reponse():
    http = FakeHTTP(200, REPONSE)
    ai = asyncio.run(Jev("cle", http).classify_tweet("Airdrop to first 2500, drop your SOL address", "scam"))
    assert ai == {"type": "arnaque", "p_type": 0.9, "confiance": 0.9, "p_officiel": 0.1}
    url, body, headers = http.bodies[0]
    assert url.endswith("/v1/systemone") and body["model"] == "jev-latest" and headers["Authorization"] == "Bearer cle"


def test_sans_cle_desactive():
    assert asyncio.run(Jev("", FakeHTTP(200, REPONSE)).classify_tweet("x", "y")) is None


def test_cle_refusee_met_en_pause():
    jev = Jev("mauvaise", FakeHTTP(401, {}))
    assert asyncio.run(jev.classify_tweet("x", "y")) is None and not jev.enabled
