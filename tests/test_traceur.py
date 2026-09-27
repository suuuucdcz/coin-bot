"""Traceur : remontée du financement, et le leurre anti-traceur vu en vrai sur $WAIF (réseau Reserve)."""
import asyncio

from conftest import make_tx

from radar.analysis.tracer import Tracer

DEV = "C94Xdevdddddddddddddddddddddddddddddddddd"
RELAIS = "77zDrelaisrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
BANK = "GVXPbankbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


class FakeRPC:
    """Historique du dev : 0,01 SOL d'un relais, puis 200 SOL du bank 30 s après, puis la création du token."""
    def __init__(self, gros=200.0, ecart=30):
        self.txs = {
            "s1": make_tx([(RELAIS, True, 1, 0.989995), (DEV, False, 0, 0.01)], []),
            "s2": make_tx([(BANK, True, 500, 500 - gros - 0.000005), (DEV, False, 0.01, 0.01 + gros)], []),
            "s3": make_tx([(DEV, True, 200, 199), ("MINTxxx", False, 0, 0.002)], []),
        }
        self.sigs = [{"signature": "s3", "blockTime": 1000 + ecart + 200, "err": None},
                     {"signature": "s2", "blockTime": 1000 + ecart, "err": None},
                     {"signature": "s1", "blockTime": 1000, "err": None}]

    async def all_signatures(self, address, max_pages=10):
        return (self.sigs, False) if address == DEV else ([], False)

    async def transaction(self, sig):
        return self.txs.get(sig)


def test_leurre_ecarte_le_vrai_financement_est_suivi():
    funding, nb, _ = asyncio.run(Tracer(FakeRPC(), 1000).first_funding(DEV))
    assert funding["source"] == BANK and funding["amount"] == 200
    assert funding["leurre"] == {"source": RELAIS, "amount": 0.01}


def test_petit_financement_sans_suite_reste_le_financement():
    # Pas de gros envoi dans l'heure : le petit financement est gardé (pas de faux « leurre »)
    funding, _nb, _ = asyncio.run(Tracer(FakeRPC(gros=0.02), 1000).first_funding(DEV))
    assert funding["source"] == RELAIS and "leurre" not in funding


def test_gros_envoi_bien_plus_tard_n_est_pas_le_financement_d_origine():
    funding, _nb, _ = asyncio.run(Tracer(FakeRPC(ecart=5 * 3600), 1000).first_funding(DEV))
    assert funding["source"] == RELAIS
