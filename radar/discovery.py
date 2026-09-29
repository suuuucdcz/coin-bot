"""🧭 Découverte automatique de devs : la watchlist se remplit toute seule.

Toutes les DISCOVERY_EVERY_H heures :
  1. on lit les tokens pump.fun qui ont migré (complete=true) créés ces 3 derniers jours ;
  2. on garde ceux dont l'ATH dépasse DISCOVERY_MIN_ATH (des lancements qui ont vraiment marché) ;
  3. on écarte les créateurs « machines à lancer » (beaucoup de tokens, presque aucun succès) ;
  4. le créateur entre dans la watchlist (groupe « découverte »), ainsi que le wallet qui l'a financé
     (le « bank » probable, qui financera le prochain wallet dev).

Pourquoi : un dev qui a réussi un lancement recommence souvent avec un wallet neuf financé par le même
bank. Le radar alerte alors dès le funding du nouveau wallet, puis à la création du token.

Commande de test (n'ajoute rien, affiche seulement) :
    python -m radar.discovery
Revérifier les anciennes découvertes avec les règles actuelles (modifie la base) :
    python -m radar.discovery --revalider
"""
from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass, field

from . import alerts as A
from .analysis.tracer import Tracer
from .sources import dexscreener, pumpfun
from .telegram import esc

log = logging.getLogger("discovery")

GROUP = "découverte"
LOOKBACK_S = 7 * 86400
# Un « succès » doit avoir TENU : vu en vrai, $TRUDY (3,4 M$) et $TJR (2,2 M$), retenus au sommet quelques heures
# après leur création, ont été vidés par leur dev 5 et 21 minutes plus tard (−100 %).
MIN_SUCCESS_AGE_S = 24 * 3600
MIN_HELD_RATIO = 0.25       # la MC actuelle vaut encore au moins 25 % de l'ATH
PAGES = 4                   # 4 × 50 tokens par tri
MAX_NEW_PER_RUN = 20        # jusqu'à 20 devs à succès ajoutés par passage (toutes les 3 h)
MAX_EVALUATED_PER_RUN = 20  # quota Helius : ~50 crédits par dev évalué (vu le 29/09 : 1,6 M/mois projeté)
MIN_LIQUIDITY_USD = 25_000  # un succès = un vrai marché encore liquide
# Anti-manipulation (vu en vrai : faux « fonds souverains » du cluster Reserve affichés à 4 000 M$ de MC avec
# 30 k$ de liquidité). Un vrai memecoin a une liquidité d'au moins ~2 % de sa MC et des centaines de trades.
MIN_LIQ_MC_RATIO = 0.02
MIN_TXNS_24H = 300
MAX_YOUNG_MC = 1_000_000_000
SPAM_MIN_COINS = 20         # au-delà, un créateur avec moins de 10 % de succès = machine à lancer
SPAM_MAX_HIT_RATE = 0.10
RECHECK_S = 7 * 86400       # un créateur écarté n'est réévalué qu'une fois par semaine
# Un dev de memecoin normal démarre avec quelques SOL. Des centaines de SOL d'un coup = schéma des faux
# « fonds souverains » du cluster Reserve (grosse liquidité pour paraître sérieux, puis retrait).
BIG_FUNDING_SOL = 100
SUSPECT_GROUP = "reserve-suspect"   # doit figurer dans RUG_GROUPS (c'est le cas par défaut)
NETWORK_RUG_GROUP = "reseau-rugs"   # idem


@dataclass
class Found:
    creator: str
    symbol: str
    mint: str
    ath: float
    coins: int
    hits: int
    funder: str | None = None
    funder_note: str = ""
    added: bool = False
    reason: str = ""
    rug_group: str | None = None     # dev relié à un cluster de rugs connu (surveillé, alertes ⛔)
    upstream: list[str] = field(default_factory=list)   # financeurs non-exchange en amont
    created: int = 0                 # création du token (la découverte regarde des tokens de 1 à 7 jours)


def suspect_market(m: dict, min_mc: float) -> str | None:
    """Raison de rejeter un marché (None = vrai succès vérifiable)."""
    mc, liq, txns = m.get("mc") or 0, m.get("liquidity") or 0, m.get("txns24h") or 0
    if mc < min_mc:
        return "MC trop faible"
    if liq < MIN_LIQUIDITY_USD:
        return "liquidité trop faible"
    if mc > MAX_YOUNG_MC or liq / mc < MIN_LIQ_MC_RATIO:
        return f"MC gonflée : {mc / 1e6:.0f} M$ pour {liq / 1e3:.0f} k$ de liquidité"
    if txns < MIN_TXNS_24H:
        return f"trop peu de trades ({txns} en 24 h)"
    return None


async def candidates(http, min_ath: float) -> list[dict]:
    """Tokens migrés récents qui ont VRAIMENT marché, du plus gros au plus petit.

    L'ATH de pump.fun n'est pas fiable (valeurs absurdes vues en vrai) : le succès est vérifié sur
    DexScreener, avec la market cap et la liquidité actuelles (un vrai marché, pas un chiffre d'API).
    """
    now = time.time()
    vus: dict[str, dict] = {}
    for sort in ("created_timestamp", "market_cap"):
        for page in range(PAGES):
            coins = await pumpfun.list_coins(http, sort=sort, complete=True, offset=page * 50)
            if not coins:
                break
            for c in coins:
                age = now - (c.get("created") or 0)
                if c.get("mint") and c.get("creator") and MIN_SUCCESS_AGE_S <= age < LOOKBACK_S:
                    vus[c["mint"]] = c
            if sort == "created_timestamp" and coins and now - (coins[-1].get("created") or now) > LOOKBACK_S:
                break  # on est déjà remonté au-delà de la fenêtre
    marches = await dexscreener.markets(http, list(vus))
    ok = []
    for mint, c in vus.items():
        m = marches.get(mint)
        if not m or suspect_market(m, min_ath) is not None:
            continue
        if c.get("ath") and (m["mc"] or 0) < MIN_HELD_RATIO * c["ath"]:
            continue  # retombé : pas un succès qui a tenu
        ok.append({**c, "ath": m["mc"], "liquidity": m["liquidity"]})
    return sorted(ok, key=lambda c: c.get("ath") or 0, reverse=True)


async def evaluate(pipeline, coin: dict, min_ath: float, ignorer: frozenset[str] = frozenset()) -> Found:
    """`ignorer` : wallets dont le groupe actuel ne compte pas comme preuve (le dev réévalué et son bank : sinon un
    ancien classement se confirme lui-même ; vu en vrai : $YAP « lié au cluster via » son propre wallet)."""
    creator = coin["creator"]
    history = await pumpfun.coins_by_creator(pipeline.http, creator) or []
    hits = sum(1 for c in history if (c.get("ath") or 0) >= min_ath / 2)
    f = Found(creator, coin.get("symbol") or "?", coin["mint"], coin.get("ath") or 0, len(history), hits,
              created=coin.get("created") or 0)
    if len(history) >= SPAM_MIN_COINS and hits / max(1, len(history)) < SPAM_MAX_HIT_RATE:
        f.reason = f"machine à lancer ({len(history)} tokens, {hits} succès)"
        return f
    # Réseau du dev (ses projets et ceux de ses wallets) : un « succès » au milieu de rugs = opérateur.
    # Vu en vrai : le dev de $DJT (20 M$) avait fait VSOF ×2, NTDA ×2 et AROS, tous à −99,99 %.
    # Il est suivi AVEC le groupe ⛔ : ses prochains tokens seront marqués à éviter.
    from .analysis import network
    try:
        rep = await network.quick(pipeline, creator, network.CHUTES_PAR_ANALYSE)
    except Exception:
        rep = None
    if rep is not None:
        f.upstream = [h["src"] for h in rep.funding_chain if not h.get("hot")]
        flag = network.quick_verdict(rep)
        if flag and flag.startswith(network.RELAIS_FLAG) and not await _ferme(pipeline, coin["mint"], creator):
            # Chaîne de relais au même montant SANS ferme de wallets dans le token : pas assez pour ⛔. Vu en vrai :
            # $YAP (5,7 M$, 14,5 M$ de vrai volume, détenteurs variés) classé arnaque pour 3,36 SOL relayés deux fois.
            f.funder_note = f"🟠 {flag}"
            flag = None
        if flag and not flag.startswith(network.LEURRE_FLAG):
            # Un petit envoi avant le vrai financement, seul, ne suffit pas : beaucoup de gens envoient d'abord
            # 0,01 SOL pour tester l'adresse. Il reste affiché (🟠) sur la fiche du réseau.
            # Chaîne de relais au même montant, réseau à rugs, relances du même nom… : suivi en ⛔
            f.rug_group = NETWORK_RUG_GROUP if "réseau à rugs" in flag else SUSPECT_GROUP
            f.reason = f"⛔ {flag}"
            return f
    known ={a: lab for a, lab in pipeline.labels.items() if "hot wallet" in lab.lower() or "exchange" in lab.lower()}
    tracer = Tracer(pipeline.rpc, pipeline.cfg.hot_wallet_tx_threshold, known)
    funding, _nb, _raison = await tracer.first_funding(creator)
    if funding:
        hot, info = await tracer.hot_check(funding["source"], funding["signature"])
        if hot:
            f.funder_note = f"financé par un exchange ({info or pipeline.labels.get(funding['source'], '')})"
            if not pipeline.db.get_label(funding["source"]):
                pipeline.db.set_label(funding["source"], f"hot wallet / service ({info})")
        else:
            f.funder = funding["source"]
            f.funder_note = f"financé par {A.short(f.funder)} ({funding['amount']:g} SOL)"
            # Lien d'argent avec un cluster de rugs connu : le financeur, ou le financeur du financeur
            chaine = [creator, f.funder]
            amont, _n, _r = await tracer.first_funding(f.funder)
            if amont:
                chaine.append(amont["source"])
            for a in chaine:
                if a in ignorer:
                    continue
                grp = pipeline.group(a)
                if grp and grp in pipeline.cfg.rug_groups:
                    f.rug_group = grp
                    f.reason = f"⛔ lié au cluster de rugs « {grp} » (via {A.short(a)})"
                    return f
            if funding["amount"] >= BIG_FUNDING_SOL:
                # Pas de lien prouvé avec un cluster connu, mais même schéma : surveillé comme suspect, avec
                # ses alertes marquées ⛔ (à signaler, pas à acheter)
                f.rug_group = SUSPECT_GROUP
                f.reason = (f"⛔ financement massif ({funding['amount']:,.0f} SOL) : schéma des faux fonds "
                            "souverains").replace(",", " ")
    return f


async def _ferme(pipeline, mint: str, creator: str) -> bool:
    """Le token a-t-il une ferme de wallets (gros détenteurs aux parts quasi identiques) ? Dans le doute : oui."""
    from .analysis.enrich import FERME_MIN, ferme
    from .smart import top_holders
    try:
        parts = dict(await top_holders(pipeline.rpc, mint, 20))
    except Exception:
        return True
    return ferme(parts, creator)[0] >= FERME_MIN


def _same_operator(db, f: Found) -> None:
    """Un même financeur derrière plusieurs devs « à succès » = usine à tokens (vu en vrai : EdcS… finançait
    USDFA, WOAR et JEANPHIL, chacun depuis un wallet neuf). Tous passent en suspects ⛔."""
    import json as _json
    for src in f.upstream:
        cle = f"disc_up:{src}"
        devs = _json.loads(db.get(cle) or "[]")
        if f.creator not in devs:
            devs.append(f.creator)
            db.put(cle, _json.dumps(devs[-20:]))
        if len(devs) >= 2:
            if not f.rug_group:
                f.rug_group = SUSPECT_GROUP
                f.reason = f"⛔ même opérateur : {len(devs)} devs « à succès » financés par {A.short(src)}"
            db.conn.executemany("UPDATE wallets SET grp=?, role=? WHERE address=? AND grp='découverte'",
                                [(SUSPECT_GROUP, f"dev reclassé : même opérateur que {len(devs)} autres ({src[:4]})", d)
                                 for d in devs])
            db.conn.commit()
            return


async def run_once(pipeline, dry_run: bool = False) -> list[Found]:
    cfg, db = pipeline.cfg, pipeline.db
    out: list[Found] = []
    for coin in await candidates(pipeline.http, cfg.discovery_min_ath):
        if len([f for f in out if f.added or dry_run]) >= MAX_NEW_PER_RUN or len(out) >= MAX_EVALUATED_PER_RUN:
            break
        creator = coin["creator"]
        last = db.get(f"disc:{creator}")
        if db.wallet(creator) or (last and time.time() - int(last) < RECHECK_S):
            continue
        try:
            f = await evaluate(pipeline, coin, cfg.discovery_min_ath)
        except Exception as e:
            log.warning("Découverte : évaluation impossible pour %s : %s", creator[:6], e)
            continue
        out.append(f)
        if dry_run:
            continue
        _same_operator(db, f)   # même si déjà suspect : ses « frères » (même financeur) le deviennent aussi
        db.put(f"disc:{creator}", int(time.time()))
        if f.rug_group:
            # Surveillé AVEC son cluster : ses prochaines alertes seront marquées ⛔ à éviter
            sym = (f.symbol or "?")[:10]
            f.added = await pipeline.watch(creator, f"DEV_{sym}_{creator[:4]}", f.rug_group,
                                           f"dev lié au cluster {f.rug_group} (découverte : ${sym})", 1, f.funder)
            if f.funder and not db.wallet(f.funder):
                await pipeline.watch(f.funder, f"BANK_{sym}_{f.funder[:4]}", f.rug_group,
                                     f"financeur lié au cluster {f.rug_group}", 1, None)
            continue
        if f.reason:
            continue
        sym = (f.symbol or "?")[:10]
        f.added = await pipeline.watch(creator, f"DEV_{sym}_{creator[:4]}", GROUP,
                                       f"dev (découverte : ${sym}, MC {A.usd(f.ath)} vérifiée DexScreener)", 0, None)
        if f.funder and not db.wallet(f.funder):
            await pipeline.watch(f.funder, f"BANK_{sym}_{f.funder[:4]}", GROUP,
                                 f"bank probable (a financé le dev de ${sym})", 1, creator)
    return out


def _depuis(f: Found) -> str:
    return A.age(int(time.time() - f.created)) if f.created else "?"


def report(found: list[Found]) -> str | None:
    added = [f for f in found if f.added and not f.rug_group]
    rugs = [f for f in found if f.added and f.rug_group]
    # L'âge du token est affiché : la découverte regarde des tokens lancés il y a 1 à 7 jours (vu en vrai : ces
    # listes donnaient l'impression que le radar analysait de vieux coins comme s'ils sortaient)
    lignes_rug = [f"⛔ <b>{len(rugs)} dev(s) du cluster de rugs repéré(s)</b> (surveillés, alertes marquées à éviter) : "
                  + ", ".join(f"${esc(f.symbol)} (lancé il y a {_depuis(f)})" for f in rugs)] if rugs else []
    if not added:
        return "\n".join(["🧭 <b>Découverte</b>"] + lignes_rug) if rugs else None
    lines = [f"🧭 <b>Découverte : {len(added)} dev(s) ajouté(s) à la surveillance</b>",
             "<i>Créateurs de tokens pump.fun lancés il y a 1 à 7 jours et qui ont vraiment tenu. "
             "Leur prochain lancement (nouveau wallet financé par le même bank) sera signalé.</i>"]
    for f in added:
        lines.append(f"• <b>${esc(f.symbol)}</b> MC {A.usd(f.ath)} · lancé il y a {_depuis(f)} · dev <code>{f.creator}</code> · "
                     f"{f.coins} token(s), {f.hits} succès" + (f"\n   ↳ {esc(f.funder_note)}" if f.funder_note else ""))
    return "\n".join(lines + lignes_rug)


async def revalider(pipeline) -> list[str]:
    """Anciennes découvertes (avant la vérification DexScreener et l'analyse de réseau : rôle « dev (découverte :
    $X ATH … M$) ») et devs reclassés ⛔ sur une seule règle faible, réévalués avec les règles actuelles. Vu en vrai : 19 devs du réseau Reserve y gardaient la
    confiance « prouvé » grâce à des ATH absurdes renvoyés par pump.fun (AROS « 482 M$ », XBC « 407 M$ »)."""
    cfg, db = pipeline.cfg, pipeline.db
    lignes = []
    anciens = db.conn.execute(
        "SELECT * FROM wallets WHERE (grp = ? AND role LIKE 'dev (découverte%' AND role NOT LIKE '%vérifiée DexScreener%')"
        # reclassés sur une règle revue le 28/09 : relais seuls, « 1 projet raté », et tout « réseau à rugs » (un projet
        # éteint doucement comptait comme un rug : seule une chute brutale en est un)
        " OR role LIKE 'dev reclassé : financement brouillé : chaîne de relais%'"
        " OR role LIKE 'dev reclassé : réseau à rugs%'"
        " OR role LIKE 'dev reclassé : lié au cluster%'", (GROUP,)).fetchall()
    for w in anciens:
        coins = await pumpfun.coins_by_creator(pipeline.http, w["address"]) or []
        coin = max(coins, key=lambda c: c.get("ath") or 0, default=None)
        if coin is None:
            continue
        sym = (coin.get("symbol") or "?")[:10]
        marche = (await dexscreener.markets(pipeline.http, [coin["mint"]])).get(coin["mint"])
        banks = [r["address"] for r in db.conn.execute(
            "SELECT address FROM wallets WHERE parent = ? AND (role LIKE 'bank probable%' OR role LIKE 'financeur lié%')",
            (w["address"],))]
        f = await evaluate(pipeline, {**coin, "creator": w["address"]}, cfg.discovery_min_ath,
                           frozenset([w["address"], *banks]))
        faux = f.reason or (suspect_market(marche, cfg.discovery_min_ath) if marche else "plus de marché")
        if f.rug_group:
            grp, role = f.rug_group, f"dev reclassé : {f.reason.removeprefix('⛔ ')}"
        elif faux:
            grp, role = GROUP, f"dev d'une ancienne découverte non confirmée (${sym} : {faux})"
        else:
            grp, role = GROUP, f"dev (découverte : ${sym}, MC {A.usd(marche['mc'])} vérifiée DexScreener)"
        db.set_wallet_role(w["address"], w["label"], grp, role, w["depth"])
        # Le bank du dev suit son classement (il avait été classé à cause du dev, ou avant la revérification)
        db.conn.executemany("UPDATE wallets SET grp = ? WHERE address = ?", [(grp, b) for b in banks])
        db.conn.commit()
        lignes.append(f"{w['label']} -> {grp} : {role}" + (f" (+ {len(banks)} bank)" if banks else ""))
    return lignes


# ---------------------------------------------------------------------------
def main() -> int:
    import asyncio
    import logging as lg

    import aiohttp

    from . import config as cfgmod
    from .db import DB
    from .pipeline import Pipeline
    from .sources.helius import SolanaRPC

    cfgmod.setup_logging("discovery", lg.INFO)
    cfg = cfgmod.load()

    async def go() -> int:
        db = DB(cfg.db_path)
        db.import_watchlist(cfg.watchlist_path)
        db.import_labels(cfg.labels_path)
        async with SolanaRPC(cfg.rpc_url) as rpc, aiohttp.ClientSession() as http:
            p = Pipeline(cfg, db, rpc, http, None, None, dry_run=True)
            if "--revalider" in sys.argv:
                for ligne in await revalider(p):
                    print(ligne)
                db.close()
                return 0
            found = await run_once(p, dry_run=True)
        db.close()
        if not found:
            print("Aucun candidat (API pump.fun indisponible ou aucun token au-dessus du seuil).")
        for f in found:
            etat = f"❌ {f.reason}" if f.reason else "✅ serait ajouté"
            print(f"${f.symbol:10} ATH {A.usd(f.ath):>9}  dev {f.creator}  {f.coins} tokens/{f.hits} succès  "
                  f"{etat}  {f.funder_note}")
        return 0

    return asyncio.run(go())


if __name__ == "__main__":
    raise SystemExit(main())
