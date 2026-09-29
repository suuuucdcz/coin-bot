"""🎯 Cibles : un dev suivi de près, dans SA section Telegram. Tout ce que font ses wallets, en direct, jusqu'à son
prochain coin.

Une cible = le wallet du dev + tous les wallets neufs qu'il finance (et ceux qu'ils financent, sans limite de
profondeur), groupe « cible:<NOM> ». Pour eux, aucun filtre anti-bruit (bavard, sourdine, tempête, sniper, usine,
ferme, purge des inactifs, plafond de la watchlist) : chaque transaction est lue et racontée dans la section.

Le prochain coin est repéré par quatre chemins indépendants (le premier arrivé alerte, les autres sont dédoublonnés) :
  1. un wallet de la cible crée un token (transaction lue par Helius) ;
  2. le flux des nouveaux tokens pump.fun (PumpPortal) : le créateur est un wallet de la cible (secours si Helius rate) ;
  3. la toile : le créateur d'un nouveau token a été financé par un wallet de la cible (wallet qu'on ne suivait pas) ;
  4. plusieurs wallets de la cible achètent le même token en 10 min : c'est son coin, même s'il l'a créé depuis un
     wallet financé par un exchange (sans lien visible).
Ces alertes partent aussi dans ‼️ (avec le son) et sont épinglées dans la section.

Ajouter une cible : python -m radar.cible ajouter <NOM> <adresse du dev> [--wallets a b c] [--notes "..."]
(prise en compte par le radar en moins d'une minute, sans redémarrage).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict

from . import alerts as A
from .analysis.classify import analyze, programs, token_deltas
from .analysis.enrich import TokenInfo
from .sources import pumpfun
from .sources.helius import AMM_PROGRAMS, PUMP_FUN, account_keys, sol_deltas
from .telegram import esc

log = logging.getLogger("cible")

PREFIXE = "cible:"
COULEUR = 16766590
ACHAT_GROUPE_S = 600         # plusieurs wallets de la cible sur le même token en 10 min : son coin
NEUF_MAX_TX = 5              # un destinataire avec 5 tx au plus = wallet neuf : il rejoint la cible
POUSSIERE_SOL = 0.002        # en dessous, sans token : bruit (address poisoning, frais)
IMPORTANT_SOL = 0.05         # au-dessus : message avec le son
RECHARGE_S = 60


def topic(nom: str) -> str:
    return f"cible_{nom.lower()}"


def _qte(raw: int, dec: int) -> str:
    v = raw / 10 ** dec if dec else float(raw)
    return f"{v / 1e9:.2f} Md" if v >= 1e9 else f"{v / 1e6:.2f} M" if v >= 1e6 else f"{v / 1e3:.1f} k" if v >= 1e3 \
        else f"{v:g}"


class Cibles:
    def __init__(self, pipeline, tg):
        self.p = pipeline
        self.tg = tg
        self.db = pipeline.db
        self.membres: dict[str, str] = {}                         # adresse -> nom de la cible
        self.noms: set[str] = set()
        self._achats: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
        self._fiches: dict[str, dict] = {}                        # fiche pump.fun de chaque token vu (mint -> fiche)
        self._taches: set = set()                                 # références des tâches lancées (sinon perdues)

    # --- membres et sections --------------------------------------------------------------------------
    def recharger(self) -> list[str]:
        """Relit la base ; renvoie les cibles nouvelles (à préparer)."""
        self.membres = self.db.cible_membres()
        nouvelles = [r["nom"] for r in self.db.cibles() if r["nom"] not in self.noms]
        return nouvelles

    def cible_de(self, adresse: str | None) -> str | None:
        return self.membres.get(adresse) if adresse else None

    async def preparer(self, nom: str) -> None:
        """Section de la cible (créée si besoin) et fiche épinglée (une fois)."""
        self.noms.add(nom)
        if self.tg is None:
            return
        self.tg.add_topic(topic(nom), "🎯", f"Cible {nom}", COULEUR)
        try:
            await self.tg.ensure_topic(topic(nom))
        except Exception as e:
            log.warning("Section de la cible %s non créée : %s", nom, e)
        row = next((r for r in self.db.cibles() if r["nom"] == nom), None)
        wallets = [a for a, n in self.membres.items() if n == nom]
        fiche = [f"🎯 <b>CIBLE {esc(nom)}</b> : tout ce que font ses wallets, en direct",
                 f"Dev : <code>{row['racine'] if row else '?'}</code>",
                 f"{len(wallets)} wallets suivis (le dev et les wallets neufs qu'il finance ; chaque nouveau wallet "
                 "financé rejoint la cible automatiquement)."]
        if row and row["notes"]:
            fiche += ["", esc(row["notes"])]
        fiche += ["", "<b>Son prochain coin</b> déclenche une alerte 🎯 ici ET dans ‼️ (avec le son), épinglée : créé par "
                  "un de ses wallets, par un wallet qu'il a financé (même inconnu), ou acheté par plusieurs de ses "
                  "wallets en même temps."]
        self._envoyer(nom, "\n".join(fiche), cle=f"cible_fiche:{nom}", fort=False, epingler=True)

    async def ajouter(self, adresse: str, nom: str, role: str, parent: str | None) -> bool:
        """Un wallet rejoint la cible : surveillé, sans filtre, jamais purgé."""
        if adresse in self.membres:
            return False
        par = self.db.wallet(parent) if parent else None
        depth = min(9, ((par["depth"] if par else 1) or 1) + 1)
        label = f"{nom}_W_{adresse[:4]}"
        if self.db.wallet(adresse) is not None:
            self.db.set_wallet_role(adresse, label, PREFIXE + nom, role, depth)
        self.db.add_wallet(adresse, label, PREFIXE + nom, role, depth, parent)   # ajoute ou réactive
        self.membres[adresse] = nom
        self.p.watched.add(adresse)
        if self.p.watcher is not None:
            await self.p.watcher.add(adresse)
        log.info("Cible %s : %s ajouté (%s)", nom, adresse[:6], role)
        return True

    # --- envoi ----------------------------------------------------------------------------------------
    def _envoyer(self, nom: str, texte: str, markup: dict | None = None, cle: str | None = None, fort: bool = False,
                 epingler: bool = False, top: bool = False) -> None:
        if self.tg is None or self.p.dry_run:
            return
        self.tg.enqueue(texte, markup, key=cle, kind="cible" if fort else "cible_info", topic=topic(nom),
                        on_sent=self._epingler if epingler else None)
        if top:
            self.tg.enqueue_top(texte, markup, key=f"top:{cle}" if cle else None)

    def _epingler(self, message_id: int) -> None:
        tache = asyncio.get_running_loop().create_task(self.tg.pin(message_id))
        self._taches.add(tache)
        tache.add_done_callback(self._taches.discard)

    def _nom(self, adresse: str) -> str:
        lab = self.p.label(adresse)
        return f"<b>{esc(lab)}</b>" if lab else f"<code>{A.short(adresse)}</code>"

    async def _symbole(self, mint: str) -> tuple[str, dict]:
        """(ticker, fiche pump.fun) ; la fiche est gardée (créateur compris) dès qu'elle a été lue."""
        c = self._fiches.get(mint)
        if c is None and self.p.http is not None:
            try:
                c = await asyncio.wait_for(pumpfun.coin(self.p.http, mint), 4) or None
            except Exception:
                c = None
            if c and c.get("symbol"):
                self._fiches[mint] = c
        return (c or {}).get("symbol") or "?", c or {}

    # --- 🎯 le prochain coin ------------------------------------------------------------------------------
    async def nouveau_coin(self, nom: str, mint: str, createur: str, comment: str, symbole: str | None = None,
                           nom_token: str | None = None) -> None:
        if not symbole:
            symbole, c = await self._symbole(mint)
            nom_token = nom_token or c.get("name")
        info = TokenInfo(mint, name=nom_token, symbol=symbole, creator=createur)
        texte = (f"🎯 <b>CIBLE {esc(nom)} : NOUVEAU COIN — ${esc(symbole or '?')}</b>"
                 + (f" ({esc(nom_token)})" if nom_token else "") + f"\n{comment}\n<code>{mint}</code>\n"
                 f"Créateur : <code>{createur}</code>")
        self._envoyer(nom, texte, A.token_buttons(info, createur), cle=f"cible_crea:{mint}", fort=True, epingler=True,
                      top=True)
        log.warning("🎯 Cible %s : nouveau coin %s ($%s) — %s", nom, mint, symbole, comment)
        if createur and len(createur) >= 32 and createur not in self.membres:
            await self.ajouter(createur, nom, f"créateur de ${symbole} (cible {nom})", None)

    def on_new_token(self, msg: dict) -> None:
        """Flux pump.fun (PumpPortal) : un créateur de la cible (secours si la transaction Helius manque)."""
        createur, mint = msg.get("traderPublicKey"), msg.get("mint")
        nom = self.cible_de(createur)
        if nom and mint:
            tache = asyncio.get_running_loop().create_task(self.nouveau_coin(
                nom, mint, createur, f"Créé par {self._nom(createur)} (flux pump.fun)", msg.get("symbol"),
                msg.get("name")))
            self._taches.add(tache)
            tache.add_done_callback(self._taches.discard)

    async def lien_toile(self, t: dict, chaine: list[dict]) -> bool:
        """La toile a relié un nouveau créateur à un wallet de la cible (un wallet qu'on ne suivait pas encore)."""
        for h in chaine:
            nom = self.cible_de(h["src"])
            if nom:
                await self.nouveau_coin(nom, t["mint"], t["creator"],
                                        f"Créé par un wallet financé par {self._nom(h['src'])} ({h['sol']:g} SOL), "
                                        "repéré par la toile", t.get("symbol"), t.get("name"))
                return True
        return False

    # --- tout ce que font ses wallets ----------------------------------------------------------------
    async def on_tx(self, sig: str, tx: dict) -> None:
        if not self.membres or (tx.get("meta") or {}).get("err") is not None:
            return
        keys = account_keys(tx)
        tdeltas = token_deltas(tx)
        touches = {k for k in keys if k in self.membres} | {o for (o, _m) in tdeltas if o in self.membres}
        par_cible: dict[str, list[str]] = defaultdict(list)
        for w in touches:
            par_cible[self.membres[w]].append(w)
        for nom, wallets in par_cible.items():
            try:
                lignes, fort = await self._raconter(nom, wallets, sig, tx, tdeltas)
            except Exception:
                log.exception("Cible %s : transaction %s non racontée", nom, sig[:8])
                continue
            if lignes:
                heure = time.strftime("%H:%M:%S", time.localtime(tx.get("blockTime") or time.time()))
                texte = f"🎯 <b>{esc(nom)}</b> · {heure}\n" + "\n".join(lignes) + \
                    f"\n<a href=\"https://solscan.io/tx/{sig}\">transaction</a>"
                self._envoyer(nom, texte, cle=f"cible:{nom}:{sig}", fort=fort)

    async def _raconter(self, nom: str, wallets: list[str], sig: str, tx: dict, tdeltas) -> tuple[list[str], bool]:
        events = analyze(tx, set(wallets))
        deltas = sol_deltas(tx)
        lignes: list[str] = []
        fort = False
        decrits = set()
        for ev in events:
            qui = self._nom(ev.wallet)
            decrits.add(ev.wallet)
            if ev.kind == "create":
                await self.nouveau_coin(nom, ev.mint, ev.wallet, f"Créé par {qui} sur {esc(ev.dex or '?')}")
                lignes.append(f"🎯 {qui} <b>crée un token</b> : <code>{ev.mint}</code> (alerte épinglée)")
                fort = True
            elif ev.kind in ("buy", "sell", "supply_in", "supply_out", "lp_add"):
                sym, c = await self._symbole(ev.mint)
                q = _qte(ev.tokens_raw, ev.decimals)
                if ev.kind == "buy":
                    lignes.append(f"🟠 {qui} <b>achète</b> {q} ${esc(sym)} pour {ev.sol:.3f} SOL"
                                  + self._age(c) + f"\n   <code>{ev.mint}</code>")
                    fort = True
                    await self._achat_groupe(nom, ev.wallet, ev.mint, sym)
                elif ev.kind == "sell":
                    lignes.append(f"🔻 {qui} <b>vend</b> {q} ${esc(sym)} contre {ev.sol:.3f} SOL\n   <code>{ev.mint}</code>")
                    fort = ev.sol >= IMPORTANT_SOL
                elif ev.kind == "supply_in":
                    lignes.append(f"📥 {qui} reçoit {q} ${esc(sym)} sans payer\n   <code>{ev.mint}</code>")
                    fort = True
                elif ev.kind == "supply_out":
                    lignes.append(f"📤 {qui} envoie {q} ${esc(sym)} à {self._nom(ev.other or '')}\n   <code>{ev.mint}</code>")
                    fort = True
                else:
                    lignes.append(f"🟢 {qui} ajoute de la liquidité : ${esc(sym)} + {ev.sol:.3f} SOL\n   <code>{ev.mint}</code>")
                    fort = True
            elif ev.kind == "transfer":
                ligne, important = await self._envoi(nom, ev.wallet, ev.other, ev.sol)
                lignes.append(ligne)
                fort = fort or important
        # SOL reçu, et tout ce que l'analyse n'a pas décrit (rien n'est passé sous silence)
        for w in wallets:
            d = deltas.get(w, 0.0)
            if d > POUSSIERE_SOL and not any(e.wallet == w and e.kind == "sell" for e in events):
                src = min(((v, k) for k, v in deltas.items() if k != w and v < 0), default=(0, None))[1]
                lignes.append(f"💰 {self._nom(w)} <b>reçoit</b> {d:.4f} SOL de {self._source(src)}")
                fort = fort or d >= IMPORTANT_SOL
                decrits.add(w)
            elif w not in decrits:
                bouge = [(m, a, b, dec) for (o, m), (a, b, dec) in tdeltas.items() if o == w and a != b]
                if abs(d) <= POUSSIERE_SOL and not bouge:
                    continue   # frais seuls, poussière : bruit
                progs = self._programmes(tx)
                lignes.append(f"⚙️ {self._nom(w)} : {d:+.4f} SOL" + (f" · {len(bouge)} token(s) modifié(s)" if bouge else "")
                              + (f" · {progs}" if progs else ""))
        return lignes, fort

    def _age(self, c: dict) -> str:
        cree = c.get("created")
        if not cree:
            return ""
        age = int(time.time() - cree)
        createur = c.get("creator")
        lien = " (créé par un wallet de la cible)" if self.cible_de(createur) else ""
        return f" · token de {A.age(age)}{lien}"

    def _source(self, adresse: str | None) -> str:
        if not adresse:
            return "?"
        if self.cible_de(adresse):
            return f"{self._nom(adresse)} (cible)"
        lab = self.db.get_label(adresse)
        if lab:
            return f"<b>{esc(lab)}</b>"
        if self.p.is_service_address(adresse):
            return f"un exchange / service (<code>{A.short(adresse)}</code>)"
        return f"<code>{adresse}</code>"

    @staticmethod
    def _programmes(tx: dict) -> str:
        noms = sorted({AMM_PROGRAMS[p] for p in programs(tx) if p in AMM_PROGRAMS}
                      | ({"pump.fun"} if PUMP_FUN in programs(tx) else set()))
        return ", ".join(noms)

    async def _envoi(self, nom: str, src: str, dst: str | None, sol: float) -> tuple[str, bool]:
        """SOL envoyé par un wallet de la cible : interne, vers un exchange, ou vers un wallet NEUF (qui la rejoint)."""
        qui = self._nom(src)
        if not dst:
            return f"💸 {qui} envoie {sol:.4f} SOL", sol >= IMPORTANT_SOL
        if self.cible_de(dst):
            return f"🔁 {qui} → {self._nom(dst)} : {sol:.4f} SOL (entre ses wallets)", sol >= IMPORTANT_SOL
        lab = self.db.get_label(dst)
        if lab or self.p.is_service_address(dst):
            return (f"🏦 {qui} <b>envoie {sol:.4f} SOL vers un exchange</b> ({esc(lab or 'service')}) : l'argent peut "
                    "revenir par un wallet neuf sans lien visible", True)
        try:
            n = len(await self.p.rpc.signatures(dst, limit=NEUF_MAX_TX + 1)) if self.p.rpc is not None else NEUF_MAX_TX + 1
        except Exception:
            n = NEUF_MAX_TX + 1
        if n <= NEUF_MAX_TX:
            await self.ajouter(dst, nom, f"wallet neuf financé par {self.p.label(src) or src[:6]} ({sol:g} SOL)", src)
            return (f"🆕 {qui} <b>finance un wallet NEUF</b> ({sol:.4f} SOL) : <code>{dst}</code>\n"
                    "   → ajouté à la cible (souvent le prochain wallet de lancement)", True)
        return f"💸 {qui} envoie {sol:.4f} SOL à <code>{dst}</code> (wallet existant, {NEUF_MAX_TX}+ tx)", sol >= IMPORTANT_SOL

    async def _achat_groupe(self, nom: str, wallet: str, mint: str, sym: str) -> None:
        """Plusieurs wallets de la cible sur le même token en 10 min : c'est son coin."""
        now = time.time()
        achats = self._achats[(nom, mint)]
        achats[wallet] = now
        for w, t in list(achats.items()):
            if now - t > ACHAT_GROUPE_S:
                del achats[w]
        if len(achats) >= 2:
            _s, c = await self._symbole(mint)
            await self.nouveau_coin(nom, mint, c.get("creator") or "?",
                                    f"{len(achats)} wallets de la cible l'achètent en moins de {ACHAT_GROUPE_S // 60} min "
                                    "(achat groupé au lancement)", sym, c.get("name"))

    # --- suivi -----------------------------------------------------------------------------------------
    def status_line(self) -> str | None:
        if not self.noms:
            return None
        par = defaultdict(int)
        for n in self.membres.values():
            par[n] += 1
        return "🎯 Cibles : " + " · ".join(f"{n} ({par[n]} wallets)" for n in sorted(self.noms))

    async def boucle(self) -> None:
        """Nouvelles cibles et nouveaux wallets ajoutés en base (commande, autre module) pris en compte."""
        while True:
            try:
                avant = set(self.membres)
                for nom in self.recharger():
                    await self.preparer(nom)
                for a in set(self.membres) - avant:
                    if a not in self.p.watched:
                        self.p.watched.add(a)
                        if self.p.watcher is not None:
                            await self.p.watcher.add(a)
            except Exception:
                log.exception("Cibles : rechargement en échec")
            await asyncio.sleep(RECHARGE_S)


def main() -> int:
    """python -m radar.cible ajouter <NOM> <adresse du dev> [--wallets a b c] [--notes "..."]"""
    import argparse

    from . import config as cfgmod
    from .db import DB

    ap = argparse.ArgumentParser(prog="python -m radar.cible")
    sous = ap.add_subparsers(dest="cmd", required=True)
    aj = sous.add_parser("ajouter", help="suivre un dev de près dans sa propre section")
    aj.add_argument("nom")
    aj.add_argument("dev")
    aj.add_argument("--wallets", nargs="*", default=[], help="wallets déjà connus de ce dev (financés par lui…)")
    aj.add_argument("--notes", default="")
    sous.add_parser("liste")
    args = ap.parse_args()
    db = DB(cfgmod.load().db_path)
    if args.cmd == "liste":
        membres = db.cible_membres()
        for r in db.cibles():
            print(f"{r['nom']} : dev {r['racine']} · {sum(1 for n in membres.values() if n == r['nom'])} wallets")
        db.close()
        return 0
    nom = args.nom.upper()
    db.cible_add(nom, args.dev, args.notes)
    for adresse, role, depth, parent in [(args.dev, f"dev (cible {nom})", 1, None)] + \
            [(a, f"wallet du dev (cible {nom})", 2, args.dev) for a in args.wallets]:
        label = f"DEV_{nom}_{adresse[:4]}" if depth == 1 else f"{nom}_W_{adresse[:4]}"
        if db.wallet(adresse) is not None:
            db.set_wallet_role(adresse, label, PREFIXE + nom, role, depth)
        db.add_wallet(adresse, label, PREFIXE + nom, role, depth, parent)
        db.conn.execute("UPDATE wallets SET active = 1 WHERE address = ?", (adresse,))
    db.conn.commit()
    print(f"Cible {nom} : {1 + len(args.wallets)} wallets. Le radar la prend en compte en moins d'une minute.")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
