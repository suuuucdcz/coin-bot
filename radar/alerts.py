"""Mise en forme des alertes Telegram (HTML) + boutons (liens et actions).

Une alerte se lit en 3 secondes, de haut en bas :
    1. quoi (titre) et quel token ;
    2. le VERDICT (à éviter / prudence / à vérifier / rien de suspect) ;
    3. qui (wallet suivi) et combien ;
    4. le token (CA copiable, âge, MC, dev, réseaux) ;
    5. les drapeaux 🚩, regroupés dans un bloc.
Boutons : liens (pump.fun, DexScreener, Solscan, X) puis actions (tracer, suivre, couper).
"""
from __future__ import annotations

from urllib.parse import quote

from .analysis.enrich import TokenInfo, x_handle
from .analysis.xlinks import parse_x_url
from .telegram import esc, keyboard

SEP = "───────────────"
RUG_MARK = "opérateur de rugs en série"
# Drapeaux qui justifient « prudence » (et pas seulement « à vérifier »)
SEVERE = ("mint authority", "freeze authority", "lanceur en série", "imite", "de la supply dès la création",
          "arnaque", "abonnés achetés", "racheté", "pas vers @", "liquidité très faible", "faux coin")


def short(a: str | None) -> str:
    return f"{a[:4]}…{a[-4:]}" if a and len(a) > 10 else (a or "?")


def usd(v: float | None) -> str:
    if v is None:
        return "?"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f} M$"
    if v >= 1000:
        return f"{v / 1000:.1f} k$"
    return f"{v:.0f} $"


def age(sec: int | None) -> str:
    if sec is None:
        return "?"
    if sec < 90:
        return f"{max(sec, 0)} s"
    if sec < 5400:
        return f"{sec // 60} min"
    if sec < 172800:
        return f"{sec // 3600} h {sec % 3600 // 60:02d}"
    return f"{sec // 86400} j"


def ticker(info: TokenInfo) -> str:
    sym = f"${esc(info.symbol)}" if info.symbol else "$???"
    return f"{sym} ({esc(info.name)})" if info.name else sym


def token_title(info: TokenInfo) -> str:
    sym = f"<b>${esc(info.symbol)}</b>" if info.symbol else "<b>$???</b>"
    return f"🪙 {sym}" + (f" · {esc(info.name)}" if info.name and info.name != info.symbol else "")


# --- verdict --------------------------------------------------------------------------------
def verdict(flags: list[str]) -> str:
    if any(RUG_MARK in f for f in flags):
        return "⛔ <b>À ÉVITER</b> — opérateur de rugs connu"
    graves = [f for f in flags if any(s in f.lower() for s in SEVERE)]
    if graves:
        return f"🟠 <b>PRUDENCE</b> — {len(graves)} signal{'aux' if len(graves) > 1 else ''} d'alerte"
    if flags:
        return f"🟡 <b>À vérifier</b> — {len(flags)} point{'s' if len(flags) > 1 else ''} à regarder"
    return "🟢 Rien de suspect détecté <i>(ça ne garantit rien)</i>"


def flags_block(flags: list[str]) -> list[str]:
    if not flags:
        return []
    uniques = list(dict.fromkeys(flags))
    corps = "\n".join(f"🚩 {esc(f)}" for f in uniques)
    return [f"<blockquote expandable>{corps}</blockquote>" if len(uniques) > 3 else f"<blockquote>{corps}</blockquote>"]


# --- sections ---------------------------------------------------------------------------------
def wallet_line(address: str, label: str | None, group: str | None = None) -> str:
    nom = f"<b>{esc(label)}</b>" if label else "<b>wallet suivi</b>"
    return f"👤 {nom}" + (f" · <i>{esc(group)}</i>" if group else "") + f"\n<code>{address}</code>"


def market_line(info: TokenInfo) -> str:
    if info.has_pool and info.pool_dex and "bonding" in info.pool_dex:
        pool = "🟣 bonding curve pump.fun"
    elif info.has_pool:
        pool = f"🏊 pool {esc(info.pool_dex)}"
        if info.liquidity_usd:
            pool += f" (liq. {usd(info.liquidity_usd)})"
    else:
        pool = "<b>👀 pas encore de pool (en avance)</b>"
    return f"⏱ {age(info.age_s)} · 💵 MC {usd(info.mc_usd)} · {pool}"


def dev_line(info: TokenInfo) -> str | None:
    if info.dev_coins is None:
        return "🧬 Dev : historique pump.fun indisponible" if info.creator else None
    if not info.dev_coins:
        return "🧬 Dev : aucun ancien token pump.fun (wallet neuf)"
    aths = [c["ath"] for c in info.dev_coins if c["ath"]]
    morts = sum(c["rug"] for c in info.dev_coins)
    s = f"🧬 Dev : {len(info.dev_coins)} ancien(s) token(s) · ATH max {usd(max(aths) if aths else None)}"
    if morts:
        s += f" · {morts} mort(s) (−99 %)"
    return s


def social_line(info: TokenInfo) -> str:
    bits = []
    link = parse_x_url(info.twitter)
    if link and link.kind == "profil":
        bits.append(f"🐦 @{esc(link.handle)}")
    elif link and link.kind == "tweet":
        bits.append(f"🐦 <a href=\"{esc(info.twitter)}\">{esc(link.describe())}</a> <i>(un tweet, pas le compte du projet)</i>")
    elif info.twitter:
        bits.append(f"🐦 <a href=\"{esc(info.twitter)}\">{esc(link.describe() if link else 'lien X')}</a>")
    if info.telegram:
        bits.append(f"💬 <a href=\"{esc(info.telegram)}\">Telegram</a>")
    if info.website:
        bits.append(f"🌐 <a href=\"{esc(info.website)}\">site</a>")
    return " · ".join(bits) if bits else "🐦 aucun lien X / site dans les métadonnées"


def security_line(info: TokenInfo) -> str | None:
    if info.supply_raw is None:
        return None
    mint = "⚠️ active" if info.mint_authority else "révoquée ✅"
    freeze = "⚠️ active" if info.freeze_authority else "révoquée ✅"
    return f"🔐 Mint : {mint} · Freeze : {freeze}"


def token_block(info: TokenInfo, extra_flags: list[str]) -> list[str]:
    """Section token (sans les drapeaux, affichés par card())."""
    lines = [f"📜 <code>{info.mint}</code>", market_line(info)]
    d = dev_line(info)
    if d:
        lines.append(d)
    lines.append(social_line(info))
    return lines


def card(title: str, info: TokenInfo | None, flags: list[str], *sections: list[str]) -> str:
    """Assemble une alerte : titre, token, verdict, sections séparées, drapeaux."""
    all_flags = list(dict.fromkeys((info.flags if info else []) + flags))
    lines = [title]
    if info is not None:
        lines.append(token_title(info))
        lines.append(verdict(all_flags))
    for sec in sections:
        if sec:
            lines.append(SEP)
            lines += sec
    lines += flags_block(all_flags)
    return "\n".join(lines)


# --- boutons ---------------------------------------------------------------------------------
def token_buttons(info: TokenInfo, wallet: str | None = None, mute: str | None = None,
                  follow: str | None = None) -> dict:
    liens = [("🟣 pump.fun", f"https://pump.fun/coin/{info.mint}"),
             ("📊 DexScreener", info.dex_url or f"https://dexscreener.com/solana/{info.mint}"),
             ("🔍 Solscan", f"https://solscan.io/token/{info.mint}")]
    social = []
    h = x_handle(info.twitter)
    if h:
        social.append(("🐦 X du token", f"https://x.com/{h}"))
    elif info.twitter:
        social.append(("🐦 Lien X", info.twitter))
    social.append(("🔎 CA sur X", f"https://x.com/search?q={quote(info.mint)}&f=live"))
    if wallet:
        social.append(("👛 Wallet", f"https://solscan.io/account/{wallet}"))
    actions = []
    if info.creator:
        actions.append(("🧬 Tracer le dev", f"t:{info.creator}"))
    if follow:
        actions.append(("👁 Suivre le dev", f"s:{follow}"))
    if mute:
        actions.append(("🔇 Couper 24 h", f"m:{mute}"))
    return keyboard(liens, social, actions)


def wallet_buttons(*addrs: str, sig: str | None = None, mute: str | None = None,
                   trace: str | None = None) -> dict:
    liens = [(f"👛 {short(a)}", f"https://solscan.io/account/{a}") for a in addrs]
    if sig:
        liens.append(("🧾 Transaction", f"https://solscan.io/tx/{sig}"))
    actions = []
    if trace:
        actions.append(("🧬 Tracer", f"t:{trace}"))
    if mute:
        actions.append(("🔇 Couper 24 h", f"m:{mute}"))
    return keyboard(liens[:2], liens[2:], actions)
