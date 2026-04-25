import os
import time
import math
import discord
from discord import app_commands
from urllib.parse import urlencode
from typing import Optional, Tuple, Dict, Any
import re
import aiohttp
from core.config import ENV_PATH, settings

print(f"Loaded .env from: {ENV_PATH}")

DISCORD_TOKEN = settings.discord_token
GUILD_ID = settings.guild_id
GUILD = discord.Object(id=GUILD_ID) if GUILD_ID else None
LOG_CHANNEL_ID = settings.log_channel_id
EXECUTION_MODE = settings.execution_mode
PLATFORM_FEE_BPS = settings.platform_fee_bps
SLIPPAGE_BPS = settings.slippage_bps
ALLOWED_CHANNEL_IDS = settings.allowed_channel_ids
ALLOWED_USER_IDS = settings.allowed_user_ids

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# --- Solana (Jupiter) helpers (Phase 1: READ-ONLY) ---
# Keep only true "base" aliases here. Everything else resolves dynamically via Jupiter Tokens V2 search.
TOKEN_MAP = {
    "SOL": {"mint": "So11111111111111111111111111111111111111112", "decimals": 9},
    "USDC": {"mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "decimals": 6},
    # Alias for convenience; we'll normalize USD -> USDC
    "USD": {"alias": "USDC"},
}

# Jupiter Tokens V2 (discovery). Prefer api.jup.ag with API key when available.
# Docs: Lite API migration (API key required on api.jup.ag), paths unchanged.
JUPITER_API_KEY = (os.getenv("JUPITER_API_KEY") or "").strip()
JUPITER_TOKENS_BASE = (os.getenv("JUPITER_TOKENS_BASE") or "https://api.jup.ag/tokens/v2").strip().rstrip("/")
JUPITER_TOKENS_LITE_BASE = (os.getenv("JUPITER_TOKENS_LITE_BASE") or "https://lite-api.jup.ag/tokens/v2").strip().rstrip("/")

# Tiny in-memory cache for token search results to reduce rate/latency.
# Keyed by lowercase query; value is (ts, list_of_tokens).
_JUP_TOKEN_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_JUP_TOKEN_TTL_SEC = 10 * 60  # 10 minutes

async def _jup_tokens_search(query: str) -> list[dict[str, Any]]:
    """Search Jupiter Tokens V2 for symbol/name/mint.

    Uses api.jup.ag with x-api-key when available; otherwise falls back to lite-api.
    """
    q = (query or "").strip()
    if not q:
        return []

    key = q.lower()
    now = time.time()
    cached = _JUP_TOKEN_CACHE.get(key)
    if cached and (now - cached[0]) < _JUP_TOKEN_TTL_SEC:
        return cached[1]

    timeout = aiohttp.ClientTimeout(total=12)
    headers = {"User-Agent": "rudis-bot/0.1", "Accept": "application/json"}

    async def _fetch(base: str, include_key: bool) -> list[dict[str, Any]]:
        url = f"{base}/search"
        h = dict(headers)
        if include_key and JUPITER_API_KEY:
            h["x-api-key"] = JUPITER_API_KEY
        async with aiohttp.ClientSession(timeout=timeout, headers=h) as session:
            async with session.get(url, params={"query": q}) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return data if isinstance(data, list) else []

    # Prefer api.jup.ag if key is set; otherwise try lite first.
    out: list[dict[str, Any]] = []
    if JUPITER_API_KEY:
        out = await _fetch(JUPITER_TOKENS_BASE, include_key=True)
        if not out:
            out = await _fetch(JUPITER_TOKENS_LITE_BASE, include_key=False)
    else:
        out = await _fetch(JUPITER_TOKENS_LITE_BASE, include_key=False)
        if not out:
            out = await _fetch(JUPITER_TOKENS_BASE, include_key=False)

    _JUP_TOKEN_CACHE[key] = (now, out)
    return out


def _pick_best_symbol_match(sym: str, results: list[dict[str, Any]]) -> tuple[Optional[dict[str, Any]], Optional[list[dict[str, Any]]]]:
    """Pick best match for an exact symbol from Jupiter tokens search.

    If multiple exact-symbol matches exist, prefer verified tokens (isVerified/tags includes 'verified').
    If still ambiguous, return (best, alts) so caller can ask user to paste mint.
    """
    s = (sym or "").strip().upper()
    if not s or not results:
        return None, None

    exact = [t for t in results if (t.get("symbol") or "").strip().upper() == s]
    if not exact:
        return None, None

    def _is_verified(t: dict[str, Any]) -> bool:
        if t.get("isVerified") is True:
            return True
        tags = t.get("tags") or []
        if isinstance(tags, list) and any((str(x).lower() == "verified") for x in tags):
            return True
        return False

    verified = [t for t in exact if _is_verified(t)]

    # If exactly one verified, use it.
    if len(verified) == 1:
        return verified[0], None

    # If multiple verified, still ambiguous.
    if len(verified) > 1:
        return verified[0], verified

    # No verified info — if only one exact, use it; otherwise ambiguous.
    if len(exact) == 1:
        return exact[0], None

    return exact[0], exact


def _tok_from_jup(t: dict[str, Any], fallback_symbol: str) -> dict[str, Any]:
    mint = (t.get("id") or t.get("mint") or "").strip()
    sym = (t.get("symbol") or fallback_symbol).strip().upper()
    name = (t.get("name") or "").strip()
    dec = t.get("decimals")
    try:
        decimals = int(dec) if dec is not None else 9
    except Exception:
        decimals = 9
    return {"mint": mint, "symbol": sym or fallback_symbol, "decimals": decimals, "name": name}


async def _jup_lookup_mint(mint: str) -> Optional[dict[str, Any]]:
    """Lookup a mint via Tokens V2 search (query by mint)."""
    m = (mint or "").strip()
    if not m:
        return None
    res = await _jup_tokens_search(m)
    for t in res:
        tid = (t.get("id") or "").strip()
        if tid == m:
            return t
    return None


# --- Token resolution (Phase 1): core aliases + DexScreener search (Solana SPL) ---
_MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

def is_probable_solana_mint(s: str) -> bool:
    return bool(_MINT_RE.match((s or "").strip()))

async def resolve_token_ref_with_alts(ref: str) -> Tuple[Optional[Dict[str, Any]], Optional[list[Dict[str, Any]]]]:
    """Resolve token ref to {mint, symbol, decimals, name?}.

    Supports:
    - SOL/USDC core aliases from TOKEN_MAP
    - direct mint paste
    - symbol lookup via Jupiter Tokens V2 (preferred) and DexScreener fallback

    Returns (token, alts). If alts is not None, it contains multiple candidates and
    the caller should ask the user to paste a mint address.

    Note: For non-core tokens we default decimals=9 for URL amount encoding.
    Without a reliable token registry, we can’t guarantee exact decimals for every token.
    """
    r = (ref or "").strip()
    if not r:
        return None, None

    # 1) Mint pasted directly (best path)
    if is_probable_solana_mint(r):
        # Try to fetch real decimals/name via Jupiter tokens search (by mint)
        try:
            t = await _jup_lookup_mint(r)
            if t:
                return _tok_from_jup(t, fallback_symbol=r[:4] + "..."), None
        except Exception:
            pass
        # Fallback if token discovery fails
        return {"mint": r, "symbol": r[:4] + "...", "decimals": 9, "name": ""}, None

    sym = normalize_symbol(r)

    # 2) Core aliases (SOL/USDC/USD)
    meta = TOKEN_MAP.get(sym)
    if meta and "mint" in meta:
        return {"mint": meta["mint"], "symbol": sym, "decimals": int(meta["decimals"]), "name": ""}, None

    # 3) Jupiter Tokens V2 search for symbol/name
    # This reduces the "BONK has 6 mints" DexScreener ambiguity for well-known tokens.
    try:
        jup_results = await _jup_tokens_search(sym)
        best, alts = _pick_best_symbol_match(sym, jup_results)
        if best:
            tok = _tok_from_jup(best, fallback_symbol=sym)
            if tok.get("mint"):
                # If still ambiguous, show the user the top candidates and ask for mint.
                if alts:
                    # Convert alts to the compact shape used by the caller.
                    alt_out: list[Dict[str, Any]] = []
                    for t in alts[:6]:
                        alt_out.append(_tok_from_jup(t, fallback_symbol=sym))
                    return tok, alt_out
                return tok, None
    except Exception:
        pass

    # 4) DexScreener search fallback (still useful for very new/unlisted tokens)
    url = f"https://api.dexscreener.com/latest/dex/search?q={sym}"
    timeout = aiohttp.ClientTimeout(total=12)
    headers = {"User-Agent": "rudis-bot/0.1", "Accept": "application/json"}

    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None, None
                data = await resp.json()

        pairs = data.get("pairs") or []
        if not pairs:
            return None, None

        cands: list[Dict[str, Any]] = []
        seen_mints: set[str] = set()

        def _liq_usd(p: dict[str, Any]) -> float:
            liq = p.get("liquidity") or {}
            v = liq.get("usd")
            try:
                return float(v)
            except Exception:
                return 0.0

        for p in sorted(pairs, key=_liq_usd, reverse=True):
            if (p.get("chainId") or "").lower() != "solana":
                continue

            bt = p.get("baseToken") or {}
            mint = (bt.get("address") or "").strip()
            name = (bt.get("name") or "").strip()
            psym = (bt.get("symbol") or sym).strip().upper()

            if not mint or mint in seen_mints:
                continue

            seen_mints.add(mint)

            # Try to enrich decimals via Jupiter lookup-by-mint
            decimals = 9
            try:
                jt = await _jup_lookup_mint(mint)
                if jt and jt.get("decimals") is not None:
                    decimals = int(jt.get("decimals"))
            except Exception:
                pass

            cands.append({"mint": mint, "symbol": psym or sym, "decimals": decimals, "name": name})
            if len(cands) >= 6:
                break

        if not cands:
            return None, None

        if len(cands) > 1:
            return cands[0], cands

        return cands[0], None

    except Exception:
        return None, None

async def resolve_token_ref(ref: str) -> Optional[Dict[str, Any]]:
    tok, _alts = await resolve_token_ref_with_alts(ref)
    return tok


def jupiter_swap_link(input_tok: Dict[str, Any], output_tok: Dict[str, Any], amount_ui: float, slippage_bps: int = 50) -> str:
    """Build a Jupiter swap link with reliable prefill.

    Uses inputMint/outputMint + atomic amount for stable amount prefill.
    """
    try:
        amount_atomic = to_atomic(float(amount_ui), int(input_tok["decimals"]))
        if amount_atomic <= 0:
            return "https://jup.ag/swap"

        qs = {
            "inputMint": input_tok["mint"],
            "outputMint": output_tok["mint"],
            "amount": str(amount_atomic),
            "slippageBps": str(int(slippage_bps)),
            "swapMode": "ExactIn",
        }
        return "https://jup.ag/swap?" + urlencode(qs)
    except Exception:
        return "https://jup.ag/swap"

def normalize_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s:
        return s
    meta = TOKEN_MAP.get(s)
    if meta and "alias" in meta:
        return meta["alias"]
    return s

def to_atomic(amount: float, decimals: int) -> int:
    # Avoid float drift
    return int(math.floor((amount * (10 ** decimals)) + 1e-9))

async def jupiter_quote(input_tok: Dict[str, Any], output_tok: Dict[str, Any], amount_ui: float, slippage_bps: int = 50) -> dict:
    """Fetch a Jupiter quote. Returns the raw JSON dict."""
    if not input_tok or not output_tok or "mint" not in input_tok or "mint" not in output_tok:
        raise ValueError("Unsupported token(s)")

    amount_in = to_atomic(float(amount_ui), int(input_tok["decimals"]))
    if amount_in <= 0:
        raise ValueError("amount must be > 0")

    url = os.getenv("JUPITER_QUOTE_URL", "https://api.jup.ag/swap/v1/quote")
    fallback_url = "https://quote-api.jup.ag/v6/quote"
    params = {
        "inputMint": input_tok["mint"],
        "outputMint": output_tok["mint"],
        "amount": str(amount_in),
        "slippageBps": str(int(slippage_bps)),
        "onlyDirectRoutes": "false",
    }

    timeout = aiohttp.ClientTimeout(total=12)
    headers = {"User-Agent": "rudis-bot/0.1", "Accept": "application/json"}

    async def _fetch(u: str) -> dict:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(u, params=params) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    raise RuntimeError(f"Jupiter quote HTTP {resp.status} @ {u}: {txt[:200]}")
                return await resp.json()

    try:
        return await _fetch(url)
    except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ClientConnectionError):
        return await _fetch(fallback_url)


async def post_log(msg: str):
    if not LOG_CHANNEL_ID:
        return
    ch = client.get_channel(LOG_CHANNEL_ID)
    if ch is None:
        try:
            ch = await client.fetch_channel(LOG_CHANNEL_ID)
        except Exception:
            ch = None
    if ch:
        try:
            await ch.send(msg)
        except Exception:
            pass


def sync_scope_label() -> str:
    return f"guild:{GUILD_ID}" if GUILD_ID else "global"


def format_token_candidates(sym: str, alts: list[Dict[str, Any]]) -> str:
    top = alts[:3]
    lines = []
    for t in top:
        name = (t.get("name") or "").strip()
        mint = (t.get("mint") or "").strip()
        shown_sym = (t.get("symbol") or sym).strip()
        lines.append(f"• {shown_sym} — {name or 'Unknown'} — `{mint}`")
    return "\n".join(lines)


async def ensure_prepare_allowed(interaction: discord.Interaction) -> bool:
    # If ALLOWED_CHANNEL_IDS is set, only allow /prepare in those channels.
    if ALLOWED_CHANNEL_IDS and interaction.channel_id not in ALLOWED_CHANNEL_IDS:
        await post_log(
            f"⛔ DENY(channel) user={interaction.user.id} channel={interaction.channel_id} guild={interaction.guild_id} "
            f"allowed_channels={len(ALLOWED_CHANNEL_IDS)}"
        )
        await interaction.followup.send("⚠️ /prepare is restricted to the execution channel.", ephemeral=True)
        return False

    # If ALLOWED_USER_IDS is set, only allow those users to run /prepare.
    if ALLOWED_USER_IDS and interaction.user.id not in ALLOWED_USER_IDS:
        await post_log(
            f"⛔ DENY(user) user={interaction.user.id} channel={interaction.channel_id} guild={interaction.guild_id} "
            f"allowed_users={len(ALLOWED_USER_IDS)}"
        )
        await interaction.followup.send("⚠️ You’re not allowed to use this command yet.", ephemeral=True)
        return False

    # If channel allowlisting is unset but a log channel exists, default to that channel only.
    if not ALLOWED_CHANNEL_IDS and LOG_CHANNEL_ID and interaction.channel_id != LOG_CHANNEL_ID:
        await interaction.followup.send("⚠️ /prepare is restricted to #bot-logs.", ephemeral=True)
        return False

    return True


async def fetch_rudis_execution_context(
    base_tok: Dict[str, Any],
    quote_tok: Dict[str, Any],
    amount: float,
    quote_json: dict,
) -> Optional[dict[str, Any]]:
    if not settings.rudis_execution_context_url:
        return None

    payload = {
        "base": {
            "symbol": base_tok.get("symbol"),
            "mint": base_tok.get("mint"),
            "decimals": base_tok.get("decimals"),
        },
        "quote_token": {
            "symbol": quote_tok.get("symbol"),
            "mint": quote_tok.get("mint"),
            "decimals": quote_tok.get("decimals"),
        },
        "amount": amount,
        "slippage_bps": SLIPPAGE_BPS,
        "quote_summary": {
            "out_amount": quote_json.get("outAmount"),
            "price_impact_pct": quote_json.get("priceImpactPct"),
        },
    }

    timeout = aiohttp.ClientTimeout(total=settings.rudis_execution_context_timeout_sec)
    headers = {"User-Agent": "rudis-bot/0.1", "Accept": "application/json", "Content-Type": "application/json"}

    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.post(settings.rudis_execution_context_url, json=payload) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data if isinstance(data, dict) else None
    except Exception:
        return None


def build_execution_embed(
    base_tok: Dict[str, Any],
    quote_tok: Dict[str, Any],
    amount: float,
    out_ui: float,
    price_impact_txt: str,
    context: Optional[dict[str, Any]],
) -> discord.Embed:
    embed = discord.Embed(
        title="Execution Ticket",
        description="Non-custodial route — opens Jupiter for wallet approval.",
    )

    embed.add_field(name="Pair", value=f"{base_tok['symbol']} → {quote_tok['symbol']}", inline=True)
    embed.add_field(name="Size", value=f"{amount:g} {base_tok['symbol']}", inline=True)
    embed.add_field(name="Est. Out", value=f"≈ {out_ui:.6g} {quote_tok['symbol']}", inline=True)
    embed.add_field(name="Price Impact", value=price_impact_txt, inline=True)
    embed.add_field(name="Slippage", value=f"{SLIPPAGE_BPS} bps", inline=True)
    embed.add_field(name="Mode", value=EXECUTION_MODE, inline=True)

    if EXECUTION_MODE == "route":
        embed.add_field(name="Fee", value="0 bps (link-out)", inline=True)
    else:
        embed.add_field(name="Fee", value=f"{PLATFORM_FEE_BPS} bps", inline=True)

    if context:
        confidence = str(context.get("confidence_label") or context.get("confidence") or "").strip()
        routing_hint = str(context.get("routing_hint") or context.get("channel") or "").strip()
        thesis = str(context.get("thesis") or context.get("summary") or "").strip()
        risk = str(context.get("risk") or context.get("risk_note") or "").strip()

        if confidence:
            embed.add_field(name="Rudis Confidence", value=confidence[:128], inline=True)
        if routing_hint:
            embed.add_field(name="Routing Hint", value=routing_hint[:128], inline=True)
        if thesis:
            embed.add_field(name="Rudis Context", value=thesis[:512], inline=False)
        if risk:
            embed.add_field(name="Risk Note", value=risk[:256], inline=False)

    embed.set_footer(text=f"{settings.bot_label} • Execution Intelligence Layer")
    return embed


class ConfirmCancelView(discord.ui.View):
    """Non-custodial routing. Uses direct link button for auto-open behavior."""

    def __init__(self, owner_id: int, base_tok: Dict[str, Any], quote_tok: Dict[str, Any], amount: float, quote_data: dict | None, created_at: float):
        super().__init__(timeout=600)
        self.owner_id = owner_id
        self.base_tok = base_tok
        self.quote_tok = quote_tok
        self.amount = amount
        self.quote_data = quote_data
        self.created_at = created_at
        self._handled = False

        # --- AUTO-OPEN LINK BUTTON ---
        swap_url = jupiter_swap_link(self.base_tok, self.quote_tok, self.amount, slippage_bps=SLIPPAGE_BPS)

        self.add_item(
            discord.ui.Button(
                label="Open Route (Jupiter)",
                style=discord.ButtonStyle.link,
                url=swap_url,
                emoji="🪐",
            )
        )


    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only the requester can cancel
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("⚠️ Only the requester can use these controls.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, emoji="❌")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._handled:
            await interaction.response.send_message("Already handled.", ephemeral=True)
            return

        self._handled = True
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

        await interaction.response.edit_message(view=self)
        await interaction.followup.send("❌ Cancelled. No route opened.", ephemeral=True)

    async def on_timeout(self):
        if self._handled:
            return
        self._handled = True
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True


@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    print(f"Allowlist: users={len(ALLOWED_USER_IDS)} channels={len(ALLOWED_CHANNEL_IDS)} log_channel_set={bool(LOG_CHANNEL_ID)}")
    print(f"Python: {os.sys.executable}")

    if GUILD:
        await tree.sync(guild=GUILD)
        msg = f"✅ Rudis Bot online. Synced slash commands to guild {GUILD_ID}"
        print(msg)
        await post_log(f"Startup: users_allow={len(ALLOWED_USER_IDS)} channels_allow={len(ALLOWED_CHANNEL_IDS)} env={ENV_PATH}")
        await post_log(msg)
    else:
        await tree.sync()
        msg = "✅ Rudis Bot online. Synced slash commands globally"
        print(msg)
        await post_log(f"Startup: users_allow={len(ALLOWED_USER_IDS)} channels_allow={len(ALLOWED_CHANNEL_IDS)} env={ENV_PATH}")
        await post_log(msg)


@tree.command(name="status", description="Show bot status", guild=GUILD)
async def status(interaction: discord.Interaction):
    await interaction.response.send_message(
        "\n".join(
            [
                f"Bot: `{settings.bot_label}`",
                f"Sync scope: `{sync_scope_label()}`",
                f"Mode: `{EXECUTION_MODE}`",
                f"Slippage: `{SLIPPAGE_BPS} bps`",
                f"Allowed channels: `{len(ALLOWED_CHANNEL_IDS)}`",
                f"Allowed users: `{len(ALLOWED_USER_IDS)}`",
                f"Rudis context: `{'enabled' if settings.has_rudis_context else 'disabled'}`",
            ]
        ),
        ephemeral=True,
    )


@tree.command(name="prepare", description="Prepare a swap session", guild=GUILD)
@app_commands.describe(
    base="Base token (e.g., SOL)",
    quote="Quote token (e.g., USDC)",
    amount="Amount to swap",
)
async def prepare(interaction: discord.Interaction, base: str, quote: str, amount: float):
    # Defer immediately to avoid “application did not respond”.
    # Sometimes Discord reports the interaction as "Unknown" (10062) if the client waited too long
    # or the interaction was already acknowledged; handle that gracefully.
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(thinking=False)
    except (discord.NotFound, discord.InteractionResponded):
        # Can't acknowledge this interaction anymore.
        return
    except Exception:
        # Don't crash the command handler on transient gateway/webhook issues.
        return

    if not await ensure_prepare_allowed(interaction):
        return

    base_u = normalize_symbol(base)
    quote_u = normalize_symbol(quote)

    base_tok, base_alts = await resolve_token_ref_with_alts(base_u)
    if not base_tok:
        base_tok, base_alts = await resolve_token_ref_with_alts(base)

    quote_tok, quote_alts = await resolve_token_ref_with_alts(quote_u)
    if not quote_tok:
        quote_tok, quote_alts = await resolve_token_ref_with_alts(quote)

    # If ambiguous symbol, ask user to paste mint for precision.
    if base_alts:
        await interaction.followup.send(
            "⚠️ **Ambiguous base symbol**. Multiple tokens share that ticker.\n"
            "Paste the token **mint address** to choose exactly which one you mean.\n\n"
            + format_token_candidates(normalize_symbol(base), base_alts),
            ephemeral=True,
        )
        return

    if quote_alts:
        await interaction.followup.send(
            "⚠️ **Ambiguous quote symbol**. Multiple tokens share that ticker.\n"
            "Paste the token **mint address** to choose exactly which one you mean.\n\n"
            + format_token_candidates(normalize_symbol(quote), quote_alts),
            ephemeral=True,
        )
        return

    if not base_tok or not quote_tok:
        await interaction.followup.send(
            "⚠️ Unknown token. Use a valid SPL symbol (ex: BONK) or paste the token mint address.",
            ephemeral=True,
        )
        return

    # Prevent same mint swaps
    if base_tok["mint"] == quote_tok["mint"]:
        await interaction.followup.send("⚠️ base and quote can’t be the same token.", ephemeral=True)
        return

    if amount is None or amount <= 0:
        await interaction.followup.send("⚠️ amount must be > 0.", ephemeral=True)
        return

    try:
        quote_json = await jupiter_quote(base_tok, quote_tok, float(amount), slippage_bps=SLIPPAGE_BPS)
    except Exception as e:
        await interaction.followup.send(f"⚠️ Quote failed: {e}", ephemeral=True)
        await post_log(f"⚠️ QUOTE FAIL by <@{interaction.user.id}>: {amount} {base_tok['symbol']}->{quote_tok['symbol']} | {e}")
        return

    # Extract basic numbers for display
    out_amount_atomic = int(quote_json.get("outAmount", 0))
    out_dec = int(quote_tok["decimals"])
    out_ui = out_amount_atomic / (10 ** out_dec) if out_dec >= 0 else 0
    price_impact = quote_json.get("priceImpactPct")
    price_impact_txt = f"{float(price_impact) * 100:.2f}%" if price_impact is not None else "n/a"
    rudis_context = await fetch_rudis_execution_context(base_tok, quote_tok, float(amount), quote_json)
    embed = build_execution_embed(
        base_tok=base_tok,
        quote_tok=quote_tok,
        amount=float(amount),
        out_ui=out_ui,
        price_impact_txt=price_impact_txt,
        context=rudis_context,
    )

    created_at = time.time()
    view = ConfirmCancelView(
        owner_id=interaction.user.id,
        base_tok=base_tok,
        quote_tok=quote_tok,
        amount=float(amount),
        quote_data=quote_json,
        created_at=created_at,
    )
    await post_log(
        f"✅ PREPARE by <@{interaction.user.id}> in {interaction.channel_id}: "
        f"{amount:g} {base_tok['symbol']} -> {quote_tok['symbol']} | mode={EXECUTION_MODE} scope={sync_scope_label()}"
    )
    await interaction.followup.send(embed=embed, view=view)


if __name__ == "__main__":
    client.run(DISCORD_TOKEN)
