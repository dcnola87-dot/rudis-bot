# Rudis Bot Dev Handoff Brief

## Context

Rudis is building toward an execution-intelligence stack, and this bot repo is the Discord-facing operator layer for that vision.

Today, the repo already contains two practical lanes:

- A live Discord execution bot in `execution_bot/main.py`
- Legacy stock and crypto scanners in `legacy/` that still matter for signal flow and formatting

The goal for the next dev is not to reinvent the bot. The goal is to preserve what is already working, tighten routing/filtering, and progressively wire the bot into the broader Rudis AI backend as those APIs come online.

Important framing:

- `rudis-ai` is expected to become the richer source of market, token, and execution data
- This repo should stay focused on bot orchestration, routing, presentation, and operator workflows
- Keep Python as-is for this bot layer unless there is a very strong reason not to

## Infrastructure

### Target deployment shape

The intended production shape is:

- Oracle Free Tier host
- PM2 supervising the long-running bot processes
- Two bot processes: one for execution/commands, one for scanning/signal delivery

### Current local/runtime reality

The repo currently launches with shell scripts and `tmux`, not PM2:

- `run_rudis.sh` starts the active execution bot in a `tmux` session named `rbot`
- `legacy/run_bot.sh` starts the older worker flow in a `tmux` session

That means the infra brief for a new dev should be:

- Migrate process supervision to PM2 if desired for production
- Do not break the existing shell launch flow while doing it
- Treat the shell scripts as the stable fallback path until PM2 is fully proven

## Bot Architecture

### Active bot

Primary active file:

- `execution_bot/main.py`

What it currently does:

- Loads repo-root `.env` explicitly
- Connects a Discord bot with slash commands
- Registers `/prepare`
- Resolves Solana token refs
- Gets Jupiter quotes
- Returns a non-custodial execution ticket
- Opens a Jupiter route via link button

This is already a usable execution-prep bot. It is not a paper design.

### Current file structure

Key files and folders:

- `execution_bot/main.py`: active Discord execution bot
- `execution_bot/clear_global_commands.py`: utility to clear global slash commands
- `run_rudis.sh`: current launcher for the execution bot
- `legacy/rth_momentum_scanner.py`: stock momentum scanner
- `legacy/crypto_momentum_scanner.py`: crypto momentum scanner
- `legacy/run_bot.sh`: older bot launcher
- `logs/`: runtime logging output

### Legacy code stance

The `legacy/` folder is not dead weight. It contains working signal logic, routing assumptions, and formatting patterns that should be treated carefully during refactors.

## Channel Routing

### Desired routing

The clean directional split should be:

- Stocks -> `rudis-stocks`
- Crypto higher-risk / casino-style plays -> `crypto-confirmed`

### Current routing reality

Current routing in the repo is still webhook/env-driven in the legacy scanners:

- `legacy/rth_momentum_scanner.py` posts to `STOCKS_WEBHOOK`
- `legacy/crypto_momentum_scanner.py` posts to `CRYPTO_WEBHOOK`

The active execution bot also has its own Discord controls:

- `DISCORD_GUILD_ID`
- `DISCORD_LOG_CHANNEL_ID`
- `ALLOWED_CHANNEL_IDS`
- `ALLOWED_USER_IDS`

Right now, `/prepare` is intentionally gated:

- It can be restricted to specific channels
- It can be restricted to specific users
- If channel allowlisting is not set, it falls back to the log channel restriction

Implementation note for the next dev:

- Keep routing decisions explicit in config
- Avoid hardcoding channel behavior deep in message formatting logic
- Preserve the current command guardrails while extending channel routing

## Crypto Filter Spec

### Target filtering direction

The crypto side should move toward a much more selective feed, with:

- Pump.fun graduation events as the primary signal
- DEX Screener and Helius as the core data sources
- A bias toward fewer, higher-conviction alerts instead of broad momentum spam

### What "high conviction" should mean

Use this as the working spec unless product direction changes:

- Token has actually graduated or hit the specific on-chain milestone being tracked
- Liquidity is real and visible
- Market data confirms tradeability on DEX venues
- Volume/participation is not purely one-wallet noise
- Basic token identity is resolved cleanly enough to avoid symbol confusion
- Alert quality is high enough that posting less often is acceptable

### Relevant current behavior

The current active execution bot already has strong token-resolution guardrails in `execution_bot/main.py`:

- Core aliases for `SOL`, `USDC`, and `USD`
- Jupiter Tokens V2 search as the preferred resolver
- DexScreener as fallback
- Ambiguity handling when multiple tokens share a ticker
- Mint-address-first precision when needed

That existing resolution path is valuable. Reuse it mentally as the standard for "don’t post sloppy token identities."

## Future Wiring

The long-term architecture should let this repo call into Rudis AI rather than duplicating backend intelligence locally.

Expected future wiring areas:

- Rudis API endpoints for richer token, market, and execution context
- Slash commands beyond `/prepare`
- An execution bridge that hands off prepared opportunities into the next Rudis layer
- Stronger crypto filtering using backend-enriched signals instead of only local heuristics

Practical guidance:

- Keep the Discord bot thin where possible
- Let `rudis-ai` own the heavier market intelligence over time
- Keep response formatting and operator UX in this repo

## What Not To Touch

Preserve these unless there is an explicit decision to change them:

- Existing signal formatting patterns in the legacy scanners
- Existing shell scripts used to launch the bot processes
- Token resolution logic already working in `execution_bot/main.py`
- The non-custodial execution flow that opens Jupiter instead of trying to custody execution in-bot

More specifically:

- Do not rewrite the bot into another language/framework just because it feels cleaner
- Do not casually replace the current token resolver with a simpler but less reliable one
- Do not remove the channel/user safety gates on `/prepare`
- Do not collapse current stock and crypto behavior into one generic undifferentiated feed

## Current Behavior Snapshot

As of this repo state, the execution bot:

- Syncs slash commands to a guild when `DISCORD_GUILD_ID` is set
- Otherwise syncs globally
- Uses `/prepare` as the main command
- Produces an "Execution Ticket" embed
- Shows pair, size, estimated out, price impact, slippage, mode, and fee
- Uses a Jupiter link button for non-custodial routing

That means the right development posture is:

- extend carefully
- preserve working UX
- improve routing and signal quality
- wire into Rudis AI progressively instead of rewriting from scratch

## Recommended First Tasks For a New Dev

1. Stand up the bot cleanly in the current Python flow without changing architecture.
2. Mirror the current shell-based runtime under PM2 on Oracle Free Tier.
3. Keep `/prepare` stable and verify Discord command sync plus allowlist behavior.
4. Formalize stock and crypto channel routing in config.
5. Build the crypto high-conviction filter around pump.fun graduation plus DEX Screener/Helius.
6. Add Rudis AI endpoint integration behind clean interfaces rather than scattering API calls through the bot.

## Bottom Line

This repo already has working pieces worth protecting. The assignment is not "build a brand-new bot." The assignment is "keep the current Python bot intact, improve routing and crypto selectivity, and wire it into the larger Rudis system without breaking the pieces that already work."
