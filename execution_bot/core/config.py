from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)


def _parse_optional_int(value: str | None) -> int | None:
    raw = (value or "").strip()
    return int(raw) if raw.isdigit() else None


def _parse_int(value: str | None, default: int) -> int:
    raw = (value or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_id_set(env_name: str) -> set[int]:
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return set()

    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


@dataclass(frozen=True)
class Settings:
    discord_token: str
    guild_id: int | None
    log_channel_id: int | None
    execution_mode: str
    platform_fee_bps: int
    slippage_bps: int
    allowed_channel_ids: set[int]
    allowed_user_ids: set[int]
    rudis_execution_context_url: str
    rudis_execution_context_timeout_sec: int
    bot_label: str

    @property
    def has_rudis_context(self) -> bool:
        return bool(self.rudis_execution_context_url)


def load_settings() -> Settings:
    discord_token = (os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    if not discord_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is missing in .env")

    execution_mode = (os.getenv("EXECUTION_MODE", "route") or "route").strip().lower()
    if execution_mode not in {"route", "preview"}:
        execution_mode = "route"

    return Settings(
        discord_token=discord_token,
        guild_id=_parse_optional_int(os.getenv("DISCORD_GUILD_ID")),
        log_channel_id=_parse_optional_int(os.getenv("DISCORD_LOG_CHANNEL_ID")),
        execution_mode=execution_mode,
        platform_fee_bps=_parse_int(os.getenv("PLATFORM_FEE_BPS"), default=15),
        slippage_bps=_parse_int(os.getenv("SLIPPAGE_BPS"), default=50),
        allowed_channel_ids=_parse_id_set("ALLOWED_CHANNEL_IDS"),
        allowed_user_ids=_parse_id_set("ALLOWED_USER_IDS"),
        rudis_execution_context_url=(os.getenv("RUDIS_EXECUTION_CONTEXT_URL") or "").strip(),
        rudis_execution_context_timeout_sec=_parse_int(os.getenv("RUDIS_EXECUTION_CONTEXT_TIMEOUT_SEC"), default=6),
        bot_label=(os.getenv("RUDIS_BOT_LABEL") or "Rudis").strip() or "Rudis",
    )


settings = load_settings()
