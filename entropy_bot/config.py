from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

from entropy_bot.coins import validate_coin_list
from entropy_bot.errors import ConfigError, LiveGuardError
from entropy_bot.fees import (
    DEFAULT_ENTROPY_REFERRAL_REWARD,
    DEFAULT_ENTROPY_REFERRED_USER_BENEFIT,
    DEFAULT_ENTROPY_SELF_REBATE,
    DEFAULT_ENTROPY_TIER,
    DEFAULT_FEE_TIER,
    entropy_program,
    rates_for_tier,
)

DEFAULT_API_URL = "https://api.hyperliquid.xyz"
DEFAULT_WS_URL = "wss://api.hyperliquid.xyz/ws"
DEFAULT_NOTIONAL = 50.0
DEFAULT_OFFSET_TICKS = 2
DEFAULT_MAX_LEVERAGE = 2
DEFAULT_MIN_REPLACE_S = 12.0
MIN_ORDER_USD = 10.0
# Current ops: SNDK only. ANTH stays supported via COINS=io:ANTH,io:SNDK.
DEFAULT_COINS: tuple[str, ...] = ("io:SNDK",)
DEFAULT_RTH_ONLY_COINS: tuple[str, ...] = ("io:SNDK",)
DEFAULT_REQUOTE_TOLERANCE_TICKS = 1
DEFAULT_STALE_QUOTE_S = 120.0
DEFAULT_DEADMAN_AHEAD_S = 60.0
MIN_DEADMAN_AHEAD_S = 10.0
DEFAULT_TAKE_SLIPPAGE_TICKS = 2
DEFAULT_ORACLE_MAX_DEV_BPS = 50.0
DEFAULT_MAX_POSITION_MULT = 3.0
DEFAULT_REQUEST_BUDGET_RESERVE = 500
DEFAULT_STOP_FLATTEN_S = 30.0
OFFICIAL_HOST_SUFFIX = "hyperliquid.xyz"


@dataclass(frozen=True)
class Settings:
    live: bool
    private_key: str | None
    account: str | None
    coins: tuple[str, ...]
    quote_notional_usd: float
    quote_offset_ticks: int
    max_leverage: int
    api_url: str
    ws_url: str
    fee_tier: int = DEFAULT_FEE_TIER
    referral_discount: float = 0.0
    maker_rebate_bps: float | None = None
    entropy_tier: int = DEFAULT_ENTROPY_TIER
    entropy_self_rebate: float = DEFAULT_ENTROPY_SELF_REBATE
    entropy_referral_reward: float = DEFAULT_ENTROPY_REFERRAL_REWARD
    entropy_referred_user_benefit: float = DEFAULT_ENTROPY_REFERRED_USER_BENEFIT
    min_replace_s: float = DEFAULT_MIN_REPLACE_S
    # Keep a resting maker quote while it is within this many ticks of the target.
    requote_tolerance_ticks: int = DEFAULT_REQUOTE_TOLERANCE_TICKS
    # Refresh an untouched flat quote after this long. 0 disables.
    stale_quote_s: float = DEFAULT_STALE_QUOTE_S
    # Account-wide scheduleCancel horizon. Renewed when a third of it remains.
    deadman_ahead_s: float = DEFAULT_DEADMAN_AHEAD_S
    # Flatten IOC crosses this many ticks past the near touch.
    take_slippage_ticks: int = DEFAULT_TAKE_SLIPPAGE_TICKS
    # Coins that only open quotes in NYSE regular hours. Flatten runs any time.
    rth_only_coins: tuple[str, ...] = DEFAULT_RTH_ONLY_COINS
    # No opening quotes while |book mid - oraclePx| exceeds this. 0 disables.
    oracle_max_dev_bps: float = DEFAULT_ORACLE_MAX_DEV_BPS
    # |position| notional that skips the ALO ladder and goes straight to IOC.
    # None → DEFAULT_MAX_POSITION_MULT x QUOTE_NOTIONAL_USD.
    max_position_usd: float | None = None
    # Stop opening quotes for the rest of the UTC day below this net PnL. 0 disables.
    max_daily_loss_usd: float = 0.0
    # Stop new opening quotes when nRequestsCap - nRequestsUsed falls below this.
    request_budget_reserve: int = DEFAULT_REQUEST_BUDGET_RESERVE
    # On stop: reduce-only IOC the position flat for up to this long before
    # canceling and exiting. 0 = cancel only (position is left open).
    stop_flatten_s: float = DEFAULT_STOP_FLATTEN_S

    @property
    def paper(self) -> bool:
        return not self.live


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _optional_env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _coin_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if not parts:
        return ()
    return validate_coin_list(parts)


def _official_url(url: str, schemes: tuple[str, ...]) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in schemes:
        return False
    return host == OFFICIAL_HOST_SUFFIX or host.endswith("." + OFFICIAL_HOST_SUFFIX)


def load_settings(dotenv_path: str | Path | None = None) -> Settings:
    load_dotenv(dotenv_path, override=False)
    coins_raw = os.environ.get("COINS", ",".join(DEFAULT_COINS))
    coins = validate_coin_list(part.strip() for part in coins_raw.split(",") if part.strip())
    rth_only = _coin_env("RTH_ONLY_COINS", DEFAULT_RTH_ONLY_COINS)
    try:
        notional = float(os.environ.get("QUOTE_NOTIONAL_USD", DEFAULT_NOTIONAL))
        offset = int(os.environ.get("QUOTE_OFFSET_TICKS", DEFAULT_OFFSET_TICKS))
        max_lev = int(os.environ.get("MAX_LEVERAGE", DEFAULT_MAX_LEVERAGE))
        fee_tier = int(os.environ.get("FEE_TIER", DEFAULT_FEE_TIER))
        referral = float(os.environ.get("REFERRAL_DISCOUNT", "0") or 0)
        rebate_raw = _optional_env("MAKER_REBATE_BPS")
        rebate_bps = float(rebate_raw) if rebate_raw is not None else None
        entropy_tier = int(os.environ.get("ENTROPY_TIER", DEFAULT_ENTROPY_TIER))
        self_rebate_raw = _optional_env("ENTROPY_SELF_REBATE")
        entropy_self_rebate = (
            float(self_rebate_raw) if self_rebate_raw is not None else DEFAULT_ENTROPY_SELF_REBATE
        )
        reward_raw = _optional_env("ENTROPY_REFERRAL_REWARD")
        entropy_referral_reward = (
            float(reward_raw) if reward_raw is not None else DEFAULT_ENTROPY_REFERRAL_REWARD
        )
        referred_raw = _optional_env("ENTROPY_REFERRED_USER_BENEFIT")
        entropy_referred = (
            float(referred_raw) if referred_raw is not None else DEFAULT_ENTROPY_REFERRED_USER_BENEFIT
        )
        min_replace_s = float(os.environ.get("MIN_REPLACE_S", DEFAULT_MIN_REPLACE_S))
        tol_ticks = int(os.environ.get("REQUOTE_TOLERANCE_TICKS", DEFAULT_REQUOTE_TOLERANCE_TICKS))
        stale_quote_s = float(os.environ.get("STALE_QUOTE_S", DEFAULT_STALE_QUOTE_S))
        deadman_ahead_s = float(os.environ.get("DEADMAN_AHEAD_S", DEFAULT_DEADMAN_AHEAD_S))
        take_slip = int(os.environ.get("TAKE_SLIPPAGE_TICKS", DEFAULT_TAKE_SLIPPAGE_TICKS))
        oracle_dev = float(os.environ.get("ORACLE_MAX_DEV_BPS", DEFAULT_ORACLE_MAX_DEV_BPS))
        max_pos_raw = _optional_env("MAX_POSITION_USD")
        max_pos = float(max_pos_raw) if max_pos_raw is not None else None
        max_loss = float(os.environ.get("MAX_DAILY_LOSS_USD", "0") or 0)
        budget_reserve = int(os.environ.get("REQUEST_BUDGET_RESERVE", DEFAULT_REQUEST_BUDGET_RESERVE))
        stop_flatten_s = float(os.environ.get("STOP_FLATTEN_S", DEFAULT_STOP_FLATTEN_S))
    except ValueError as exc:
        raise ConfigError(f"invalid numeric config: {exc}") from exc
    if notional <= 0 or offset < 0 or max_lev < 1:
        raise ConfigError("QUOTE_NOTIONAL_USD, QUOTE_OFFSET_TICKS, MAX_LEVERAGE must be positive")
    if min_replace_s < 0:
        raise ConfigError("MIN_REPLACE_S must be >= 0")
    if tol_ticks < 0 or take_slip < 0 or stale_quote_s < 0 or oracle_dev < 0 or budget_reserve < 0:
        raise ConfigError(
            "REQUOTE_TOLERANCE_TICKS, TAKE_SLIPPAGE_TICKS, STALE_QUOTE_S, "
            "ORACLE_MAX_DEV_BPS, REQUEST_BUDGET_RESERVE must be >= 0"
        )
    if deadman_ahead_s < MIN_DEADMAN_AHEAD_S:
        raise ConfigError(f"DEADMAN_AHEAD_S must be >= {MIN_DEADMAN_AHEAD_S:g}")
    if max_pos is not None and max_pos <= 0:
        raise ConfigError("MAX_POSITION_USD must be > 0 when set")
    if stop_flatten_s < 0:
        raise ConfigError("STOP_FLATTEN_S must be >= 0 (0 = cancel only, leave the position)")
    if max_loss < 0:
        raise ConfigError("MAX_DAILY_LOSS_USD is a positive loss limit (0 disables)")
    rates_for_tier(fee_tier)
    entropy_program(entropy_tier)
    if not 0.0 <= referral < 1.0:
        raise ConfigError("REFERRAL_DISCOUNT must be in [0, 1)")
    if entropy_self_rebate < 0 or entropy_referral_reward < 0 or entropy_referred < 0:
        raise ConfigError("Entropy rebate rates must be >= 0")
    api_url = _optional_env("HYPERLIQUID_API_URL") or DEFAULT_API_URL
    ws_url = _optional_env("HYPERLIQUID_WS_URL") or DEFAULT_WS_URL
    if not _official_url(api_url, ("https",)) or not _official_url(ws_url, ("wss",)):
        raise ConfigError("only official Hyperliquid HTTP/WS endpoints are allowed")
    return Settings(
        live=_truthy(os.environ.get("LIVE")),
        private_key=_optional_env("HYPERLIQUID_PRIVATE_KEY"),
        account=_optional_env("HYPERLIQUID_ACCOUNT"),
        coins=coins,
        quote_notional_usd=notional,
        quote_offset_ticks=offset,
        max_leverage=max_lev,
        api_url=api_url.rstrip("/"),
        ws_url=ws_url,
        fee_tier=fee_tier,
        referral_discount=referral,
        maker_rebate_bps=rebate_bps,
        entropy_tier=entropy_tier,
        entropy_self_rebate=entropy_self_rebate,
        entropy_referral_reward=entropy_referral_reward,
        entropy_referred_user_benefit=entropy_referred,
        min_replace_s=min_replace_s,
        requote_tolerance_ticks=tol_ticks,
        stale_quote_s=stale_quote_s,
        deadman_ahead_s=deadman_ahead_s,
        take_slippage_ticks=take_slip,
        rth_only_coins=rth_only,
        oracle_max_dev_bps=oracle_dev,
        max_position_usd=max_pos,
        max_daily_loss_usd=max_loss,
        request_budget_reserve=budget_reserve,
        stop_flatten_s=stop_flatten_s,
    )


def require_live(settings: Settings) -> None:
    if not settings.live:
        raise LiveGuardError("live trading refused: set LIVE=1 (paper is the default)")
    if not settings.private_key:
        raise LiveGuardError("live trading refused: HYPERLIQUID_PRIVATE_KEY is not set")


def require_signer(settings: Settings) -> None:
    if not settings.private_key:
        raise LiveGuardError("signing refused: HYPERLIQUID_PRIVATE_KEY is not set")


def live_notional(settings: Settings) -> float:
    """Default ~$50/side, never below the $10 exchange minimum."""
    return max(MIN_ORDER_USD, settings.quote_notional_usd)


def max_position_usd(settings: Settings) -> float:
    if settings.max_position_usd is not None:
        return settings.max_position_usd
    return live_notional(settings) * DEFAULT_MAX_POSITION_MULT
