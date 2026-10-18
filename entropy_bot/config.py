from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

from entropy_bot.coins import ALLOWED_COINS, validate_coin_list
from entropy_bot.errors import ConfigError, LiveGuardError
from entropy_bot.precision import MIN_NOTIONAL_USD
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
DEFAULT_REQUOTE_TOLERANCE_TICKS = 1
DEFAULT_STALE_QUOTE_S = 120.0
DEFAULT_TAKE_SLIPPAGE_TICKS = 0
DEFAULT_MAX_POSITION_MULT = 3.0
DEFAULT_ORACLE_MAX_DEV_BPS = 50.0
MIN_ORDER_USD = MIN_NOTIONAL_USD


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
    # 挂单离目标价不超过 N 个 tick 就不动（省写请求、保留排队位置）。0 = 任何变动都重挂。
    requote_tolerance_ticks: int = DEFAULT_REQUOTE_TOLERANCE_TICKS
    # 空仓挂单超过 N 秒价格没变也刷新一次。0 = 关闭。（原固定 45 秒）
    stale_quote_s: float = DEFAULT_STALE_QUOTE_S
    # 减仓 IOC 比盘口多穿 N 个 tick。0 = 原行为（正好打在买一/卖一）。
    take_slippage_ticks: int = DEFAULT_TAKE_SLIPPAGE_TICKS
    # 仓位名义金额达到这个值就跳过 ALO 阶梯直接 IOC。None = 3 x QUOTE_NOTIONAL_USD。
    max_position_usd: float | None = None
    # 盘口 mid 偏离 oraclePx 超过 N bp 时不挂开仓单（有仓照常平）。0 = 关闭。
    oracle_max_dev_bps: float = DEFAULT_ORACLE_MAX_DEV_BPS
    # 只在纽约正常交易时段挂开仓单的币，如 "io:SNDK"。默认空 = 不限时段。
    rth_only_coins: tuple[str, ...] = ()
    # 本次运行已实现净亏损（closedPnl - fee）达到 N 美元就停止开仓并收尾。0 = 关闭。
    max_session_loss_usd: float = 0.0

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


OFFICIAL_HOST_SUFFIX = "hyperliquid.xyz"


def _official_url(url: str, schemes: tuple[str, ...]) -> bool:
    """按 hostname 精确判断；子串判断会放过 https://evil.example/hyperliquid.xyz。"""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in schemes:
        return False
    return host == OFFICIAL_HOST_SUFFIX or host.endswith("." + OFFICIAL_HOST_SUFFIX)


def load_settings(dotenv_path: str | Path | None = None) -> Settings:
    load_dotenv(dotenv_path, override=False)
    coins_raw = os.environ.get("COINS", ",".join(ALLOWED_COINS))
    coins = validate_coin_list(part.strip() for part in coins_raw.split(",") if part.strip())
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
        take_slip = int(os.environ.get("TAKE_SLIPPAGE_TICKS", DEFAULT_TAKE_SLIPPAGE_TICKS))
        max_pos_raw = _optional_env("MAX_POSITION_USD")
        max_pos = float(max_pos_raw) if max_pos_raw is not None else None
        oracle_dev = float(os.environ.get("ORACLE_MAX_DEV_BPS", DEFAULT_ORACLE_MAX_DEV_BPS))
        max_loss = float(os.environ.get("MAX_SESSION_LOSS_USD", "0") or 0)
    except ValueError as exc:
        raise ConfigError(f"invalid numeric config: {exc}") from exc
    if notional <= 0 or offset < 0 or max_lev < 1:
        raise ConfigError("QUOTE_NOTIONAL_USD, QUOTE_OFFSET_TICKS, MAX_LEVERAGE must be positive")
    if min_replace_s < 0:
        raise ConfigError("MIN_REPLACE_S must be >= 0")
    if tol_ticks < 0 or stale_quote_s < 0 or take_slip < 0:
        raise ConfigError("REQUOTE_TOLERANCE_TICKS, STALE_QUOTE_S, TAKE_SLIPPAGE_TICKS must be >= 0")
    if max_pos is not None and max_pos <= 0:
        raise ConfigError("MAX_POSITION_USD must be > 0 when set")
    if oracle_dev < 0 or max_loss < 0:
        raise ConfigError("ORACLE_MAX_DEV_BPS and MAX_SESSION_LOSS_USD must be >= 0")
    rth_raw = os.environ.get("RTH_ONLY_COINS", "")
    rth_only = validate_coin_list(c.strip() for c in rth_raw.split(",") if c.strip()) if rth_raw.strip() else ()
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
        take_slippage_ticks=take_slip,
        max_position_usd=max_pos,
        oracle_max_dev_bps=oracle_dev,
        rth_only_coins=rth_only,
        max_session_loss_usd=max_loss,
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
