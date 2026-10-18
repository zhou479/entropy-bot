"""Official-API live MM. Port of entropy-desk.user.js 1.6.1.

Talks only to https://api.hyperliquid.xyz and wss://api.hyperliquid.xyz/ws.
No Tampermonkey / window.ethereum. Per coin: at most 1 buy + 1 sell.

Dead-man `scheduleCancel` is **account-wide**: when it fires it cancels every
open order on the master account, not just io:ANTH / io:SNDK.
"""

from __future__ import annotations

import logging
import math
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from entropy_bot.coins import ALLOWED_DEX, Market, perp_dex_info
from entropy_bot.config import Settings, live_notional, max_position_usd, require_live
from entropy_bot.fees import (
    ALO_TIF_NOTE,
    alo_fill_rebate_usd,
    describe_fee_model,
    estimate_from_settings,
    fee_for_notional,
)
from entropy_bot.orders import (
    LiveSigner,
    action_ok,
    as_oid,
    format_fee_banner,
    is_alo_reject,
    is_bot_cloid,
    resp_statuses,
    status_error,
    status_oid,
)
from entropy_bot.quoting import (
    FLAT_TAKE_MS,
    BookTop,
    QuotePlan,
    book_top,
    quote_plan,
    sync_pos_since,
)
from entropy_bot.diagnostics import BookSnap, FillDiagnostics, snap_from_top
from entropy_bot.errors import (ExchangeOutcomeUnknown, LiveGuardError, RateLimited,
                                RequestWeightLimited, is_weight_limit_error)
from entropy_bot.safety import (BUDGET_POLL_S, HTTP_BACKOFF_S, LIMITED_EXIT_GAP_S,
                                RequestBudget, cancel_confirmed)
from entropy_bot.market_hours import is_rth
from entropy_bot.rest import InfoClient
from entropy_bot.status import load_io_markets
from entropy_bot.session import SupervisedSession
from entropy_bot.ws import BookFeed

log = logging.getLogger("entropy_bot.live")

WS_STALE_S = 15.0
ORACLE_STALE_S = 30.0  # activeAssetCtx 超过这么久没更新，就不拿它做偏离检查
POS_POLL_S = 0.8
FILL_POLL_S = 30.0  # WS 保持逐笔诊断；REST 仅低频补漏，避免抢占查询权重。
LOOP_S = 0.4
DEADMAN_AHEAD_MS = 20_000
DEADMAN_MIN_MS = 5_000
DEADMAN_FALLBACK_MS = 6_000
DEADMAN_REMAIN_LT_S = 8.0
DEADMAN_REFRESH_S = DEADMAN_REMAIN_LT_S  # alias: refresh only when remaining < 8s
IOC_GAP_S = 3.0  # 首次退出仍是 >=15s；后续重试共享节流，避免 info 查询耗尽 IP 权重。
WEIGHT_BACKOFF_FLOOR_S = 30.0
ALO_REJECT_RETRY_S = 2.0
# 其它拒单（保证金不足、builder 未授权、数量不对……）按侧退避 2s、4s、…、最长 60s，
# 每轮都重试同一个必拒的单只会消耗请求额度。
ORDER_REJECT_BASE_S = 2.0
ORDER_REJECT_MAX_S = 60.0


@dataclass
class RestSlot:
    oid: int | None = None
    cloid: str | None = None
    px: float | None = None
    sz: float | None = None
    tif: str = "Alo"
    reduce_only: bool = False
    placed_at: float = 0.0
    stage: str = "flat"

    def occupied(self) -> bool:
        return self.oid is not None or self.cloid is not None


def empty_rests(coins: tuple[str, ...]) -> dict[str, dict[str, RestSlot]]:
    return {coin: {"B": RestSlot(), "A": RestSlot()} for coin in coins}


def order_side_buy(order: dict[str, Any]) -> bool | None:
    side = order.get("side")
    if side in {"B", "b"}:
        return True
    if side in {"A", "S", "a", "s"}:
        return False
    if order.get("b") is True:
        return True
    if order.get("b") is False:
        return False
    return None


def match_order_coin(order: dict[str, Any], markets: dict[str, Market]) -> str | None:
    raw = str(order.get("coin") or "")
    if raw.startswith(("xyz:", "vntl:")):
        return None
    if raw in markets:
        return raw
    aid_raw = order.get("a", order.get("asset"))
    aid: int | None
    try:
        aid = int(aid_raw) if aid_raw is not None else None
    except (TypeError, ValueError):
        aid = None
    for coin, market in markets.items():
        base = coin.split(":", 1)[1]
        if raw == base or raw == f"@{market.asset_id}" or raw == str(market.asset_id):
            return coin
        if aid is not None and aid == market.asset_id:
            return coin
    return None


def apply_open_orders(
    rests: dict[str, dict[str, RestSlot]],
    extras: dict[str, list[int]],
    opens: list[dict[str, Any]] | None,
    markets: dict[str, Market],
) -> tuple[dict[str, dict[str, RestSlot]], dict[str, list[int]]]:
    """Rebuild rest cache from frontendOpenOrders. None/429 must not wipe cache."""
    if opens is None:
        log.warning("open-order query null/429; keeping local rest cache")
        return rests, extras
    next_rests = empty_rests(tuple(markets))
    next_extras: dict[str, list[int]] = {coin: [] for coin in markets}
    # 已知挂单保留阶段/方向/下单时间等元数据；只有交易所列表决定它是否还在。
    known_by_oid: dict[int, RestSlot] = {}
    known_by_cloid: dict[str, RestSlot] = {}
    for sides in rests.values():
        for old in sides.values():
            if old.oid is not None:
                known_by_oid[old.oid] = old
            if old.cloid:
                known_by_cloid[old.cloid] = old
    for order in opens:
        coin = match_order_coin(order, markets)
        if coin is None or coin not in next_rests:
            continue
        oid_raw = order.get("oid", order.get("o"))
        if oid_raw is None:
            continue
        try:
            oid = as_oid(oid_raw)
        except ValueError:
            continue
        is_buy = order_side_buy(order)
        side = "B" if is_buy is True else "A" if is_buy is False else None
        if side is None:
            continue
        slot = next_rests[coin][side]
        cloid = order.get("cloid") or order.get("cloidStr")
        if not slot.occupied():
            old = known_by_oid.get(oid) or (known_by_cloid.get(cloid) if isinstance(cloid, str) else None)
            if old is not None:
                slot.sz, slot.tif, slot.reduce_only = old.sz, old.tif, old.reduce_only
                slot.placed_at, slot.stage = old.placed_at, old.stage
            else:
                slot.stage = "unknown"  # 不知道是哪一阶段的单：下一轮按"类型不对"立即替换
            slot.oid = oid
            slot.cloid = cloid if isinstance(cloid, str) else None
            try:
                slot.px = float(order.get("limitPx") or order.get("px") or 0) or None
            except (TypeError, ValueError):
                slot.px = None
        else:
            next_extras[coin].append(oid)
    for coin, extra_oids in next_extras.items():
        if extra_oids:
            log.info("extra rests %s oids=%s", coin, extra_oids)
    return next_rests, next_extras


def list_cached_oids(
    rests: dict[str, dict[str, RestSlot]],
    extras: dict[str, list[int]],
    coin: str,
) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for side in ("B", "A"):
        slot = rests.get(coin, {}).get(side)
        if slot and slot.oid is not None and slot.oid not in seen:
            seen.add(slot.oid)
            out.append(slot.oid)
    for oid in extras.get(coin, []):
        if oid not in seen:
            seen.add(oid)
            out.append(oid)
    return out


def list_cached_cloids(rests: dict[str, dict[str, RestSlot]], coin: str) -> list[str]:
    out: list[str] = []
    for side in ("B", "A"):
        slot = rests.get(coin, {}).get(side)
        if slot and slot.cloid and slot.oid is None:
            out.append(slot.cloid)
    return out


def clear_coin_rests(rests: dict[str, dict[str, RestSlot]], coin: str) -> None:
    rests[coin] = {"B": RestSlot(), "A": RestSlot()}


def positions_from_state(
    state: dict[str, Any] | None,
    coins: tuple[str, ...],
    markets: dict[str, Market],
) -> dict[str, float] | None:
    if not isinstance(state, dict):
        return None
    out = {coin: 0.0 for coin in coins}
    rows = state.get("assetPositions")
    if not isinstance(rows, list):
        return None
    for item in rows:
        if not isinstance(item, dict) or not isinstance(item.get("position"), dict):
            return None
        pos = (item or {}).get("position") or {}
        coin = match_order_coin({"coin": pos.get("coin"), "a": None}, markets)
        if coin is None or coin not in out:
            raw = str(pos.get("coin") or "")
            if raw in out:
                coin = raw
            else:
                continue
        try:
            out[coin] = float(pos["szi"])
            if not math.isfinite(out[coin]):
                return None
        except (KeyError, TypeError, ValueError):
            return None
    return out


def deadman_deadline_ms(ahead_ms: int, *, now_ms: int | None = None) -> int:
    now = int(time.time() * 1000) if now_ms is None else now_ms
    t = now + int(ahead_ms)
    if t - now < DEADMAN_MIN_MS:
        t = now + DEADMAN_FALLBACK_MS
    return t


def weight_backoff_s(min_replace_s: float, floor_s: float = WEIGHT_BACKOFF_FLOOR_S) -> float:
    """Signed-write pause after a cumulative request-weight error. Waiting does not grow the cap."""
    return max(float(min_replace_s), float(floor_s))


def replace_allowed(
    *,
    take: bool,
    has_rest: bool,
    elapsed_s: float,
    min_replace_s: float,
) -> bool:
    """Throttle mid/far/flat cancel+replace. Flatten ≥15s IOC take is always allowed."""
    if take:
        return True
    if not has_rest:
        return True
    return elapsed_s >= min_replace_s


def deadman_needs_refresh(
    deadline_s: float,
    now_s: float,
    remain_lt_s: float = DEADMAN_REMAIN_LT_S,
) -> bool:
    """Renew scheduleCancel only when unset or remaining time is under 8s."""
    if deadline_s <= 0:
        return True
    return (deadline_s - now_s) < remain_lt_s


def side_occupied(rests: dict[str, dict[str, RestSlot]], coin: str, side: str) -> bool:
    slot = rests.get(coin, {}).get(side)
    return bool(slot and slot.occupied())


def slot_verdict(
    slot: RestSlot,
    want: Any,
    *,
    tick: float,
    tolerance_ticks: int,
    stale_quote_s: float,
    now: float,
) -> str:
    """一侧已有挂单、且这一侧仍需要挂单时怎么处理。

    - "keep"：离目标价不超过 tolerance_ticks 个 tick，不动（不花写请求，保留排队位置）。
    - "now"：挂单类型不对（阶段 / TIF / reduce-only 变了），立即替换，不受 MIN_REPLACE_S 限制。
      例如成交后空仓变多仓：整额的非减仓卖单必须马上换成 reduce-only。
    - "slow"：只是价格偏离超过容差，或空仓挂单超过 stale_quote_s 未动；按 MIN_REPLACE_S 节流替换。
    """
    if (
        slot.stage != want.stage
        or bool(slot.reduce_only) != bool(want.reduce_only)
        or str(slot.tif).lower() != str(want.tif).lower()
    ):
        return "now"
    if slot.px is None:
        return "slow"
    if abs(slot.px - want.px) > max(0, int(tolerance_ticks)) * tick + 1e-9:
        return "slow"
    if want.reduce_only and slot.sz is not None and slot.sz < want.sz - 1e-12:
        return "slow"  # 仓位变大了，减仓单数量跟着调
    if (want.stage == "flat" and stale_quote_s > 0 and slot.placed_at > 0
            and now - slot.placed_at > stale_quote_s):
        return "slow"
    return "keep"


@dataclass
class LiveQuoter:
    settings: Settings
    markets: dict[str, Market]
    client: InfoClient
    signer: LiveSigner
    notional: float = 0.0
    rests: dict[str, dict[str, RestSlot]] = field(default_factory=dict)
    extra_oids: dict[str, list[int]] = field(default_factory=dict)
    pos: dict[str, float] = field(default_factory=dict)
    pos_since: dict[str, float] = field(default_factory=dict)
    last_plan_key: dict[str, str] = field(default_factory=dict)
    quote_placed_at: dict[str, float] = field(default_factory=dict)
    last_take_at: dict[str, float] = field(default_factory=dict)
    last_pos_poll: float = 0.0
    last_fill_poll: float = 0.0
    last_deadman: float = 0.0
    last_replace: dict[str, float] = field(default_factory=dict)
    deadman_until: float = 0.0
    _weight_backoff_until: float = 0.0
    _weight_logged: bool = False
    _replace_skip_logged: dict[str, bool] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    books: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict)
    diag: FillDiagnostics | None = None
    _feed: BookFeed | None = None
    budget: RequestBudget = field(default_factory=RequestBudget)
    last_budget_poll: float = 0.0
    positions_valid: bool = False
    last_position_success: float = 0.0
    entry_halt: str = ""
    unknown_write: bool = False
    http_backoff_until: float = 0.0
    next_exit_at: float = 0.0
    next_cancel_at: float = 0.0
    next_deadman_at: float = 0.0
    managed_started: bool = False
    shutting_down: bool = False
    _fill_generation: int = 0
    _position_generation: int = -1
    next_safety_poll: dict[str, float] = field(default_factory=dict)
    session: SupervisedSession | None = None
    _reject_until: dict[tuple[str, str], float] = field(default_factory=dict)
    _reject_streak: dict[tuple[str, str], int] = field(default_factory=dict)
    _maxpos_logged: dict[str, bool] = field(default_factory=dict)
    asset_ctx: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict)
    _block_logged: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.notional:
            self.notional = live_notional(self.settings)
        coins = tuple(self.markets)
        if not self.rests:
            self.rests = empty_rests(coins)
        if not self.extra_oids:
            self.extra_oids = {coin: [] for coin in coins}
        if not self.pos:
            self.pos = {coin: 0.0 for coin in coins}
        if not self.pos_since:
            self.pos_since = {coin: 0.0 for coin in coins}
        if not self.last_replace:
            self.last_replace = {coin: 0.0 for coin in coins}
        if not self._replace_skip_logged:
            self._replace_skip_logged = {coin: False for coin in coins}
        if self.diag is None:
            self.diag = FillDiagnostics(coins)

    def _in_weight_backoff(self) -> bool:
        return time.time() < self._weight_backoff_until

    def _trip_weight_backoff(self, err: object) -> None:
        self.halt_entries("cumulative request limit; manual review required before restart")
        self.budget.valid = False
        self.next_exit_at = max(self.next_exit_at, time.time() + LIMITED_EXIT_GAP_S)
        wait = weight_backoff_s(self.settings.min_replace_s)
        self._weight_backoff_until = time.time() + wait
        if not self._weight_logged:
            log.warning(
                "request weight exhausted; entries latched OFF, maintenance backoff %.0fs "
                "(cancel/exit use separate paced paths; waiting does not restore cap): %s",
                wait,
                err,
            )
            self._weight_logged = True

    def halt_entries(self, reason: str) -> None:
        if not self.entry_halt:
            self.entry_halt = reason
            log.error("SAFETY entry_halt=%s", reason)

    def raise_if_halted_and_flat(self) -> None:
        """严格模式安全拒绝后，空仓时进入最终清理；持仓管理仍继续。"""
        if not self.entry_halt or self.unknown_write:
            return
        if not self.position_fresh() or any(self.pos.values()):
            return
        if any(self._has_rest(c) or self.extra_oids.get(c) for c in self.markets):
            return
        raise LiveGuardError(f"SAFETY STOP: {self.entry_halt}; final account checks will follow")

    def session_account_clear(self) -> bool:
        """停机期间先查挂单，再查仓位，不能把撤单确认等同于平仓。"""
        if self.unknown_write:
            return False
        try:
            opens = self.client.open_orders(self.signer.account, ALLOWED_DEX, optional=True)
            if opens != []:
                return False
            self.refresh_positions(force=True)
            return self.position_fresh() and not any(self.pos.values())
        except Exception as exc:  # noqa: BLE001
            log.warning("SESSION final state query failed: %s", exc)
            return False

    def refresh_budget(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_budget_poll < BUDGET_POLL_S:
            return
        self.last_budget_poll = now
        try:
            raw = self.client.user_rate_limit(self.signer.account)
            self.budget.update(raw, time.time())
        except Exception:
            self.budget.valid = False
        if self.budget.valid and not self.budget.can_enter(time.time(), len(self.markets)):
            self.halt_entries("request reserve too low; no automatic quota purchase")

    def position_fresh(self) -> bool:
        with self._lock:
            unchanged = self._position_generation == self._fill_generation
        return (self.positions_valid and unchanged
                and 0 <= time.time() - self.last_position_success <= 3.0)

    def entry_allowed(self) -> bool:
        return (not self.entry_halt and not self.unknown_write and not self.shutting_down
                and (self.session is None or self.session.can_enter())
                and self.position_fresh()
                and self.budget.can_enter(time.time(), len(self.markets)))

    def preflight(self) -> None:
        """启动阶段只有查询；拒绝接管已有仓位/挂单，避免误管手工交易。"""
        self.refresh_positions(force=True)
        opens = self.client.open_orders(self.signer.account, ALLOWED_DEX, optional=True)
        self.refresh_budget(force=True)
        if not self.position_fresh() or opens is None:
            raise LiveGuardError("preflight: account state unavailable; no signed actions sent")
        if any(self.pos.values()) or opens:
            raise LiveGuardError("preflight: verify existing positions/orders manually; start flat and empty")
        if not self.entry_allowed():
            raise LiveGuardError("preflight: request budget unknown/insufficient; no signed actions sent")

    def _mark_replaced(self, coin: str) -> None:
        self.last_replace[coin] = time.time()
        self._replace_skip_logged[coin] = False

    def _has_rest(self, coin: str) -> bool:
        return side_occupied(self.rests, coin, "B") or side_occupied(self.rests, coin, "A")

    def on_book(self, coin: str, data: dict[str, Any]) -> None:
        with self._lock:
            self.books[coin] = (time.time(), data)

    def on_asset_ctx(self, coin: str, ctx: dict[str, Any]) -> None:
        with self._lock:
            self.asset_ctx[coin] = (time.time(), ctx)

    def entry_block_reason(self, coin: str, top: BookTop) -> str:
        """按币暂停开仓的原因（不锁死：条件恢复后自动继续）。有仓位时照常减仓。"""
        if coin in self.settings.rth_only_coins and not is_rth():
            return "outside NYSE regular hours (RTH_ONLY_COINS)"
        max_dev = self.settings.oracle_max_dev_bps
        if max_dev > 0 and top.mid:
            with self._lock:
                entry = self.asset_ctx.get(coin)
            if entry is not None and time.time() - entry[0] <= ORACLE_STALE_S:
                try:
                    oracle = float(entry[1].get("oraclePx"))
                except (TypeError, ValueError):
                    oracle = 0.0
                if oracle > 0 and math.isfinite(oracle):
                    dev = abs(top.mid - oracle) / oracle * 10_000.0
                    if dev > max_dev:
                        return f"book mid {top.mid:g} is {dev:.1f} bp from oracle {oracle:g}"
        return ""

    def _note_block(self, coin: str, reason: str) -> None:
        if self._block_logged.get(coin, "") == reason:
            return
        if reason:
            log.warning("ENTRY PAUSE %s: %s", coin, reason)
        else:
            log.info("ENTRY RESUME %s", coin)
        self._block_logged[coin] = reason

    def check_session_loss(self) -> None:
        limit = self.settings.max_session_loss_usd
        if limit <= 0 or self.diag is None:
            return
        pnl = self.diag.run_net_pnl()
        if pnl <= -limit:
            self.halt_entries(f"session realized net PnL {pnl:.2f} <= -MAX_SESSION_LOSS_USD {limit:g}")

    def peek_book(self, coin: str) -> BookSnap | None:
        """Cache/WS l2Book only. Diagnostics must not open a WS or REST-fallback."""
        now = time.time()
        ts = 0.0
        data: dict[str, Any] | None = None
        with self._lock:
            if coin in self.books:
                ts, data = self.books[coin]
        feed = self._feed
        if feed is not None:
            latest = feed.latest(coin)
            if latest is not None and latest[0] >= ts:
                ts, data = latest
        if data is None:
            return None
        return snap_from_top(book_top(coin, data), now - ts)

    def _diag_quotes(self, coin: str, n: int, *, take: bool) -> None:
        if take or n <= 0 or self.diag is None:
            return
        try:
            self.diag.note_quotes(coin, n)
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag quotes: %s", exc)

    def on_user_fills(self, data: dict[str, Any]) -> None:
        if isinstance(data, dict) and data.get("fills"):
            with self._lock:
                self._fill_generation += 1
        if self.diag is None:
            return
        fills = data.get("fills") if isinstance(data, dict) else None
        if not isinstance(fills, list):
            return
        try:
            self.diag.ingest_fills(fills, self.peek_book, prime=bool(data.get("isSnapshot")))
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag userFills: %s", exc)

    def poll_user_fills(self, *, force: bool = False) -> None:
        if self.diag is None:
            return
        now = time.time()
        if not force and now - self.last_fill_poll < FILL_POLL_S:
            return
        rows = self.client.user_fills(self.signer.account, optional=True)
        self.last_fill_poll = now
        if rows is None:
            return
        try:
            self.diag.ingest_fills(rows, self.peek_book, prime=not self.diag.primed)
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag userFills poll: %s", exc)

    def flush_markouts(self) -> None:
        if self.diag is None:
            return
        try:
            self.diag.flush_markouts(self.peek_book)
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag markout: %s", exc)

    def bootstrap_isolated(self) -> None:
        for market in self.markets.values():
            fee = estimate_from_settings(market, self.settings)
            maker_usd = fee_for_notional(fee, self.notional, maker=True)
            rebate_usd = (
                alo_fill_rebate_usd(self.notional, self.settings.maker_rebate_bps)
                if self.settings.maker_rebate_bps is not None
                else None
            )
            print(
                format_fee_banner(market.coin, fee.summary(), self.notional, maker_usd, rebate_usd),
                flush=True,
            )
            payload = self.signer.signed_update_leverage(market, self.settings.max_leverage)
            resp = self._post(payload)
            if not isinstance(resp, dict) or resp.get("status") != "ok":
                raise LiveGuardError("isolated leverage update rejected; startup stopped")
            log.info("isolated leverage %s -> %s", market.coin, resp)

    def _fee_banner(self, coin: str) -> None:
        market = self.markets[coin]
        fee = estimate_from_settings(market, self.settings)
        maker_usd = fee_for_notional(fee, self.notional, maker=True)
        rebate_usd = (
            alo_fill_rebate_usd(self.notional, self.settings.maker_rebate_bps)
            if self.settings.maker_rebate_bps is not None
            else None
        )
        print(format_fee_banner(coin, fee.summary(), self.notional, maker_usd, rebate_usd), flush=True)

    def refresh_positions(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_pos_poll < POS_POLL_S:
            return
        self.positions_valid = False
        with self._lock:
            generation = self._fill_generation
        state = self.client.clearinghouse_state(self.signer.account, ALLOWED_DEX, optional=True)
        parsed = positions_from_state(state, tuple(self.markets), self.markets)
        if parsed is None:
            log.warning("clearinghouseState null/429; keeping inventory cache")
            if force:
                return
            self.last_pos_poll = now
            return
        self.pos_since = sync_pos_since(self.pos, parsed, self.pos_since, now)
        self.pos = parsed
        self.last_pos_poll = now
        self.last_position_success = time.time()
        self.positions_valid = True
        self._position_generation = generation

    def book_for(self, coin: str, feed: BookFeed | None) -> BookTop | None:
        now = time.time()
        ts = 0.0
        data: dict[str, Any] | None = None
        with self._lock:
            if coin in self.books:
                ts, data = self.books[coin]
        if feed is not None:
            latest = feed.latest(coin)
            if latest is not None and latest[0] >= ts:
                ts, data = latest
        stale = data is None or (now - ts) > WS_STALE_S
        if stale:
            try:
                data = self.client.l2_book(coin)
                ts = time.time()
                with self._lock:
                    self.books[coin] = (ts, data)
            except Exception as exc:  # noqa: BLE001
                log.warning("REST l2Book fallback failed %s: %s", coin, exc)
                return None
        top = book_top(coin, data or {})
        if (top.bid is None or top.ask is None or not math.isfinite(top.bid)
                or not math.isfinite(top.ask) or not 0 < top.bid < top.ask):
            return None
        # 接收时间新鲜，不代表服务器快照新鲜。
        if top.time is not None:
            try:
                age = time.time() - float(top.time) / 1000
                if not math.isfinite(age) or not -5 <= age <= WS_STALE_S:
                    return None
            except (TypeError, ValueError):
                return None
        return top

    def plan_for(self, coin: str, top: BookTop) -> QuotePlan:
        now = time.time()
        szi = self.pos.get(coin, 0.0)
        since = self.pos_since.get(coin, 0.0)
        age_ms = int(max(0.0, (now - since) * 1000)) if since and abs(szi) > 0 else 0
        cap = max_position_usd(self.settings)
        mid = top.mid or 0.0
        if abs(szi) > 0 and mid > 0 and abs(szi) * mid >= cap:
            if not self._maxpos_logged.get(coin):
                log.warning("position %s $%.2f >= MAX_POSITION_USD $%.2f; skip ALO ladder, IOC now",
                            coin, abs(szi) * mid, cap)
                self._maxpos_logged[coin] = True
            age_ms = max(age_ms, FLAT_TAKE_MS)
        else:
            self._maxpos_logged[coin] = False
        return quote_plan(
            self.markets[coin],
            top,
            szi=szi,
            age_ms=age_ms,
            notional_usd=self.notional,
            take_slippage_ticks=self.settings.take_slippage_ticks,
        )

    def _note_reject(self, coin: str, side: str, *, alo: bool) -> float:
        key = (coin, side)
        now = time.time()
        if alo:
            wait = ALO_REJECT_RETRY_S
        else:
            streak = self._reject_streak.get(key, 0)
            wait = min(ORDER_REJECT_MAX_S, ORDER_REJECT_BASE_S * (2 ** streak))
            self._reject_streak[key] = streak + 1
        self._reject_until[key] = max(self._reject_until.get(key, 0.0), now + wait)
        return wait

    def _cooling_down(self, coin: str, side: str) -> bool:
        return time.time() < self._reject_until.get((coin, side), 0.0)

    def _post(self, payload: dict[str, Any], *, purpose: str = "maintenance", actions: int = 1) -> Any:
        now = time.time()
        if now < self.http_backoff_until:
            raise RateLimited("local HTTP backoff")
        if purpose == "entry" and not self.entry_allowed():
            raise RequestWeightLimited("entry safety gate closed")
        if purpose == "exit" and self.session is not None and not self.session.can_exit():
            raise RateLimited("session cleanup deadline passed; no new exit orders")
        if purpose == "exit" and (self.unknown_write or now < self.next_exit_at):
            raise RateLimited("exit awaits reconciliation or paced retry")
        if purpose == "cancel" and now < self.next_cancel_at:
            raise RateLimited("cancel paced retry")
        if purpose == "maintenance" and (self.entry_halt or self._in_weight_backoff()):
            left = self._weight_backoff_until - time.time()
            raise RequestWeightLimited(f"signed write skipped; request-weight backoff {left:.1f}s left")
        if purpose == "exit":
            self.next_exit_at = now + (LIMITED_EXIT_GAP_S if self.entry_halt else IOC_GAP_S)
        self.budget.debit(actions)
        log.info("EXEC action=%s purpose=%s local_debit=%d", payload.get("action", {}).get("type"), purpose, actions)
        try:
            resp = self.client.post_exchange(payload)
        except RequestWeightLimited as exc:
            self._trip_weight_backoff(exc)
            if purpose == "cancel":
                self.next_cancel_at = time.time() + LIMITED_EXIT_GAP_S
            raise
        except RateLimited as exc:
            self.http_backoff_until = time.time() + HTTP_BACKOFF_S
            if is_weight_limit_error(exc):
                self._trip_weight_backoff(exc)
            raise
        except Exception as exc:  # noqa: BLE001
            self.unknown_write = True
            self.halt_entries("exchange write outcome unknown; reconcile manually, do not restart blindly")
            if is_weight_limit_error(exc):
                self._trip_weight_backoff(exc)
            raise
        if isinstance(resp, dict) and resp.get("status") == "err" and is_weight_limit_error(resp):
            exc = RequestWeightLimited(str(resp))
            self._trip_weight_backoff(exc)
            raise exc
        if not self._in_weight_backoff():
            self._weight_logged = False
        return resp

    def schedule_deadman(self, ahead_ms: int = DEADMAN_AHEAD_MS) -> None:
        """Account-wide: cancels ALL open orders on the master when it fires."""
        if self.session is not None:
            return  # 显式人工值守模式：不申请、不伪造交易所自动撤单保护。
        if time.time() < self.next_deadman_at:
            return
        self.next_deadman_at = time.time() + HTTP_BACKOFF_S
        deadline = deadman_deadline_ms(ahead_ms)
        try:
            resp = self._post(self.signer.signed_schedule_cancel(deadline))
            if not isinstance(resp, dict) or resp.get("status") != "ok":
                raw_error = resp.get("response") if isinstance(resp, dict) else None
                detail = (" ".join(raw_error.split())[:500] if isinstance(raw_error, str)
                          else "missing or unexpected exchange response")
                self.halt_entries(f"dead-man rejected: {detail}; no confirmed protection")
                return
            now = time.time()
            self.last_deadman = now
            self.deadman_until = deadline / 1000.0
            log.info("dead-man scheduleCancel +%ss account-wide -> %s", ahead_ms // 1000, resp)
        except RequestWeightLimited:
            return
        except RateLimited:
            log.warning("dead-man rate limited; no immediate fallback retry")

    def maybe_deadman(self) -> None:
        if self.session is not None:
            return
        now = time.time()
        if self.entry_halt or self.shutting_down or self._in_weight_backoff():
            return
        if not self.entry_allowed() and not any(self._has_rest(c) for c in self.markets):
            return
        if not deadman_needs_refresh(self.deadman_until, now):
            return
        try:
            self.schedule_deadman(DEADMAN_AHEAD_MS)
        except RequestWeightLimited:
            return
        except Exception as exc:  # noqa: BLE001
            if is_weight_limit_error(exc):
                self._trip_weight_backoff(exc)
                return
            log.warning("dead-man renew failed: %s", exc)
            self.last_deadman = now
            # Do not tight-loop: wait at least MIN_REPLACE_S before another attempt.
            self.next_deadman_at = now + max(self.settings.min_replace_s, HTTP_BACKOFF_S)

    def clear_deadman(self) -> None:
        if self.session is not None or self.deadman_until <= 0:
            return
        try:
            resp = self._post(self.signer.signed_schedule_cancel(None))
            if not isinstance(resp, dict) or resp.get("status") != "ok":
                log.error("dead-man clear rejected; existing deadline remains unconfirmed")
                return
            self.deadman_until = 0.0
            log.info("dead-man scheduleCancel cleared (no time) -> %s", resp)
        except RateLimited:
            self.schedule_deadman(DEADMAN_FALLBACK_MS)
        except Exception as exc:  # noqa: BLE001
            log.warning("dead-man clear failed: %s", exc)

    def _cancel_pairs(self, coin: str, oids: list[int], cloids: list[str]) -> bool:
        market = self.markets[coin]
        ok = True
        if oids:
            try:
                resp = self._post(self.signer.signed_cancel_oids([(market.asset_id, oid) for oid in oids]),
                                  purpose="cancel", actions=len(oids))
                log.info("cancel oids %s %s -> %s", coin, oids, resp)
                if is_weight_limit_error(resp):
                    self._trip_weight_backoff(resp)
                    self.next_cancel_at = time.time() + LIMITED_EXIT_GAP_S
                if not cancel_confirmed(resp, len(oids)):
                    ok = False
            except RateLimited:
                log.warning("cancel oids 429 %s", coin)
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("cancel oids %s: %s", coin, exc)
                ok = False
        if cloids:
            try:
                resp = self._post(self.signer.signed_cancel_cloids([(market.asset_id, c) for c in cloids]),
                                  purpose="cancel", actions=len(cloids))
                log.info("cancel cloids %s -> %s", coin, resp)
                if is_weight_limit_error(resp):
                    self._trip_weight_backoff(resp)
                    self.next_cancel_at = time.time() + LIMITED_EXIT_GAP_S
                if not cancel_confirmed(resp, len(cloids)):
                    ok = False
            except Exception as exc:  # noqa: BLE001
                log.warning("cancel cloids %s: %s", coin, exc)
                ok = False
        return ok

    def cancel_coin_rests(
        self,
        *,
        refetch: bool = True,
        only_coin: str | None = None,
        sides: list[str] | None = None,
    ) -> dict[str, Any]:
        """Cancel cache then refetch frontendOpenOrders dex:io. 429 does not empty cache.

        `sides` (with `only_coin`) cancels just those sides plus any extras; the other
        side keeps resting. None = every side.
        """
        limited = False
        known = {oid for c in self.markets for oid in list_cached_oids(self.rests, self.extra_oids, c)}
        canceled: set[int] = set()
        for coin in self.markets:
            if only_coin is not None and coin != only_coin:
                continue
            if sides is None:
                oids = list_cached_oids(self.rests, self.extra_oids, coin)
                cloids = list_cached_cloids(self.rests, coin)
            else:
                picked = [self.rests[coin][s] for s in sides if side_occupied(self.rests, coin, s)]
                oids = [sl.oid for sl in picked if sl.oid is not None]
                oids += [o for o in self.extra_oids.get(coin, []) if o not in oids]
                cloids = [sl.cloid for sl in picked if sl.oid is None and sl.cloid]
            canceled.update(oids)
            if oids or cloids:
                try:
                    if self._cancel_pairs(coin, oids, cloids):
                        if sides is None:
                            clear_coin_rests(self.rests, coin)
                        else:
                            for s in sides:
                                self.rests[coin][s] = RestSlot()
                        self.extra_oids[coin] = []
                    else:
                        limited = True
                except RateLimited:
                    limited = True
                    log.warning("cancel cache 429 %s; keeping rest cache", coin)
        if not refetch:
            return {"rateLimited": limited}
        opens = self.client.open_orders(self.signer.account, ALLOWED_DEX, optional=True)
        if opens is None:
            log.warning("frontendOpenOrders null/429; not wiping rest cache")
            return {"rateLimited": True, "cachePreserved": True}
        # 只接管 EBOT 标记或本进程已知的订单，不撤手工/其他机器人的订单。
        foreign = [o for o in opens if match_order_coin(o, self.markets) is not None
                   and not is_bot_cloid(o.get("cloid")) and o.get("oid") not in known]
        if foreign:
            self.halt_entries("unmanaged open orders detected; exclusive account required")
        opens = [o for o in opens if is_bot_cloid(o.get("cloid")) or o.get("oid") in known]
        self.rests, self.extra_oids = apply_open_orders(self.rests, self.extra_oids, opens, self.markets)
        # 同一轮不再次撤销刚查到的订单；旧快照不能触发重复撤单。
        if sides is None:
            limited = limited or any(self._has_rest(c) or self.extra_oids[c]
                                     for c in self.markets if only_coin is None or c == only_coin)
        else:
            still = {sl.oid for c in self.markets for sl in self.rests[c].values() if sl.oid is not None}
            limited = limited or bool(canceled & still) or any(
                self.extra_oids[c] for c in self.markets if only_coin is None or c == only_coin)
        return {"rateLimited": limited, "fetched": True}

    def _place(self, coin: str, plan: QuotePlan) -> dict[str, Any] | None:
        if not plan.intents:
            return None
        reducing = all(i.reduce_only for i in plan.intents)
        if self.unknown_write or not self.position_fresh():
            return {"blocked": True}
        if not reducing:
            self.refresh_budget()
            if not self.entry_allowed() or (
                self.session is None and self.deadman_until - time.time() < DEADMAN_REMAIN_LT_S
            ):
                return {"blocked": True}
        # 撤单、仓位查询、额度查询及 dead-man 续期均可能阻塞。
        # 最后一个查询完成后重新取盘口及计算阶段，不能发送旧计划。
        # 正常行情直接读已有 WS 缓存；断流仍走原来的 REST 兜底及校验。
        top = self.book_for(coin, self._feed)
        if top is None or not self.position_fresh() or self.unknown_write:
            log.warning("pre-send blocked %s: book/position unavailable", coin)
            return {"blocked": True}
        fresh_plan = self.plan_for(coin, top)
        if (fresh_plan.mode != plan.mode or fresh_plan.szi != plan.szi
                or fresh_plan.take != plan.take):
            # ALO 跨入 IOC 时交回下一轮的 IOC 分支，保留独立撤单和重试节流。
            log.info("pre-send replan %s: %s/%s -> %s/%s; defer to next loop",
                     coin, plan.mode, plan.stage, fresh_plan.mode, fresh_plan.stage)
            return {"blocked": True}
        plan = fresh_plan
        if not plan.intents:
            return {"blocked": True}
        if not reducing and (not self.entry_allowed() or (
            self.session is None and self.deadman_until - time.time() < DEADMAN_REMAIN_LT_S
        )):
            return {"blocked": True}
        specs = []
        pending: list[tuple[str, Any]] = []
        for intent in plan.intents:
            if not plan.take and self._cooling_down(coin, intent.side):
                log.debug("%s %s in reject cool-down; not placing", coin, intent.side)
                continue
            if side_occupied(self.rests, coin, intent.side):
                # 这一侧的挂单在容差内被保留；绝不在同一侧再挂第二张。
                log.debug("keep %s %s rest; not placing a second order on that side", coin, intent.side)
                continue
            cloid = self.signer.next_cloid(intent.coin, intent.side)  # type: ignore[arg-type]
            specs.append(
                (
                    self.markets[coin],
                    intent.is_buy,
                    intent.px,
                    intent.sz,
                    cloid,
                    intent.reduce_only,
                    intent.tif,
                )
            )
            pending.append((intent.side, intent))
        if not specs:
            return None
        self._fee_banner(coin)
        payload = self.signer.signed_orders(specs)
        # 签名期间收到成交推送，原仓位快照立即失效；减仓单也不能使用它。
        if not self.position_fresh() or self.unknown_write:
            return {"blocked": True}
        log.info("ORDER_SEND coin=%s stage=%s hold_age_ms=%d book_time=%s "
                 "bid=%s ask=%s intents=%s", coin, plan.stage, plan.age_ms,
                 top.time, top.bid, top.ask,
                 [(i.side, i.px, i.sz, i.tif, i.reduce_only) for _, i in pending])
        send_started = time.monotonic()
        try:
            resp = self._post(payload, purpose="exit" if reducing else "entry", actions=len(specs))
        except RateLimited:
            log.warning("place 429 %s", coin)
            raise
        except Exception:
            # 未知结果可能是已成交或已挂出；保留原 CLOID 以便撤销，不生成新单重发。
            for idx, (side, intent) in enumerate(pending):
                self.rests[coin][side] = RestSlot(cloid=specs[idx][4], px=intent.px, sz=intent.sz)
            raise
        finally:
            log.info("ORDER_RETURN coin=%s elapsed_ms=%.1f", coin,
                     (time.monotonic() - send_started) * 1000)
        if not action_ok(resp):
            if plan.take:
                log.warning("IOC flatten failed %s %s", coin, resp)
                return {"iocFail": True, "resp": resp}
            if is_alo_reject(resp):
                log.warning("ALO px rejected, will requote %s", coin)
                self.last_plan_key[coin] = ""
                for side, _intent in pending:
                    self._note_reject(coin, side, alo=True)
                return {"aloReject": True, "resp": resp}
            for side, _intent in pending:
                wait = self._note_reject(coin, side, alo=False)
            log.warning("order not ok %s %s (retry in %.0fs)", coin, resp, wait)
            return {"rejected": True, "resp": resp}
        statuses = resp_statuses(resp)
        if len(statuses) > len(pending):
            self.unknown_write = True
            self.halt_entries("unexpected extra order statuses; reconcile manually")
        now = time.time()
        accepted = 0
        rejected = 0
        alo_rejected = False
        for idx, (_side, intent) in enumerate(pending):
            st = statuses[idx] if idx < len(statuses) else None
            err = status_error(st)
            if err:
                rejected += 1
                if is_weight_limit_error(err):
                    self._trip_weight_backoff(err)
                if is_alo_reject(err):
                    alo_rejected = True
                    log.warning("ALO px rejected, will requote %s %s", coin, err)
                    self.last_plan_key[coin] = ""
                    self._note_reject(coin, _side, alo=True)
                    continue
                if plan.take:
                    log.warning("IOC flatten status %s %s", coin, err)
                    continue
                wait = self._note_reject(coin, _side, alo=False)
                log.warning("order status %s %s %s (retry that side in %.0fs)", coin, _side, err, wait)
                continue
            self._reject_streak.pop((coin, _side), None)
            if isinstance(st, dict) and isinstance(st.get("filled"), dict):
                # IOC 全部/部分成交均不会成为挂单；仓位由新查询确认。
                self.positions_valid = False
                continue
            if not isinstance(st, dict) or not isinstance(st.get("resting"), dict):
                self.unknown_write = True
                self.halt_entries("missing/unknown order status; reconcile manually")
                self.rests[coin][intent.side] = RestSlot(cloid=specs[idx][4])
                continue
            try:
                oid = status_oid(st)
            except (ValueError, TypeError):
                oid = None
            if oid is None:
                self.unknown_write = True
                self.halt_entries("resting order without valid oid; reconcile manually")
            slot = RestSlot(
                oid=oid,
                cloid=specs[idx][4],
                px=intent.px,
                sz=intent.sz,
                tif=intent.tif,
                reduce_only=intent.reduce_only,
                placed_at=now,
                stage=intent.stage,
            )
            self.rests[coin][intent.side] = slot
            accepted += 1
        log.info(
            "order_result kind=%s coin=%s accepted_rests=%d intents=%s",
            "take" if plan.take else "maker",
            coin,
            accepted,
            [(i.side, i.px, i.sz, i.tif, "ro" if i.reduce_only else "") for i in plan.intents],
        )
        self._diag_quotes(coin, accepted, take=plan.take)
        return {"resp": resp, "rejected": rejected > 0, "aloReject": alo_rejected,
                "blocked": self.unknown_write, "planKey": plan.key()}

    def requote(self, coin: str, top: BookTop) -> None:
        plan_started = time.time()
        if time.time() < max(self.next_safety_poll.get(coin, 0), self.http_backoff_until):
            return
        self.refresh_positions()
        self.refresh_budget()
        if not self.position_fresh() or self.unknown_write:
            self.next_safety_poll[coin] = time.time() + 5.0
            self.cancel_coin_rests(only_coin=coin)
            return
        plan = self.plan_for(coin, top)
        block = self.entry_block_reason(coin, top) if plan.mode == "flat" else ""
        if plan.mode == "flat":
            self._note_block(coin, block)
        if plan.mode == "flat" and (block or not self.entry_allowed()):
            self.next_safety_poll[coin] = time.time() + 5.0
            self.cancel_coin_rests(only_coin=coin)
            return
        if plan.mode != "flat" and self.entry_halt and not plan.take:
            # 限额期间不消耗稀缺动作重报 ALO，先撤增仓单，15s 后才走原 IOC 阶梯。
            self.cancel_coin_rests(only_coin=coin)
            self.next_safety_poll[coin] = time.time() + 5.0
            return
        if plan.mode != "flat" and time.time() < self.next_exit_at:
            return
        stale = (
            plan.mode == "flat"
            and self.settings.stale_quote_s > 0
            and self.quote_placed_at.get(coin, 0) > 0
            and time.time() - self.quote_placed_at[coin] > self.settings.stale_quote_s
        )
        have_buy = side_occupied(self.rests, coin, "B")
        have_sell = side_occupied(self.rests, coin, "A")
        if plan.take:
            now = time.time()
            if now - self.last_take_at.get(coin, 0.0) < IOC_GAP_S:
                self.maybe_deadman()
                return
            log.info("flatten take %s age=%.1fs szi=%s", coin, plan.age_ms / 1000, plan.szi)
            try:
                result = self.cancel_coin_rests(refetch=True, only_coin=coin)
            except Exception as exc:  # noqa: BLE001
                log.warning("cancel before IOC %s: %s", coin, exc)
                return
            if result.get("rateLimited"):
                self.next_safety_poll[coin] = time.time() + 5.0
                return
            if side_occupied(self.rests, coin, "B") or side_occupied(self.rests, coin, "A"):
                log.warning("still have rests after cancel; skip IOC place %s", coin)
                self.maybe_deadman()
                return
            self.last_take_at[coin] = now
            self.refresh_positions(force=True)
            if not self.position_fresh():
                return
            plan = self.plan_for(coin, top)
            if not plan.take:
                return
            if time.time() - plan_started > 3.0:
                return
            try:
                self._place(coin, plan)
            except RequestWeightLimited:
                log.warning("IOC skipped %s; request-weight backoff", coin)
            except RateLimited:
                log.warning("IOC 429 %s; continue", coin)
            except Exception as exc:  # noqa: BLE001
                if is_alo_reject(exc):
                    log.warning("ALO/post-only reject on take path %s: %s", coin, exc)
                else:
                    log.warning("IOC flatten retry %s: %s", coin, exc)
            self.last_plan_key[coin] = ""
            self.quote_placed_at[coin] = 0.0
            self.refresh_positions(force=True)
            self.maybe_deadman()
            return
        if not plan.intents:
            log.warning("no maker plan %s; skip", coin)
            self.last_plan_key[coin] = ""
            self.maybe_deadman()
            return
        # 两侧分开判断：在容差内的挂单不动；类型不对的立即撤；只是价格偏了的按节流替换。
        now = time.time()
        tick = top.inferred_tick(self.markets[coin].sz_decimals)
        desired = {i.side: i for i in plan.intents}
        urgent: list[str] = []
        reprice: list[str] = []
        for side in ("B", "A"):
            slot = self.rests[coin][side]
            if not slot.occupied():
                continue
            want = desired.get(side)
            if want is None:
                urgent.append(side)  # 有仓时的开仓方向：一刻都不留
                continue
            verdict = slot_verdict(
                slot,
                want,
                tick=tick,
                tolerance_ticks=self.settings.requote_tolerance_ticks,
                stale_quote_s=self.settings.stale_quote_s,
                now=now,
            )
            if verdict == "now":
                urgent.append(side)
            elif verdict == "slow" and not self._cooling_down(coin, side):
                reprice.append(side)  # 冷却中的一侧不改价：旧单留着，好过撤了挂不上
        missing = [side for side in desired
                   if not side_occupied(self.rests, coin, side) and not self._cooling_down(coin, side)]
        elapsed = now - self.last_replace.get(coin, 0.0)
        throttled = not replace_allowed(
            take=False,
            has_rest=have_buy or have_sell,
            elapsed_s=elapsed,
            min_replace_s=self.settings.min_replace_s,
        )
        # 空仓时、另一侧还挂着、又在节流期内：不补挂也不改价（避免拒单后紧密重报）。
        defer = throttled and plan.mode == "flat" and not urgent
        cancel_sides = urgent + ([] if throttled else reprice)
        will_place = not defer and (bool(missing) or any(s in desired for s in cancel_sides))
        if not cancel_sides and not will_place:
            if reprice and throttled and not self._replace_skip_logged.get(coin):
                log.info(
                    "min-replace: keep %s rests (%.1fs < MIN_REPLACE_S=%ss); take still allowed",
                    coin,
                    elapsed,
                    self.settings.min_replace_s,
                )
                self._replace_skip_logged[coin] = True
            extras = list(self.extra_oids.get(coin, []))
            if extras:
                try:
                    if self._cancel_pairs(coin, extras, []):
                        self.extra_oids[coin] = []
                except RequestWeightLimited:
                    return
                except Exception as extra_exc:  # noqa: BLE001
                    log.warning("sweep extras %s: %s", coin, extra_exc)
            self.maybe_deadman()
            return
        if stale:
            log.info("stale quote refresh %s", coin)
        log.info("requote %s stage=%s cancel=%s place=%s", coin, plan.stage, cancel_sides,
                 sorted(set(missing) | {s for s in cancel_sides if s in desired}))
        try:
            # 先撤需要撤的那几侧，再重新拉一次挂单列表核对，才允许下单。
            result = self.cancel_coin_rests(refetch=True, only_coin=coin, sides=cancel_sides)
        except Exception as exc:  # noqa: BLE001
            if is_alo_reject(exc):
                log.warning("ALO reject during cancel/replace %s: %s", coin, exc)
                self.last_plan_key[coin] = ""
                return
            log.warning("cancel before replace %s: %s", coin, exc)
            return
        if result.get("rateLimited"):
            self.next_safety_poll[coin] = time.time() + 5.0
            return
        if not will_place:
            if reprice and not throttled:
                self._mark_replaced(coin)
            return
        self.refresh_positions(force=True)
        if not self.position_fresh():
            return
        fresh_plan = self.plan_for(coin, top)
        if fresh_plan.mode != plan.mode or fresh_plan.szi != plan.szi:
            self.last_plan_key[coin] = ""
            return
        if plan.mode == "flat":
            self.maybe_deadman()
        if time.time() - plan_started > 3.0:
            return
        try:
            placed = self._place(coin, plan)
        except RequestWeightLimited:
            log.warning("place skipped %s; request-weight backoff", coin)
            self._mark_replaced(coin)
            return
        except RateLimited:
            log.warning("place 429 %s; continue loop", coin)
            self._mark_replaced(coin)
            return
        except Exception as exc:  # noqa: BLE001
            if is_alo_reject(exc):
                log.warning("ALO px rejected, will requote %s: %s", coin, exc)
                self.last_plan_key[coin] = ""
                self._mark_replaced(coin)
                return
            log.warning("place failed %s: %s", coin, exc)
            self._mark_replaced(coin)
            return
        if not placed or placed.get("aloReject") or placed.get("rejected") or placed.get("blocked"):
            self.last_plan_key[coin] = ""
            self._mark_replaced(coin)
            return
        # _place 可能用新盘口重算过；缓存必须对应实际发出的计划。
        self.last_plan_key[coin] = placed["planKey"]
        self.quote_placed_at[coin] = time.time() if plan.mode == "flat" else 0.0
        self._mark_replaced(coin)
        self.maybe_deadman()

    def step(self, feed: BookFeed | None) -> None:
        self._feed = feed
        self.check_session_loss()
        try:
            self.refresh_positions()
        except Exception as exc:  # noqa: BLE001
            log.warning("position poll: %s", exc)
        try:
            self.poll_user_fills()
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag poll: %s", exc)
        self.flush_markouts()
        for coin in self.markets:
            try:
                if time.time() < self.next_safety_poll.get(coin, 0):
                    continue
                top = self.book_for(coin, feed)
                if top is None or top.bid is None or top.ask is None:
                    self.next_safety_poll[coin] = time.time() + 5.0
                    self.cancel_coin_rests(only_coin=coin)
                    continue
                self.requote(coin, top)
            except Exception as exc:  # noqa: BLE001
                if is_alo_reject(exc):
                    log.warning("ALO px rejected, will requote %s: %s", coin, exc)
                    self.last_plan_key[coin] = ""
                    continue
                log.warning("live step %s: %s", coin, exc)
                self.next_safety_poll[coin] = time.time() + 5.0
                self.halt_entries("execution loop could not establish safe state; manual review required")
                try:
                    self.cancel_coin_rests(only_coin=coin)
                except Exception:
                    log.error("SAFETY cleanup unconfirmed for %s; check account manually", coin)
        self.maybe_deadman()

    def shutdown_cancels(self) -> None:
        self.shutting_down = True
        limited = False
        try:
            result = self.cancel_coin_rests(refetch=True)
            limited = bool(result.get("rateLimited"))
        except Exception as exc:  # noqa: BLE001
            log.warning("shutdown cancel: %s", exc)
            limited = True
        if limited:
            log.error("SHUTDOWN orders unconfirmed; confirmed dead-man deadline=%s; "
                      "check account manually (0 means no confirmed protection)", self.deadman_until)
        else:
            self.clear_deadman()
        self.refresh_positions(force=True)
        if not self.position_fresh() or any(self.pos.values()) or limited or self.unknown_write:
            raise LiveGuardError("SHUTDOWN NOT CLEAN: position/order state requires manual review; cancel is not flatten")
        log.info("SHUTDOWN confirmed flat with no managed orders; final verification complete")


def _log_agent(client: InfoClient, signer: LiveSigner) -> None:
    if not signer.is_agent:
        return
    extras = client.extra_agents(signer.account, optional=True)
    if extras is None:
        log.info("extraAgents unavailable; continuing with agent %s", signer.signer_address)
        return
    found = any(
        str(row.get("address") or row.get("agentAddress") or row.get("agent") or "").lower()
        == signer.signer_address.lower()
        for row in extras
        if isinstance(row, dict)
    )
    if found:
        log.info("agent %s is approved on master %s", signer.signer_address, signer.account)
    else:
        log.warning(
            "agent %s not listed in extraAgents for %s; orders may fail until approved on-site",
            signer.signer_address,
            signer.account,
        )


CLOCK_SKEW_WARN_MS = 2_000
CLOCK_SKEW_MAX_MS = 15_000  # 盘口新鲜度按服务器时间判断；时钟偏太多会一直判成旧盘口


def check_signer_role(client: InfoClient, signer: LiveSigner) -> None:
    """私钥和 HYPERLIQUID_ACCOUNT 对不上时拒绝启动：否则会查 A 账户的仓位、在 B 账户下单。只读。"""
    data = client.post_info({"type": "userRole", "user": signer.signer_address}, optional=True)
    role = data.get("role") if isinstance(data, dict) else None
    if role is None:
        log.warning("userRole unavailable; cannot verify signer %s -> account %s",
                    signer.signer_address, signer.account)
        return
    if role == "agent":
        master = str((data.get("data") or {}).get("user") or "")
        if master and master.lower() != signer.account.lower():
            raise LiveGuardError(
                f"preflight: signing key {signer.signer_address} is an agent of {master}, but "
                f"HYPERLIQUID_ACCOUNT={signer.account}; set HYPERLIQUID_ACCOUNT={master}"
            )
        return
    if signer.is_agent:
        raise LiveGuardError(
            f"preflight: key {signer.signer_address} is not an approved agent of {signer.account} "
            f"(userRole={role!r}); orders would hit the key's own account"
        )


def check_clock(client: InfoClient, coin: str) -> None:
    """只读：本机时钟和交易所时间对比。"""
    try:
        server_ms = int(client.l2_book(coin).get("time"))
    except Exception as exc:  # noqa: BLE001
        log.warning("clock check skipped: %s", exc)
        return
    skew = int(time.time() * 1000) - server_ms
    if abs(skew) > CLOCK_SKEW_MAX_MS:
        raise LiveGuardError(f"preflight: local clock is {skew / 1000:+.1f}s off Hyperliquid; fix NTP first")
    if abs(skew) > CLOCK_SKEW_WARN_MS:
        log.warning("local clock is %+.1fs off Hyperliquid; check NTP", skew / 1000)


def _run_supervised(quoter: LiveQuoter, feed: BookFeed, stop: dict[str, Any]) -> int:
    """到时/首个停止信号后只减仓；等待有界，不循环重启或自动购买额度。"""
    session = quoter.session
    assert session is not None
    cancellation_confirmed = False
    next_cancel = next_check = next_report = 0.0
    reported_reason = ""
    while True:
        now = time.monotonic()
        if stop["count"] > 1:
            raise LiveGuardError("STOP second signal: cleanup interrupted; check account manually")
        if stop["flag"]:
            session.request_stop(f"signal:{signal.Signals(stop['signal']).name}")
        if quoter.entry_halt:
            session.request_stop(f"safety:{quoter.entry_halt}")
        session.tick()
        if session.stop_reason:
            if not reported_reason:
                reported_reason = session.stop_reason
                log.warning("SESSION DRAIN reason=%s; entries OFF; reduce-only until flat or deadline",
                            reported_reason)
            if not session.can_exit():
                raise LiveGuardError("SESSION cleanup timed out; positions/orders require manual check")
            if not cancellation_confirmed:
                if now >= next_cancel:
                    next_cancel = now + 5.0
                    try:
                        result = quoter.cancel_coin_rests(refetch=True)
                        cancellation_confirmed = not result.get("rateLimited", True)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("SESSION initial cancellation unconfirmed: %s", exc)
                if not cancellation_confirmed:
                    time.sleep(LOOP_S)
                    continue
            if now >= next_check:
                next_check = now + 5.0
                if quoter.session_account_clear():
                    if quoter.entry_halt:
                        raise LiveGuardError(f"SAFETY STOP: {quoter.entry_halt}; account reconciled flat")
                    log.info("SESSION DONE reason=%s; exchange reports flat and no orders", reported_reason)
                    return 0
        quoter.step(feed)  # 原有报价/6s-15s退出阶梯；session.can_enter 阻止重新开仓。
        if now >= next_report:
            next_report = now + 60.0
            log.info("SESSION heartbeat mode=supervised-no-deadman remaining_s=%.0f draining=%s budget=%.0f",
                     max(0, session.end_at - now), bool(session.stop_reason), quoter.budget.available)
        time.sleep(LOOP_S)


def run_live(settings: Settings, *, seconds: float | None = None,
             supervised_no_deadman: bool = False) -> int:
    session = SupervisedSession(seconds) if supervised_no_deadman else None
    if seconds is not None and (not math.isfinite(seconds) or seconds <= 0):
        raise LiveGuardError("--seconds must be finite and positive")
    require_live(settings)
    signer = LiveSigner(settings.private_key or "", settings.account)
    client = InfoClient(settings.api_url)
    feed: BookFeed | None = None
    quoter: LiveQuoter | None = None
    previous_handlers: dict[int, Any] = {}
    try:
        _meta, markets, _ctxs = load_io_markets(client, settings.coins)
        dex = perp_dex_info(client.perp_dexs(), ALLOWED_DEX)
        log.info(
            "LIVE isolated MM  dex=%s/%s  master=%s  signer=%s  agent=%s  "
            "notional=$%s  coins=%s  builder=%s fee=0",
            dex.get("name"),
            dex.get("fullName"),
            signer.account,
            signer.signer_address,
            signer.is_agent,
            live_notional(settings),
            ",".join(settings.coins),
            "0xcD254d2A328f7f67C7c6FEf930A4757516F7b601",
        )
        log.info("%s", describe_fee_model(settings))
        log.info("%s", ALO_TIF_NOTE)
        if session is not None:
            log.warning(
                "SUPERVISED NO DEAD-MAN: exchange auto-cancel is DISABLED; server/network failure "
                "can leave live orders and positions. Manual supervision required. seconds=%s", seconds,
            )
        else:
            log.info(
                "dead-man is account-wide: scheduleCancel +20s; refresh only when "
                "remaining < %ss (not every few seconds). Failed weight renew: log, no tight-loop. "
                "Only acknowledged deadlines count. No immediate 429 fallback. "
                "Clear (no time) attempted only after confirmed cancellations; cancel is not flatten.",
                int(DEADMAN_REMAIN_LT_S),
            )
        log.info(
            "write throttle: MIN_REPLACE_S=%ss per coin while a rest is live "
            "(book ticks do not cancel+replace). Flatten ≥15s IOC take still fires. "
            "request-weight error latches entries OFF; maintenance backoff %ss. "
            "Cancels and reduce-only exits are paced separately; no quota purchase. "
            "No automatic session filter; account must be exclusive to this bot.",
            settings.min_replace_s,
            weight_backoff_s(settings.min_replace_s),
        )
        log.info(
            "fill-diag observe-only: FILL_DIAG jsonl per fill (+3s markout) and "
            "quote/fill counters split io:ANTH vs io:SNDK. SNDK session=rth|ah "
            "(Mon–Fri 09:30–16:00 America/New_York; no holiday calendar). "
            "AH markout stays in its own bucket."
        )
        _log_agent(client, signer)
        check_signer_role(client, signer)
        check_clock(client, settings.coins[0])
        quoter = LiveQuoter(settings, markets, client, signer)
        quoter.session = session
        quoter.preflight()
        quoter.managed_started = True

        stop = {"flag": False, "signal": None, "count": 0}

        def _handle(signum: int, _frame: object) -> None:
            stop["signal"] = signum
            stop["count"] += 1
            stop["flag"] = True
            if session is not None:
                session.request_stop(f"signal:{signal.Signals(signum).name}")

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, _handle)

        quoter.bootstrap_isolated()

        quoter.poll_user_fills(force=True)
        feed = BookFeed(
            settings.ws_url,
            settings.coins,
            quoter.on_book,
            user=signer.account,
            on_user_fills=quoter.on_user_fills,
            on_asset_ctx=quoter.on_asset_ctx,
        )
        quoter._feed = feed
        feed.start()
        if not feed.wait_ready():
            log.error("websocket did not become ready")
            feed.stop()
            return 1
        if session is not None:
            return _run_supervised(quoter, feed, stop)
        deadline = time.time() + seconds if seconds else None
        while not stop["flag"]:
            if deadline and time.time() >= deadline:
                log.info("STOP reason=duration seconds=%s", seconds)
                break
            quoter.step(feed)
            if not stop["flag"]:
                quoter.raise_if_halted_and_flat()
            time.sleep(LOOP_S)
        if stop["signal"] is not None:
            log.info("STOP reason=signal signal=%s", signal.Signals(stop["signal"]).name)
        if quoter.entry_halt:
            raise LiveGuardError(f"SAFETY STOP: {quoter.entry_halt}")
        return 0
    finally:
        if feed is not None:
            feed.stop()
        shutdown_error = None
        if quoter is not None and quoter.managed_started:
            try:
                quoter.flush_markouts()
            except Exception as exc:  # noqa: BLE001
                log.warning("fill-diag shutdown flush: %s", exc)
            try:
                quoter.shutdown_cancels()
            except Exception as exc:  # noqa: BLE001
                log.warning("shutdown: %s", exc)
                shutdown_error = exc
        client.close()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        if shutdown_error is not None:
            raise LiveGuardError(str(shutdown_error)) from shutdown_error
