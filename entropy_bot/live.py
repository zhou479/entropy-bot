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
from entropy_bot.config import Settings, live_notional, require_live
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
    STALE_QUOTE_MS,
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
from entropy_bot.rest import InfoClient
from entropy_bot.status import load_io_markets
from entropy_bot.ws import BookFeed

log = logging.getLogger("entropy_bot.live")

WS_STALE_S = 15.0
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


@dataclass
class RestSlot:
    oid: int | None = None
    cloid: str | None = None
    px: float | None = None
    sz: float | None = None
    tif: str = "Alo"
    reduce_only: bool = False
    placed_at: float = 0.0

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
            if latest is not None:
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
            if latest is not None:
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
        return quote_plan(
            self.markets[coin],
            top,
            szi=szi,
            age_ms=age_ms,
            notional_usd=self.notional,
        )

    def _post(self, payload: dict[str, Any], *, purpose: str = "maintenance", actions: int = 1) -> Any:
        now = time.time()
        if now < self.http_backoff_until:
            raise RateLimited("local HTTP backoff")
        if purpose == "entry" and not self.entry_allowed():
            raise RequestWeightLimited("entry safety gate closed")
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
        if time.time() < self.next_deadman_at:
            return
        self.next_deadman_at = time.time() + HTTP_BACKOFF_S
        deadline = deadman_deadline_ms(ahead_ms)
        try:
            resp = self._post(self.signer.signed_schedule_cancel(deadline))
            if not isinstance(resp, dict) or resp.get("status") != "ok":
                self.halt_entries("dead-man rejected; no confirmed protection")
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

    def cancel_coin_rests(self, *, refetch: bool = True, only_coin: str | None = None) -> dict[str, Any]:
        """Cancel cache then refetch frontendOpenOrders dex:io. 429 does not empty cache."""
        limited = False
        known = {oid for c in self.markets for oid in list_cached_oids(self.rests, self.extra_oids, c)}
        for coin in self.markets:
            if only_coin is not None and coin != only_coin:
                continue
            oids = list_cached_oids(self.rests, self.extra_oids, coin)
            cloids = list_cached_cloids(self.rests, coin)
            if oids or cloids:
                try:
                    if self._cancel_pairs(coin, oids, cloids):
                        clear_coin_rests(self.rests, coin)
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
        limited = limited or any(self._has_rest(c) or self.extra_oids[c]
                                 for c in self.markets if only_coin is None or c == only_coin)
        return {"rateLimited": limited, "fetched": True}

    def _place(self, coin: str, plan: QuotePlan) -> dict[str, Any] | None:
        if not plan.intents:
            return None
        reducing = all(i.reduce_only for i in plan.intents)
        if self.unknown_write or not self.position_fresh():
            return {"blocked": True}
        if not reducing:
            self.refresh_budget()
            if not self.entry_allowed() or self.deadman_until - time.time() < DEADMAN_REMAIN_LT_S:
                return {"blocked": True}
        specs = []
        pending: list[tuple[str, Any]] = []
        for intent in plan.intents:
            if side_occupied(self.rests, coin, intent.side):
                log.warning("refuse duplicate %s %s; oid/cloid already cached", coin, intent.side)
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
        if not action_ok(resp):
            if plan.take:
                log.warning("IOC flatten failed %s %s", coin, resp)
                return {"iocFail": True, "resp": resp}
            if is_alo_reject(resp):
                log.warning("ALO px rejected, will requote %s", coin)
                self.last_plan_key[coin] = ""
                return {"aloReject": True, "resp": resp}
            log.warning("order not ok %s %s", coin, resp)
            return {"resp": resp}
        statuses = resp_statuses(resp)
        if len(statuses) > len(pending):
            self.unknown_write = True
            self.halt_entries("unexpected extra order statuses; reconcile manually")
        now = time.time()
        accepted = 0
        for idx, (_side, intent) in enumerate(pending):
            st = statuses[idx] if idx < len(statuses) else None
            err = status_error(st)
            if err:
                if is_weight_limit_error(err):
                    self._trip_weight_backoff(err)
                if is_alo_reject(err):
                    log.warning("ALO px rejected, will requote %s %s", coin, err)
                    self.last_plan_key[coin] = ""
                    continue
                if plan.take:
                    log.warning("IOC flatten status %s %s", coin, err)
                    continue
                log.warning("order status %s %s", coin, err)
                continue
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
        return {"resp": resp}

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
        if plan.mode == "flat" and not self.entry_allowed():
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
        key = plan.key()
        stale = (
            plan.mode == "flat"
            and self.quote_placed_at.get(coin, 0) > 0
            and (time.time() - self.quote_placed_at[coin]) * 1000 > STALE_QUOTE_MS
        )
        have_buy = side_occupied(self.rests, coin, "B")
        have_sell = side_occupied(self.rests, coin, "A")
        want_buy = any(i.is_buy for i in plan.intents)
        want_sell = any(not i.is_buy for i in plan.intents)
        same_coverage = have_buy == want_buy and have_sell == want_sell
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
        if same_coverage and key == self.last_plan_key.get(coin) and not stale:
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
        elapsed = time.time() - self.last_replace.get(coin, 0.0)
        if not replace_allowed(
            take=False,
            has_rest=have_buy or have_sell,
            elapsed_s=elapsed,
            min_replace_s=self.settings.min_replace_s,
        ):
            if not self._replace_skip_logged.get(coin):
                log.info(
                    "min-replace: keep %s rests (%.1fs < MIN_REPLACE_S=%ss); "
                    "mid/far/flat reprice skipped, take still allowed",
                    coin,
                    elapsed,
                    self.settings.min_replace_s,
                )
                self._replace_skip_logged[coin] = True
            self.maybe_deadman()
            return
        if stale:
            log.info("stale same-price rest >45s, cancel+replace %s", coin)
        else:
            log.info("plan change, cancel then place %s stage=%s tif=%s", coin, plan.stage, plan.tif)
        try:
            result = self.cancel_coin_rests(refetch=True, only_coin=coin)
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
        if placed and (placed.get("aloReject") or placed.get("blocked")):
            self._mark_replaced(coin)
            return
        self.last_plan_key[coin] = key
        self.quote_placed_at[coin] = time.time() if plan.mode == "flat" else 0.0
        self._mark_replaced(coin)
        self.maybe_deadman()

    def step(self, feed: BookFeed | None) -> None:
        self._feed = feed
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
            log.error("SHUTDOWN orders unconfirmed; existing dead-man retained, check account manually")
        else:
            self.clear_deadman()
        self.refresh_positions(force=True)
        if not self.position_fresh() or any(self.pos.values()) or limited or self.unknown_write:
            raise LiveGuardError("SHUTDOWN NOT CLEAN: position/order state requires manual review; cancel is not flatten")
        log.info("SHUTDOWN confirmed flat with no managed orders; no automatic flatten was attempted")


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


def run_live(settings: Settings, *, seconds: float | None = None) -> int:
    require_live(settings)
    signer = LiveSigner(settings.private_key or "", settings.account)
    client = InfoClient(settings.api_url)
    feed: BookFeed | None = None
    quoter: LiveQuoter | None = None
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
        quoter = LiveQuoter(settings, markets, client, signer)
        quoter.preflight()
        quoter.managed_started = True
        quoter.bootstrap_isolated()

        stop = {"flag": False}

        def _handle(_signum: int, _frame: object) -> None:
            stop["flag"] = True

        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)

        quoter.poll_user_fills(force=True)
        feed = BookFeed(
            settings.ws_url,
            settings.coins,
            quoter.on_book,
            user=signer.account,
            on_user_fills=quoter.on_user_fills,
        )
        quoter._feed = feed
        feed.start()
        if not feed.wait_ready():
            log.error("websocket did not become ready")
            feed.stop()
            return 1
        deadline = time.time() + seconds if seconds else None
        while not stop["flag"]:
            if deadline and time.time() >= deadline:
                break
            quoter.step(feed)
            time.sleep(LOOP_S)
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
        if shutdown_error is not None:
            raise LiveGuardError(str(shutdown_error)) from shutdown_error
