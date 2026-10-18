"""Official-API live MM. Port of entropy-desk.user.js 1.6.1.

Talks only to https://api.hyperliquid.xyz and wss://api.hyperliquid.xyz/ws.
No Tampermonkey / window.ethereum. Per coin: at most 1 buy + 1 sell.

Write budget: Hyperliquid allows one address-level request per 1 USDC of
lifetime volume, and every order or cancel in a batch counts. So each side is
managed on its own: a rest within `REQUOTE_TOLERANCE_TICKS` of its target stays
put, a cancel goes out by oid only, and a coin's requote never touches another
coin's rests.

Safety writes (cancels, reduce-only flatten, dead-man) are never held back by the
local request-weight backoff: Hyperliquid gives cancels a larger cap, and
skipping them only leaves stale quotes resting.

Dead-man `scheduleCancel` is **account-wide**: when it fires it cancels every
open order on the master account, not just this bot's coins.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from entropy_bot.coins import ALLOWED_DEX, Market, perp_dex_info
from entropy_bot.config import Settings, live_notional, max_position_usd, require_live
from entropy_bot.diagnostics import BookSnap, FillDiagnostics, snap_from_top
from entropy_bot.errors import (
    ExchangeUncertain,
    RateLimited,
    RequestWeightLimited,
    is_weight_limit_error,
)
from entropy_bot.fees import (
    ALO_TIF_NOTE,
    alo_fill_rebate_usd,
    describe_fee_model,
    estimate_from_settings,
    fee_for_notional,
)
from entropy_bot.orders import (
    ENTROPY_BUILDER,
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
    QuoteIntent,
    QuotePlan,
    book_top,
    quote_plan,
    sync_pos_since,
)
from entropy_bot.rest import InfoClient
from entropy_bot.session import is_rth
from entropy_bot.status import load_io_markets
from entropy_bot.ws import BookFeed

log = logging.getLogger("entropy_bot.live")

WS_STALE_S = 15.0
POS_POLL_S = 1.0
POS_DIRTY_MIN_S = 0.25  # re-poll inventory this soon after a fill
FILL_POLL_S = 60.0  # REST backup only; WS userFills is primary
RECONCILE_S = 20.0  # frontendOpenOrders cross-check; orderUpdates WS keeps the cache live
RECONCILE_GRACE_S = 2.0  # never drop a rest this young just because info lags
RECONCILE_RETRY_S = 2.0  # pending reconcile after null/429: retry no faster than this
BUDGET_POLL_S = 120.0
LOOP_S = 0.4  # longest wait between steps without a book update
MIN_STEP_S = 0.1
DEADMAN_MIN_MS = 5_000
DEADMAN_FALLBACK_MS = 6_000
DEADMAN_REMAIN_LT_S = 8.0
DEADMAN_REFRESH_S = DEADMAN_REMAIN_LT_S  # alias kept for callers of the old name
DEADMAN_RETRY_S = 10.0
IOC_GAP_S = 0.4
IOC_GAP_MAX_S = 5.0
WEIGHT_LIMITED_GAP_S = 10.0  # Hyperliquid: one request per 10s while address-limited
WEIGHT_BACKOFF_FLOOR_S = 30.0
ALO_REJECT_RETRY_S = 2.0
BUDGET_SLOW_MULT = 4  # headroom < reserve * this → slow down reprices
BUDGET_SLOW_FACTOR = 3.0
ORACLE_STALE_S = 30.0
EPS = 1e-9

Verdict = str  # "keep" | "now" | "slow"


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


def _float_or_none(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


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
            slot.px = _float_or_none(order.get("limitPx") or order.get("px"))
        else:
            next_extras[coin].append(oid)
    for coin, extra_oids in next_extras.items():
        if extra_oids:
            log.info("extra rests %s oids=%s", coin, extra_oids)
    return next_rests, next_extras


def reconcile_open_orders(
    rests: dict[str, dict[str, RestSlot]],
    opens: list[dict[str, Any]],
    markets: dict[str, Market],
    *,
    now: float,
    grace_s: float = RECONCILE_GRACE_S,
) -> tuple[dict[str, dict[str, RestSlot]], dict[str, list[int]]]:
    """Merge frontendOpenOrders into the cache without losing slot metadata.

    - Cached rests still on the book keep their stage / placed_at.
    - Cached rests gone from the book (filled / canceled) are cleared, unless
      younger than `grace_s` (info can trail a fresh placement).
    - An unknown order fills an empty slot (stage "adopted", so the next requote
      replaces it on purpose); anything else on a traded coin is an extra to cancel.
    """
    by_coin: dict[str, list[dict[str, Any]]] = {coin: [] for coin in markets}
    for order in opens:
        coin = match_order_coin(order, markets)
        if coin is not None and coin in by_coin:
            by_coin[coin].append(order)
    out = {coin: dict(rests.get(coin) or {"B": RestSlot(), "A": RestSlot()}) for coin in markets}
    extras: dict[str, list[int]] = {coin: [] for coin in markets}
    for coin, orders in by_coin.items():
        claimed: set[int] = set()
        parsed: list[tuple[int, str, dict[str, Any]]] = []
        for order in orders:
            try:
                oid = as_oid(order.get("oid", order.get("o")))
            except ValueError:
                continue
            is_buy = order_side_buy(order)
            if is_buy is None:
                continue
            parsed.append((oid, "B" if is_buy else "A", order))
        for side in ("B", "A"):
            slot = out[coin].get(side) or RestSlot()
            match = None
            for oid, o_side, order in parsed:
                if o_side != side or oid in claimed:
                    continue
                cloid = order.get("cloid") or order.get("cloidStr")
                if (slot.oid is not None and oid == slot.oid) or (
                    slot.oid is None and slot.cloid and cloid == slot.cloid
                ):
                    match = (oid, order)
                    break
            if match is not None:
                oid, order = match
                claimed.add(oid)
                slot.oid = oid
                live_sz = _float_or_none(order.get("sz"))
                if live_sz is not None:
                    slot.sz = live_sz
            elif slot.occupied() and now - slot.placed_at >= grace_s:
                slot = RestSlot()
            out[coin][side] = slot
        for oid, side, order in parsed:
            if oid in claimed:
                continue
            slot = out[coin][side]
            cloid = order.get("cloid") or order.get("cloidStr")
            if not slot.occupied() and is_bot_cloid(cloid if isinstance(cloid, str) else None):
                out[coin][side] = RestSlot(
                    oid=oid,
                    cloid=cloid,
                    px=_float_or_none(order.get("limitPx") or order.get("px")),
                    sz=_float_or_none(order.get("sz")),
                    tif=str(order.get("tif") or "Alo"),
                    reduce_only=bool(order.get("reduceOnly")),
                    placed_at=now,
                    stage="adopted",
                )
                claimed.add(oid)
            else:
                extras[coin].append(oid)
    return out, extras


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
    """Cloids for slots that have no oid yet (an oid cancel already covers the rest)."""
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
    if state is None:
        return None
    out = {coin: 0.0 for coin in coins}
    rows = state.get("assetPositions")
    if not isinstance(rows, list):
        return None
    for item in rows:
        pos = (item or {}).get("position") or {}
        coin = match_order_coin({"coin": pos.get("coin"), "a": None}, markets)
        if coin is None or coin not in out:
            raw = str(pos.get("coin") or "")
            if raw in out:
                coin = raw
            else:
                continue
        try:
            out[coin] = float(pos.get("szi") or 0)
        except (TypeError, ValueError):
            out[coin] = 0.0
    return out


def deadman_deadline_ms(ahead_ms: int, *, now_ms: int | None = None) -> int:
    now = int(time.time() * 1000) if now_ms is None else now_ms
    t = now + int(ahead_ms)
    if t - now < DEADMAN_MIN_MS:
        t = now + DEADMAN_FALLBACK_MS
    return t


def deadman_remain_lt_s(ahead_s: float) -> float:
    """Renew once a third of the horizon is left (never later than 8s before it fires)."""
    return max(DEADMAN_REMAIN_LT_S, float(ahead_s) / 3.0)


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
    """Renew scheduleCancel only when unset or remaining time is under `remain_lt_s`."""
    if deadline_s <= 0:
        return True
    return (deadline_s - now_s) < remain_lt_s


def side_occupied(rests: dict[str, dict[str, RestSlot]], coin: str, side: str) -> bool:
    slot = rests.get(coin, {}).get(side)
    return bool(slot and slot.occupied())


def slot_verdict(
    slot: RestSlot,
    want: QuoteIntent,
    *,
    tick: float,
    tolerance_ticks: int,
    stale_quote_s: float,
    now: float,
) -> Verdict:
    """What to do with a resting order whose side still has a target.

    - "keep": close enough; leave it (keeps queue priority, costs nothing).
    - "now":  wrong kind of order (stage / TIF / reduce-only changed). Replace
              without waiting: the flatten ladder and never-add rule depend on it.
    - "slow": price moved past the tolerance, or the rest is stale; replace on
              the MIN_REPLACE_S throttle (book ticks alone never reprice faster).
    """
    if (
        slot.stage != want.stage
        or bool(slot.reduce_only) != bool(want.reduce_only)
        or str(slot.tif).lower() != str(want.tif).lower()
    ):
        return "now"
    if slot.px is None:
        return "slow"
    tol = max(0, tolerance_ticks) * tick + EPS
    if abs(slot.px - want.px) > tol:
        return "slow"
    if want.reduce_only and slot.sz is not None and slot.sz < want.sz - EPS:
        return "slow"  # position grew; resize the reducer
    if (
        want.stage == "flat"
        and stale_quote_s > 0
        and slot.placed_at > 0
        and now - slot.placed_at > stale_quote_s
    ):
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
    last_take_at: dict[str, float] = field(default_factory=dict)
    last_pos_poll: float = 0.0
    last_fill_poll: float = 0.0
    last_reconcile: float = 0.0
    last_budget_poll: float = 0.0
    last_deadman: float = 0.0
    last_replace: dict[str, float] = field(default_factory=dict)
    deadman_until: float = 0.0
    budget_headroom: int | None = None
    writes: int = 0
    _deadman_retry_at: float = 0.0
    _weight_backoff_until: float = 0.0
    _weight_logged: bool = False
    _pos_dirty: bool = False
    _take_misses: dict[str, int] = field(default_factory=dict)
    _reject_until: dict[tuple[str, str], float] = field(default_factory=dict)
    _needs_reconcile: dict[str, bool] = field(default_factory=dict)
    _replace_skip_logged: dict[str, bool] = field(default_factory=dict)
    _block_logged: dict[str, str | None] = field(default_factory=dict)
    _maxpos_logged: dict[str, bool] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _wake: threading.Event = field(default_factory=threading.Event)
    _events: deque[tuple[str, Any]] = field(default_factory=deque)
    books: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict)
    asset_ctx: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict)
    diag: FillDiagnostics | None = None
    _feed: BookFeed | None = None

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
        for coin in coins:
            self._replace_skip_logged.setdefault(coin, False)
            self._take_misses.setdefault(coin, 0)
            self._needs_reconcile.setdefault(coin, False)
        if self.diag is None:
            self.diag = FillDiagnostics(coins)

    # ------------------------------------------------------------------ budget

    def _in_weight_backoff(self) -> bool:
        return time.time() < self._weight_backoff_until

    def _trip_weight_backoff(self, err: object) -> None:
        wait = weight_backoff_s(self.settings.min_replace_s)
        self._weight_backoff_until = time.time() + wait
        if not self._weight_logged:
            log.warning(
                "request weight exhausted; pausing new quotes for %.0fs "
                "(cancels / reduce-only / dead-man still go out; waiting does not "
                "restore the cap, fills do): %s",
                wait,
                err,
            )
            self._weight_logged = True

    def _effective_min_replace(self) -> float:
        base = self.settings.min_replace_s
        reserve = self.settings.request_budget_reserve
        if self.budget_headroom is not None and self.budget_headroom < reserve * BUDGET_SLOW_MULT:
            return base * BUDGET_SLOW_FACTOR
        return base

    def _opening_writes_paused(self) -> str | None:
        """Reason to hold back new non-reduce-only quotes (existing rests stay)."""
        if self._in_weight_backoff():
            return "request-weight backoff"
        reserve = self.settings.request_budget_reserve
        if self.budget_headroom is not None and self.budget_headroom < reserve:
            return f"request budget headroom {self.budget_headroom} < reserve {reserve}"
        if self.deadman_until < time.time() + DEADMAN_MIN_MS / 1000.0:
            return "dead-man not armed"
        return None

    def poll_budget(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_budget_poll < BUDGET_POLL_S:
            return
        self.last_budget_poll = now
        fetch = getattr(self.client, "user_rate_limit", None)
        if fetch is None:
            return
        data = fetch(self.signer.account, optional=True)
        if not data:
            return
        try:
            used = int(data.get("nRequestsUsed"))
            cap = int(data.get("nRequestsCap"))
        except (TypeError, ValueError):
            return
        self.budget_headroom = cap - used
        log.info(
            "request budget used=%s cap=%s headroom=%s cumVlm=%s writes_since_last=%s",
            used,
            cap,
            self.budget_headroom,
            data.get("cumVlm"),
            self.writes,
        )
        self.writes = 0

    def _post(self, payload: dict[str, Any], *, essential: bool = False) -> Any:
        """Signed write. Non-essential writes are skipped during weight backoff."""
        if self._in_weight_backoff() and not essential:
            left = self._weight_backoff_until - time.time()
            raise RequestWeightLimited(f"signed write skipped; request-weight backoff {left:.1f}s left")
        action = payload.get("action") if isinstance(payload, dict) else None
        if isinstance(action, dict):
            batch = action.get("orders") or action.get("cancels")
            self.writes += len(batch) if isinstance(batch, list) and batch else 1
        try:
            resp = self.client.post_exchange(payload)
        except RequestWeightLimited as exc:
            self._trip_weight_backoff(exc)
            raise
        except RateLimited as exc:
            if is_weight_limit_error(exc):
                self._trip_weight_backoff(exc)
            raise
        except Exception as exc:  # noqa: BLE001
            if is_weight_limit_error(exc):
                self._trip_weight_backoff(exc)
            raise
        if is_weight_limit_error(resp):
            exc = RequestWeightLimited(str(resp))
            self._trip_weight_backoff(exc)
            raise exc
        if not self._in_weight_backoff():
            self._weight_logged = False
        return resp

    # ------------------------------------------------------------ WS callbacks

    def on_book(self, coin: str, data: dict[str, Any]) -> None:
        with self._lock:
            self.books[coin] = (time.time(), data)
        self._wake.set()

    def on_asset_ctx(self, coin: str, ctx: dict[str, Any]) -> None:
        with self._lock:
            self.asset_ctx[coin] = (time.time(), ctx)

    def on_order_updates(self, rows: list[dict[str, Any]]) -> None:
        self._events.append(("orders", rows))
        self._wake.set()

    def on_user_fills(self, data: dict[str, Any]) -> None:
        fills = data.get("fills") if isinstance(data, dict) else None
        if not isinstance(fills, list):
            return
        snapshot = bool(data.get("isSnapshot"))
        if not snapshot:
            self._events.append(("fills", fills))
            self._wake.set()
        if self.diag is None:
            return
        try:
            self.diag.ingest_fills(fills, self.peek_book, prime=snapshot)
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag userFills: %s", exc)

    def wait_for_book(self, timeout: float) -> None:
        self._wake.wait(timeout)
        self._wake.clear()

    def _find_slot(self, oid: int | None, cloid: Any = None) -> tuple[str, str] | None:
        for coin, sides in self.rests.items():
            for side, slot in sides.items():
                if oid is not None and slot.oid == oid:
                    return coin, side
                if cloid and slot.oid is None and slot.cloid == cloid:
                    return coin, side
        return None

    def drain_events(self) -> None:
        """Apply WS orderUpdates / fills to the rest cache on the main thread."""
        while self._events:
            kind, rows = self._events.popleft()
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                if kind == "orders":
                    self._apply_order_update(row)
                else:
                    self._apply_fill(row)

    def _apply_order_update(self, row: dict[str, Any]) -> None:
        order = row.get("order") if isinstance(row.get("order"), dict) else row
        status = str(row.get("status") or "")
        try:
            oid = as_oid(order.get("oid"))
        except ValueError:
            return
        where = self._find_slot(oid, order.get("cloid"))
        if where is None:
            return
        coin, side = where
        slot = self.rests[coin][side]
        if status == "open":
            slot.oid = oid
            live_sz = _float_or_none(order.get("sz"))
            if live_sz is not None:
                slot.sz = live_sz
            return
        # filled / canceled / rejected / marginCanceled / reduceOnlyCanceled …
        self.rests[coin][side] = RestSlot()
        if status == "filled":
            self._pos_dirty = True

    def _apply_fill(self, _fill: dict[str, Any]) -> None:
        # Slot status comes from orderUpdates (remaining sz, filled / canceled);
        # subtracting fill sizes here too would double count a partial fill.
        self._pos_dirty = True

    # ------------------------------------------------------------ diagnostics

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

    def poll_user_fills(self, *, force: bool = False) -> None:
        """REST backup for the WS fill stream: incremental `userFillsByTime`."""
        if self.diag is None:
            return
        now = time.time()
        if not force and now - self.last_fill_poll < FILL_POLL_S:
            return
        prime = not self.diag.primed
        if prime:
            start_ms = int(now // 86_400 * 86_400 * 1000)  # UTC midnight: today's PnL
        else:
            start_ms = int((self.last_fill_poll - 5.0) * 1000)
        self.last_fill_poll = now
        fetch = getattr(self.client, "user_fills_by_time", None)
        rows = fetch(self.signer.account, start_ms, optional=True) if fetch else None
        if rows is None:
            if prime:
                rows = self.client.user_fills(self.signer.account, optional=True)
            if rows is None:
                return
        try:
            self.diag.ingest_fills(rows, self.peek_book, prime=prime)
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag userFills poll: %s", exc)

    def flush_markouts(self) -> None:
        if self.diag is None:
            return
        try:
            self.diag.flush_markouts(self.peek_book)
        except Exception as exc:  # noqa: BLE001
            log.warning("fill-diag markout: %s", exc)

    # -------------------------------------------------------------- bootstrap

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
            resp = self._post(payload, essential=True)
            log.info("isolated leverage %s -> %s", market.coin, resp)

    # -------------------------------------------------------------- inventory

    def refresh_positions(self, *, force: bool = False) -> None:
        now = time.time()
        due = now - self.last_pos_poll >= POS_POLL_S
        dirty = self._pos_dirty and now - self.last_pos_poll >= POS_DIRTY_MIN_S
        if not force and not due and not dirty:
            return
        state = self.client.clearinghouse_state(self.signer.account, ALLOWED_DEX, optional=True)
        parsed = positions_from_state(state, tuple(self.markets), self.markets)
        self.last_pos_poll = now
        if parsed is None:
            log.warning("clearinghouseState null/429; keeping inventory cache")
            return
        self._pos_dirty = False
        for coin, szi in parsed.items():
            if abs(szi - self.pos.get(coin, 0.0)) > EPS:
                self._take_misses[coin] = 0
        self.pos_since = sync_pos_since(self.pos, parsed, self.pos_since, now)
        self.pos = parsed

    # ------------------------------------------------------------------ books

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
                if data is None:
                    return None
        return book_top(coin, data or {})

    # ------------------------------------------------------------------- plan

    def plan_for(self, coin: str, top: BookTop) -> QuotePlan:
        now = time.time()
        szi = self.pos.get(coin, 0.0)
        since = self.pos_since.get(coin, 0.0)
        age_ms = int(max(0.0, (now - since) * 1000)) if since and abs(szi) > 0 else 0
        mid = top.mid or 0.0
        cap = max_position_usd(self.settings)
        if abs(szi) > 0 and mid > 0 and abs(szi) * mid >= cap:
            if not self._maxpos_logged.get(coin):
                log.warning(
                    "position %s %.2f USD >= MAX_POSITION_USD %.2f; flatten by IOC now",
                    coin,
                    abs(szi) * mid,
                    cap,
                )
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

    def opening_block(self, coin: str, top: BookTop) -> str | None:
        """Reason not to hold *any* opening (flat) quote. Opening rests get canceled."""
        if coin in self.settings.rth_only_coins and not is_rth():
            return "outside NYSE regular hours (RTH_ONLY_COINS)"
        limit = self.settings.max_daily_loss_usd
        if limit > 0 and self.diag is not None:
            pnl = self.diag.day_net_pnl()
            if pnl <= -limit:
                return f"daily net PnL {pnl:.2f} <= -MAX_DAILY_LOSS_USD {limit:g}"
        max_dev = self.settings.oracle_max_dev_bps
        if max_dev > 0 and top.mid:
            with self._lock:
                entry = self.asset_ctx.get(coin)
            if entry is not None and time.time() - entry[0] <= ORACLE_STALE_S:
                oracle = _float_or_none(entry[1].get("oraclePx"))
                if oracle:
                    dev = abs(top.mid - oracle) / oracle * 10_000.0
                    if dev > max_dev:
                        return f"book mid {top.mid:g} is {dev:.1f} bp from oracle {oracle:g}"
        return None

    def _note_block(self, coin: str, reason: str | None) -> None:
        if self._block_logged.get(coin) == reason:
            return
        if reason:
            log.warning("opening quotes off %s: %s", coin, reason)
        elif self._block_logged.get(coin):
            log.info("opening quotes back on %s", coin)
        self._block_logged[coin] = reason

    # ----------------------------------------------------------------- writes

    def _cancel(self, coin: str, sides: list[str], extra_oids: list[int] | None = None) -> bool:
        """Cancel the given sides (by oid; by cloid only when no oid) plus extras.

        Essential write. Returns False when the outcome is unknown; the cache is
        then kept and the coin is reconciled before anything else is placed.
        """
        market = self.markets[coin]
        oid_pairs: list[tuple[int, int]] = []
        cloid_pairs: list[tuple[int, str]] = []
        for side in sides:
            slot = self.rests[coin][side]
            if slot.oid is not None:
                oid_pairs.append((market.asset_id, slot.oid))
            elif slot.cloid:
                cloid_pairs.append((market.asset_id, slot.cloid))
        for oid in extra_oids or []:
            if all(oid != o for _a, o in oid_pairs):
                oid_pairs.append((market.asset_id, oid))
        if not oid_pairs and not cloid_pairs:
            for side in sides:
                self.rests[coin][side] = RestSlot()
            return True
        try:
            if oid_pairs:
                resp = self._post(self.signer.signed_cancel_oids(oid_pairs), essential=True)
                log.info("cancel %s oids=%s -> %s", coin, [o for _a, o in oid_pairs], resp)
                if not action_ok(resp):
                    self._needs_reconcile[coin] = True
                    return False
            if cloid_pairs:
                resp = self._post(self.signer.signed_cancel_cloids(cloid_pairs), essential=True)
                log.info("cancel %s cloids -> %s", coin, resp)
                if not action_ok(resp):
                    self._needs_reconcile[coin] = True
                    return False
        except Exception as exc:  # noqa: BLE001
            log.warning("cancel %s failed; reconcile before placing: %s", coin, exc)
            self._needs_reconcile[coin] = True
            return False
        # "success" or "already canceled, or filled": either way it is off the book.
        for side in sides:
            self.rests[coin][side] = RestSlot()
        if extra_oids:
            self.extra_oids[coin] = []
        return True

    def _place(self, coin: str, intents: list[QuoteIntent], plan: QuotePlan) -> dict[str, Any] | None:
        if not intents:
            return None
        specs = []
        pending: list[QuoteIntent] = []
        for intent in intents:
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
            pending.append(intent)
        if not specs:
            return None
        payload = self.signer.signed_orders(specs)
        essential = plan.take or all(i.reduce_only for i in pending)
        try:
            resp = self._post(payload, essential=essential)
        except ExchangeUncertain:
            log.warning("place %s timed out; reconcile before placing again", coin)
            self._needs_reconcile[coin] = True
            raise
        now = time.time()
        if not action_ok(resp):
            if is_alo_reject(resp):
                for intent in pending:
                    self._reject_until[(coin, intent.side)] = now + ALO_REJECT_RETRY_S
                log.warning("ALO px rejected, will requote %s %s", coin, resp)
                return {"aloReject": True, "resp": resp}
            log.warning("%s not ok %s %s", "IOC flatten" if plan.take else "order", coin, resp)
            return {"iocFail": plan.take, "resp": resp}
        statuses = resp_statuses(resp)
        accepted = 0
        filled = 0
        out: dict[str, Any] = {"resp": resp}
        for idx, intent in enumerate(pending):
            st = statuses[idx] if idx < len(statuses) else None
            err = status_error(st)
            if err:
                if plan.take:
                    log.warning("IOC flatten status %s %s", coin, err)
                    out["iocFail"] = True
                elif is_alo_reject(err):
                    self._reject_until[(coin, intent.side)] = now + ALO_REJECT_RETRY_S
                    log.warning("ALO px rejected, will requote %s %s %s", coin, intent.side, err)
                    out["aloReject"] = True
                else:
                    log.warning("order status %s %s %s", coin, intent.side, err)
                continue
            if plan.take or intent.tif.lower() == "ioc":
                filled += 1  # IOC never rests
                continue
            self.rests[coin][intent.side] = RestSlot(
                oid=status_oid(st),
                cloid=specs[idx][4],
                px=intent.px,
                sz=intent.sz,
                tif=intent.tif,
                reduce_only=intent.reduce_only,
                placed_at=now,
                stage=intent.stage,
            )
            accepted += 1
        log.info(
            "%s %s %s",
            "take" if plan.take else "rest",
            coin,
            [(i.side, i.px, i.sz, i.tif, "ro" if i.reduce_only else "") for i in pending],
        )
        if plan.take:
            self._pos_dirty = True
            if filled:
                self._take_misses[coin] = 0
        self._diag_quotes(coin, accepted, take=plan.take)
        return out

    # ---------------------------------------------------------------- dead-man

    def _deadman_ahead_ms(self) -> int:
        return int(self.settings.deadman_ahead_s * 1000)

    def schedule_deadman(self, ahead_ms: int | None = None) -> None:
        """Account-wide: cancels ALL open orders on the master when it fires."""
        ahead = self._deadman_ahead_ms() if ahead_ms is None else int(ahead_ms)
        deadline = deadman_deadline_ms(ahead)
        try:
            resp = self._post(self.signer.signed_schedule_cancel(deadline), essential=True)
        except RequestWeightLimited:
            raise
        except RateLimited:
            fallback = deadman_deadline_ms(DEADMAN_FALLBACK_MS)
            resp = self._post(self.signer.signed_schedule_cancel(fallback), essential=True)
            self.last_deadman = time.time()
            if action_ok(resp):
                self.deadman_until = fallback / 1000.0
            log.warning("dead-man 429; fallback +%ss account-wide -> %s", DEADMAN_FALLBACK_MS // 1000, resp)
            return
        self.last_deadman = time.time()
        if not action_ok(resp):
            raise RuntimeError(f"scheduleCancel rejected: {resp}")
        self.deadman_until = deadline / 1000.0
        log.info("dead-man scheduleCancel +%ss account-wide -> %s", ahead // 1000, resp)

    def maybe_deadman(self) -> None:
        now = time.time()
        remain = deadman_remain_lt_s(self.settings.deadman_ahead_s)
        if not deadman_needs_refresh(self.deadman_until, now, remain):
            return
        if now < self._deadman_retry_at:
            return
        try:
            self.schedule_deadman()
        except Exception as exc:  # noqa: BLE001
            # Includes "max 10 triggers per day". New opening quotes pause until it arms.
            log.warning("dead-man renew failed (opening quotes pause until armed): %s", exc)
            self.last_deadman = now
            self._deadman_retry_at = now + DEADMAN_RETRY_S

    def clear_deadman(self) -> None:
        try:
            resp = self._post(self.signer.signed_schedule_cancel(None), essential=True)
            log.info("dead-man scheduleCancel cleared (no time) -> %s", resp)
        except RateLimited:
            self.schedule_deadman(DEADMAN_FALLBACK_MS)
        except Exception as exc:  # noqa: BLE001
            log.warning("dead-man clear failed: %s", exc)

    # -------------------------------------------------------------- reconcile

    def reconcile(self, *, force: bool = False) -> bool:
        """Cross-check the cache with frontendOpenOrders (dex:io). False on null/429."""
        now = time.time()
        pending = any(self._needs_reconcile.values())
        interval = RECONCILE_RETRY_S if pending else RECONCILE_S
        if not force and now - self.last_reconcile < interval:
            return not pending
        self.last_reconcile = now
        opens = self.client.open_orders(self.signer.account, ALLOWED_DEX, optional=True)
        if opens is None:
            log.warning("frontendOpenOrders null/429; keeping rest cache")
            return False
        self.rests, extras = reconcile_open_orders(self.rests, opens, self.markets, now=now)
        for coin in self.markets:
            self.extra_oids[coin] = extras.get(coin, [])
            if self.extra_oids[coin]:
                log.info("extra rests %s oids=%s (will cancel)", coin, self.extra_oids[coin])
            self._needs_reconcile[coin] = False
        return True

    # ---------------------------------------------------------------- requote

    def _take(self, coin: str, plan: QuotePlan) -> None:
        now = time.time()
        gap = min(IOC_GAP_MAX_S, IOC_GAP_S * (2 ** self._take_misses.get(coin, 0)))
        if self._in_weight_backoff():
            gap = max(gap, WEIGHT_LIMITED_GAP_S)
        if now - self.last_take_at.get(coin, 0.0) < gap:
            return
        log.info("flatten take %s age=%.1fs szi=%s", coin, plan.age_ms / 1000, plan.szi)
        occupied = [s for s in ("B", "A") if side_occupied(self.rests, coin, s)]
        extras = list(self.extra_oids.get(coin, []))
        if (occupied or extras) and not self._cancel(coin, occupied, extras):
            log.warning("cancel before IOC failed %s; retry next step", coin)
            return
        self.last_take_at[coin] = now
        self._take_misses[coin] = self._take_misses.get(coin, 0) + 1
        try:
            self._place(coin, plan.intents, plan)
        except RequestWeightLimited:
            log.warning("IOC skipped %s; request-weight limited", coin)
        except RateLimited as exc:
            log.warning("IOC 429/timeout %s: %s", coin, exc)
        except Exception as exc:  # noqa: BLE001
            log.warning("IOC flatten retry %s: %s", coin, exc)
        self.refresh_positions(force=True)

    def requote(self, coin: str, top: BookTop) -> None:
        self.refresh_positions()
        plan = self.plan_for(coin, top)
        if plan.take:
            self._take(coin, plan)
            return
        block = self.opening_block(coin, top) if plan.mode == "flat" else None
        self._note_block(coin, block)
        if not block and not plan.intents and plan.mode == "flat":
            return  # locked / one-sided book: leave rests alone until it clears
        # In a position with no usable flatten price, keep any reducer resting.
        keep_reducers = not block and not plan.intents
        desired = {} if block else {i.side: i for i in plan.intents}
        now = time.time()
        tick = top.inferred_tick(self.markets[coin].sz_decimals)
        elapsed = now - self.last_replace.get(coin, 0.0)
        min_replace = self._effective_min_replace()
        paused = self._opening_writes_paused()
        unknown = self._needs_reconcile.get(coin, False)
        cancel_sides: list[str] = []
        place: list[QuoteIntent] = []
        replacing = False
        held = False
        for side in ("B", "A"):
            slot = self.rests[coin][side]
            want = desired.get(side)
            if not slot.occupied():
                if want is not None:
                    place.append(want)
                continue
            if want is None:
                if keep_reducers and slot.reduce_only:
                    continue
                # Opening side while in a position, or quoting is off: never leave it.
                cancel_sides.append(side)
                continue
            verdict = slot_verdict(
                slot,
                want,
                tick=tick,
                tolerance_ticks=self.settings.requote_tolerance_ticks,
                stale_quote_s=self.settings.stale_quote_s,
                now=now,
            )
            if verdict == "keep":
                continue
            wait = 0.0 if verdict == "now" else min_replace
            if elapsed < wait:
                held = True
                continue
            cancel_sides.append(side)
            place.append(want)
            replacing = True
        # Placement gates: never stack orders on an unknown book, respect ALO
        # reject cool-down, and hold new opening quotes while writes are paused.
        allowed: list[QuoteIntent] = []
        for intent in place:
            if unknown:
                continue
            if now < self._reject_until.get((coin, intent.side), 0.0):
                continue
            if self._in_weight_backoff():
                continue
            if paused and not intent.reduce_only:
                continue
            allowed.append(intent)
        dropped = {i.side for i in place} - {i.side for i in allowed}
        # A replacement whose new order cannot go out keeps its old rest.
        cancel_sides = [s for s in cancel_sides if s not in dropped or s not in desired]
        if held and not self._replace_skip_logged.get(coin):
            log.info(
                "min-replace: keep %s rests (%.1fs < %.0fs); take still allowed",
                coin,
                elapsed,
                min_replace,
            )
            self._replace_skip_logged[coin] = True
        extras = list(self.extra_oids.get(coin, []))
        if not cancel_sides and not allowed and not extras:
            return
        if cancel_sides or extras:
            if not self._cancel(coin, cancel_sides, extras):
                return
        if allowed:
            try:
                self._place(coin, allowed, plan)
            except RequestWeightLimited:
                log.warning("place skipped %s; request-weight backoff", coin)
            except RateLimited as exc:
                log.warning("place 429/timeout %s; continue loop: %s", coin, exc)
            except Exception as exc:  # noqa: BLE001
                log.warning("place failed %s: %s", coin, exc)
        if replacing or allowed:
            self._mark_replaced(coin)

    def _mark_replaced(self, coin: str) -> None:
        self.last_replace[coin] = time.time()
        self._replace_skip_logged[coin] = False

    def _has_rest(self, coin: str) -> bool:
        return side_occupied(self.rests, coin, "B") or side_occupied(self.rests, coin, "A")

    def step(self, feed: BookFeed | None) -> None:
        self._feed = feed
        self.drain_events()
        self.maybe_deadman()
        for name, poll in (
            ("position poll", self.refresh_positions),
            ("budget poll", self.poll_budget),
            ("fill-diag poll", self.poll_user_fills),
            ("reconcile", self.reconcile),
        ):
            try:
                poll()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s: %s", name, exc)
        self.flush_markouts()
        for coin in self.markets:
            try:
                top = self.book_for(coin, feed)
                if top is None or top.bid is None or top.ask is None:
                    continue
                self.requote(coin, top)
            except Exception as exc:  # noqa: BLE001
                log.warning("live step %s: %s", coin, exc)

    def shutdown_cancels(self) -> None:
        """Cancel every rest on this bot's coins, then clear (or shorten) the dead-man."""
        clean = True
        self.drain_events()
        if not self.reconcile(force=True):
            clean = False
        for coin in self.markets:
            sides = [s for s in ("B", "A") if side_occupied(self.rests, coin, s)]
            extras = list(self.extra_oids.get(coin, []))
            if (sides or extras) and not self._cancel(coin, sides, extras):
                clean = False
        if clean:
            self.clear_deadman()
            return
        try:
            self.schedule_deadman(DEADMAN_FALLBACK_MS)
        except Exception as exc:  # noqa: BLE001
            log.warning("shutdown dead-man fallback: %s", exc)


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


def log_startup(settings: Settings, markets: dict[str, Market]) -> None:
    log.info("%s", describe_fee_model(settings))
    log.info("%s", ALO_TIF_NOTE)
    log.info(
        "dead-man account-wide: scheduleCancel +%gs, renew when < %gs left; failures pause "
        "new opening quotes. Clear (no time) on clean stop.",
        settings.deadman_ahead_s,
        deadman_remain_lt_s(settings.deadman_ahead_s),
    )
    log.info(
        "write budget: per-side updates, keep rests within %s tick(s), MIN_REPLACE_S=%ss, "
        "stale refresh %ss, cancels by oid only. Weight error pauses new quotes %ss; "
        "cancels / reduce-only / dead-man still go out. New quotes stop at headroom < %s.",
        settings.requote_tolerance_ticks,
        settings.min_replace_s,
        settings.stale_quote_s or "off",
        weight_backoff_s(settings.min_replace_s),
        settings.request_budget_reserve,
    )
    log.info(
        "risk: RTH-only=%s  oracle guard=%s bp  max position=$%.0f (then IOC)  "
        "daily loss stop=%s  IOC slippage=%s tick(s)",
        ",".join(settings.rth_only_coins) or "none",
        settings.oracle_max_dev_bps or "off",
        max_position_usd(settings),
        f"${settings.max_daily_loss_usd:g}" if settings.max_daily_loss_usd else "off",
        settings.take_slippage_ticks,
    )
    for coin, market in markets.items():
        log.info("%s asset=%s maxLev(meta)=%s szDecimals=%s", coin, market.asset_id, market.max_leverage, market.sz_decimals)


def run_quoter(
    quoter: LiveQuoter,
    feed: BookFeed,
    *,
    seconds: float | None,
    stop: dict[str, bool],
) -> int:
    feed.start()
    if not feed.wait_ready():
        log.error("websocket did not become ready")
        return 1
    deadline = time.time() + seconds if seconds else None
    last_step = 0.0
    while not stop["flag"]:
        if deadline and time.time() >= deadline:
            break
        quoter.wait_for_book(LOOP_S)
        gap = MIN_STEP_S - (time.time() - last_step)
        if gap > 0:
            time.sleep(gap)
        last_step = time.time()
        quoter.step(feed)
    return 0


def install_stop_handlers() -> dict[str, bool]:
    stop = {"flag": False}

    def _handle(_signum: int, _frame: object) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    return stop


def run_live(settings: Settings, *, seconds: float | None = None) -> int:
    require_live(settings)
    signer = LiveSigner(settings.private_key or "", settings.account)
    client = InfoClient(settings.api_url, pause_s=0.0)
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
            ENTROPY_BUILDER,
        )
        log_startup(settings, markets)
        _log_agent(client, signer)
        user_state = client.clearinghouse_state(signer.account, ALLOWED_DEX, optional=True)
        if user_state:
            log.info("isolated user state withdrawable=%s", user_state.get("withdrawable"))
        quoter = LiveQuoter(settings, markets, client, signer)
        if user_state:
            parsed = positions_from_state(user_state, tuple(markets), markets)
            if parsed is not None:
                quoter.pos = parsed
                quoter.pos_since = sync_pos_since({}, parsed, quoter.pos_since, time.time())
        quoter.bootstrap_isolated()
        stop = install_stop_handlers()
        quoter.poll_budget(force=True)
        quoter.poll_user_fills(force=True)
        # Adopt or cancel whatever a previous run left on the book.
        quoter._needs_reconcile = {coin: True for coin in markets}
        feed = BookFeed(
            settings.ws_url,
            settings.coins,
            quoter.on_book,
            user=signer.account,
            on_user_fills=quoter.on_user_fills,
            on_order_updates=quoter.on_order_updates,
            on_asset_ctx=quoter.on_asset_ctx,
        )
        quoter._feed = feed
        return run_quoter(quoter, feed, seconds=seconds, stop=stop)
    finally:
        if feed is not None:
            feed.stop()
        if quoter is not None:
            try:
                quoter.flush_markouts()
            except Exception as exc:  # noqa: BLE001
                log.warning("fill-diag shutdown flush: %s", exc)
            try:
                quoter.shutdown_cancels()
            except Exception as exc:  # noqa: BLE001
                log.warning("shutdown: %s", exc)
        client.close()
