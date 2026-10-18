"""Per-coin / per-side order management, safety writes, risk gates."""

from __future__ import annotations

from typing import Any

import pytest

from entropy_bot.diagnostics import utc_day
from entropy_bot.errors import ExchangeUncertain
from entropy_bot.live import (
    LiveQuoter,
    RestSlot,
    reconcile_open_orders,
    slot_verdict,
)
from entropy_bot.quoting import QuoteIntent
from tests.test_live_throttle import WEIGHT_ERR, Clock, FakeClient, FakeSigner, _settings, _top

SNDK = "io:SNDK"
ANTH = "io:ANTH"


def _actions(client: FakeClient) -> list[tuple[str, int]]:
    out = []
    for payload in client.posts:
        action = payload.get("action") or {}
        batch = action.get("orders") or action.get("cancels") or []
        out.append((action.get("type"), len(batch)))
    return out


def _q(markets, clock: Clock, coins=(SNDK,), client: FakeClient | None = None, **kw) -> LiveQuoter:
    kw.setdefault("rth_only_coins", ())
    q = LiveQuoter(
        _settings(coins=coins, **kw),
        {c: markets[c] for c in coins},
        client or FakeClient(),
        FakeSigner(),
    )
    q.deadman_until = clock.t + 1_000.0
    return q


@pytest.fixture
def clock(monkeypatch) -> Clock:
    c = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", c.time)
    return c


def test_requote_one_coin_never_cancels_another(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, coins=(ANTH, SNDK), client=client)
    q.requote(ANTH, _top("1985.0", "1985.1"))
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    client.posts.clear()
    clock.advance(13)
    q.requote(ANTH, _top("1985.5", "1985.6"))
    assert _actions(client) == [("cancel", 2), ("order", 2)]
    assert q._has_rest(SNDK)
    for _a, _o in client.posts[0]["action"]["cancels"]:
        assert _a == markets[ANTH].asset_id


def test_cancel_by_oid_only_no_duplicate_cloid_cancel(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    client.posts.clear()
    clock.advance(13)
    q.requote(SNDK, _top("1294.8", "1294.9", SNDK))
    kinds = [k for k, _n in _actions(client)]
    assert "cancelByCloid" not in kinds
    assert kinds == ["cancel", "order"]


def test_one_tick_drift_keeps_rests(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    client.posts.clear()
    clock.advance(60)
    q.requote(SNDK, _top("1293.9", "1294.0", SNDK))
    assert client.posts == []


def test_only_the_moved_side_is_replaced(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    buy_oid = q.rests[SNDK]["B"].oid
    client.posts.clear()
    clock.advance(13)
    q.requote(SNDK, _top("1293.8", "1294.2", SNDK))  # wide: buy 1293.9 (1 tick), sell 1294.1
    assert _actions(client) == [("cancel", 1), ("order", 1)]
    assert q.rests[SNDK]["B"].oid == buy_oid


def test_fill_cancels_opening_side_immediately(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    client.posts.clear()
    clock.advance(1.0)  # far inside MIN_REPLACE_S
    q.pos[SNDK] = 0.0386
    q.pos_since[SNDK] = clock.t
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert _actions(client) == [("cancel", 2), ("order", 1)]
    assert not q.rests[SNDK]["B"].occupied()
    assert q.rests[SNDK]["A"].reduce_only


def test_weight_backoff_still_sends_cancels_and_keeps_rests(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    q._trip_weight_backoff(WEIGHT_ERR)
    client.posts.clear()
    clock.advance(13)
    # Price moved: maker reprice is held, rests stay resting.
    q.requote(SNDK, _top("1294.8", "1294.9", SNDK))
    assert client.posts == []
    assert q._has_rest(SNDK)
    # Position appears: the opening side must still be canceled during backoff.
    q.pos[SNDK] = 0.0386
    q.pos_since[SNDK] = clock.t
    q.requote(SNDK, _top("1294.8", "1294.9", SNDK))
    assert ("cancel", 1) in _actions(client)
    assert not any(k == "order" for k, _n in _actions(client))
    assert not q.rests[SNDK]["B"].occupied()


def test_deadman_renews_during_weight_backoff(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q._trip_weight_backoff(WEIGHT_ERR)
    q.deadman_until = clock.t + 5.0
    q.maybe_deadman()
    assert [k for k, _n in _actions(client)] == ["scheduleCancel"]
    assert q.deadman_until > clock.t + 50


def test_deadman_not_armed_pauses_opening_quotes(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.deadman_until = 0.0
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert client.posts == []


class IocMissClient(FakeClient):
    def post_exchange(self, payload: dict[str, Any]) -> Any:
        action = payload.get("action") or {}
        if action.get("type") == "order":
            self.posts.append(payload)
            err = {"error": "Order could not immediately match against any resting orders."}
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [err]}}}
        return super().post_exchange(payload)


def test_take_slippage_and_ioc_backoff(markets, clock):
    client = IocMissClient()
    q = _q(markets, clock, client=client, take_slippage_ticks=2)
    q.pos[SNDK] = 0.0386
    q.pos_since[SNDK] = clock.t - 16
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    orders = [p for p in client.posts if p["action"]["type"] == "order"]
    assert len(orders) == 1
    spec = orders[0]["action"]["orders"][0]
    assert spec[2] == pytest.approx(1293.6)  # long exit @bid - 2 ticks
    assert spec[-1] == "Ioc" and spec[-2] is True
    # Position unchanged (miss): next IOC waits longer than the base 0.4s gap.
    clock.advance(0.5)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert len([p for p in client.posts if p["action"]["type"] == "order"]) == 1
    clock.advance(0.5)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert len([p for p in client.posts if p["action"]["type"] == "order"]) == 2


def test_max_position_goes_straight_to_ioc(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client, max_position_usd=40.0)
    q.pos[SNDK] = 0.0386  # ~$50 > $40
    q.pos_since[SNDK] = clock.t  # age 0: would normally be the "far" stage
    plan = q.plan_for(SNDK, _top("1293.8", "1293.9", SNDK))
    assert plan.take and plan.tif == "Ioc"


def test_rth_gate_cancels_opening_quotes_but_flatten_runs(markets, clock, monkeypatch):
    client = FakeClient()
    q = _q(markets, clock, client=client, rth_only_coins=(SNDK,))
    monkeypatch.setattr("entropy_bot.live.is_rth", lambda: True)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert q._has_rest(SNDK)
    client.posts.clear()
    monkeypatch.setattr("entropy_bot.live.is_rth", lambda: False)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert _actions(client) == [("cancel", 2)]
    client.posts.clear()
    q.pos[SNDK] = -0.0386
    q.pos_since[SNDK] = clock.t
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert _actions(client) == [("order", 1)]
    assert q.rests[SNDK]["B"].reduce_only


def test_oracle_deviation_blocks_opening(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client, oracle_max_dev_bps=50.0)
    q.on_asset_ctx(SNDK, {"oraclePx": "1200.0"})
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert client.posts == []
    q.on_asset_ctx(SNDK, {"oraclePx": "1293.9"})
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert _actions(client) == [("order", 2)]


def test_daily_loss_limit_blocks_opening(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client, max_daily_loss_usd=5.0)
    q.diag.day_pnl[utc_day()] = -6.0
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert client.posts == []


def test_order_update_and_fill_clear_slots(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    buy = q.rests[SNDK]["B"]
    sell = q.rests[SNDK]["A"]
    q.on_order_updates([{"order": {"coin": SNDK, "oid": buy.oid, "sz": "0"}, "status": "filled"}])
    # Partial fill: orderUpdates "open" carries the remaining size; the fill row
    # itself must not subtract again.
    q.on_order_updates([{"order": {"coin": SNDK, "oid": sell.oid, "sz": "0.02"}, "status": "open"}])
    q.on_user_fills({"fills": [{"coin": SNDK, "oid": sell.oid, "sz": "0.0186", "px": "1293.9", "side": "A"}]})
    q.drain_events()
    assert not q.rests[SNDK]["B"].occupied()
    assert q.rests[SNDK]["A"].sz == 0.02
    assert q._pos_dirty


def test_failed_reconcile_retries_are_spaced(markets, clock):
    calls = []

    class NullOpens(FakeClient):
        def open_orders(self, *args: object, **kwargs: object):
            calls.append(1)
            return None

    q = _q(markets, clock, client=NullOpens())
    q._needs_reconcile[SNDK] = True
    for _ in range(10):
        q.reconcile()
        clock.advance(0.3)
    assert len(calls) == 2  # t=0 and t≈2.1s, not every step


def test_timeout_on_place_blocks_until_reconciled(markets, clock):
    client = FakeClient(exchange_error=ExchangeUncertain("read timed out"))
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert q._needs_reconcile[SNDK]
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert client.posts == []
    q.reconcile()
    assert not q._needs_reconcile[SNDK]
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert _actions(client) == [("order", 2)]


def test_reconcile_keeps_known_drops_gone_adopts_bot_and_flags_foreign(markets):
    rests = {SNDK: {"B": RestSlot(oid=11, px=1293.8, sz=0.04, stage="flat"), "A": RestSlot(oid=12, placed_at=0.0)}}
    opens: list[dict[str, Any]] = [
        {"coin": SNDK, "oid": 11, "side": "B", "limitPx": "1293.8", "sz": "0.02"},
        {"coin": SNDK, "oid": 20, "side": "A", "limitPx": "1294.0", "sz": "0.04", "cloid": "0x45424f54aa"},
        {"coin": SNDK, "oid": 21, "side": "A", "limitPx": "1294.1", "sz": "0.04", "cloid": "0xdead"},
    ]
    out, extras = reconcile_open_orders(rests, opens, {SNDK: markets[SNDK]}, now=100.0)
    assert out[SNDK]["B"].oid == 11 and out[SNDK]["B"].sz == 0.02 and out[SNDK]["B"].stage == "flat"
    assert out[SNDK]["A"].oid == 20 and out[SNDK]["A"].stage == "adopted"
    assert extras[SNDK] == [21]


def test_reconcile_grace_keeps_fresh_rest(markets):
    rests = {SNDK: {"B": RestSlot(oid=11, placed_at=99.5), "A": RestSlot()}}
    out, _ = reconcile_open_orders(rests, [], {SNDK: markets[SNDK]}, now=100.0)
    assert out[SNDK]["B"].oid == 11


def test_slot_verdict_rules():
    want = QuoteIntent(SNDK, 1, True, 1293.8, 0.04, "B", "Alo", False, "flat")
    base = dict(tick=0.1, tolerance_ticks=1, stale_quote_s=120.0, now=1000.0)
    assert slot_verdict(RestSlot(oid=1, px=1293.7, placed_at=990.0), want, **base) == "keep"
    assert slot_verdict(RestSlot(oid=1, px=1293.5, placed_at=990.0), want, **base) == "slow"
    assert slot_verdict(RestSlot(oid=1, px=1293.8, placed_at=800.0), want, **base) == "slow"
    ro = RestSlot(oid=1, px=1293.8, reduce_only=True, stage="far", placed_at=990.0)
    assert slot_verdict(ro, want, **base) == "now"


def test_shutdown_cancels_everything_then_clears_deadman(markets, clock):
    client = FakeClient()
    q = _q(markets, clock, client=client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    client.posts.clear()
    clock.advance(5)
    q.shutdown_cancels()
    kinds = [k for k, _n in _actions(client)]
    assert kinds[-1] == "scheduleCancel"
    assert "time" not in client.posts[-1]["action"]
