"""两侧分开管理、价格容差、类型不对立即替换；只使用模拟交易所，不联网。"""

from __future__ import annotations

from typing import Any

import pytest

from entropy_bot.live import LiveQuoter, RestSlot, slot_verdict
from entropy_bot.quoting import QuoteIntent
from test_live_throttle import Clock, FakeClient, FakeSigner, _book_data, _settings, _top

COIN = "io:SNDK"


class BookKeepingClient(FakeClient):
    """记住挂单的模拟交易所：frontendOpenOrders 返回真实还挂着的单。"""

    def __init__(self) -> None:
        super().__init__()
        self.resting: dict[int, dict[str, Any]] = {}

    def post_exchange(self, payload: dict[str, Any]) -> Any:
        action = payload["action"]
        if action["type"] == "order":
            resp = super().post_exchange(payload)
            for spec, st in zip(action["orders"], resp["response"]["data"]["statuses"]):
                if "resting" in st:
                    _m, is_buy, px, sz, cloid, ro, tif = spec
                    self.resting[st["resting"]["oid"]] = {
                        "coin": COIN, "oid": st["resting"]["oid"], "side": "B" if is_buy else "A",
                        "limitPx": str(px), "sz": str(sz), "cloid": cloid,
                    }
            return resp
        if action["type"] == "cancel":
            for _asset, oid in action["cancels"]:
                self.resting.pop(oid, None)
        return super().post_exchange(payload)

    def open_orders(self, *args: object, **kwargs: object) -> list:
        return list(self.resting.values())


@pytest.fixture
def setup(markets, monkeypatch):
    clock = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", clock.time)
    client = BookKeepingClient()
    q = LiveQuoter(_settings(coins=(COIN,)), {COIN: markets[COIN]}, client, FakeSigner())
    q.preflight()
    q.deadman_until = clock.t + 10_000
    return clock, client, q


def quote(q, bid: str, ask: str) -> None:
    q.on_book(COIN, _book_data(bid, ask))
    q.requote(COIN, _top(bid, ask, COIN))


def actions(client) -> list[tuple[str, int]]:
    return [(p["action"]["type"], len(p["action"].get("orders") or p["action"].get("cancels") or []))
            for p in client.posts]


def test_one_tick_drift_keeps_both_rests(setup):
    clock, client, q = setup
    quote(q, "1293.8", "1293.9")
    client.posts.clear()
    clock.advance(13)
    quote(q, "1293.9", "1294.0")
    assert client.posts == []


def test_only_the_side_that_moved_is_replaced(setup):
    clock, client, q = setup
    quote(q, "1293.8", "1293.9")
    kept = q.rests[COIN]["B"].oid
    client.posts.clear()
    clock.advance(13)
    quote(q, "1293.8", "1294.2")  # 宽价差：买 1293.9（差 1 tick 保留），卖 1294.1（换）
    assert actions(client) == [("cancel", 1), ("order", 1)]
    assert q.rests[COIN]["B"].oid == kept
    assert q.rests[COIN]["A"].px == pytest.approx(1294.1)


def test_unchanged_book_does_not_refresh_until_stale_quote_s(setup):
    clock, client, q = setup
    quote(q, "1293.8", "1293.9")
    client.posts.clear()
    for _ in range(11):  # 110s < 120s
        clock.advance(10)
        quote(q, "1293.8", "1293.9")
    assert client.posts == []
    clock.advance(15)
    quote(q, "1293.8", "1293.9")
    assert actions(client) == [("cancel", 2), ("order", 2)]


def test_fill_to_position_cancels_opening_side_immediately(setup):
    clock, client, q = setup
    quote(q, "1293.8", "1293.9")
    client.posts.clear()
    clock.advance(1)  # 远小于 MIN_REPLACE_S
    client.state = {"assetPositions": [{"position": {"coin": COIN, "szi": "0.0386"}}]}
    client.resting = {oid: o for oid, o in client.resting.items() if o["side"] == "A"}  # 买单成交
    quote(q, "1293.8", "1293.9")
    # 本地还不知道买单已成交：两侧一起撤（已成交那张交易所回 "already filled"），只挂减仓卖单。
    assert actions(client) == [("cancel", 2), ("order", 1)]
    sell = q.rests[COIN]["A"]
    assert sell.reduce_only and sell.stage == "far"
    assert not q.rests[COIN]["B"].occupied()


def test_partial_fill_cancels_remaining_opening_bid(setup):
    clock, client, q = setup
    quote(q, "1293.8", "1293.9")
    client.posts.clear()
    clock.advance(1)
    # 买单只成交一部分：剩余的非减仓买单还挂着，必须立即撤掉，不能继续加仓。
    client.state = {"assetPositions": [{"position": {"coin": COIN, "szi": "0.01"}}]}
    quote(q, "1293.8", "1293.9")
    assert actions(client)[0] == ("cancel", 2)
    assert all(o["side"] != "B" for o in client.resting.values())


def test_far_to_mid_stage_change_is_not_held_by_min_replace(setup):
    clock, client, q = setup
    client.state = {"assetPositions": [{"position": {"coin": COIN, "szi": "0.0386"}}]}
    q.refresh_positions(force=True)
    q.pos_since[COIN] = clock.t
    quote(q, "1293.8", "1293.9")
    assert q.rests[COIN]["A"].stage == "far"
    client.posts.clear()
    clock.advance(6.5)  # 6s 进入 mid；距上次挂单只有 6.5s < 12s
    quote(q, "1293.8", "1294.4")
    assert actions(client) == [("cancel", 1), ("order", 1)]
    assert q.rests[COIN]["A"].stage == "mid"


def test_kept_rest_metadata_survives_refetch(setup):
    clock, client, q = setup
    quote(q, "1293.8", "1293.9")
    placed_at = q.rests[COIN]["B"].placed_at
    clock.advance(13)
    quote(q, "1293.8", "1294.2")  # 卖侧替换，触发一次挂单列表重建
    b = q.rests[COIN]["B"]
    assert (b.stage, b.placed_at, b.reduce_only) == ("flat", placed_at, False)


def test_slot_verdict_rules():
    want = QuoteIntent(COIN, 1, True, 1293.8, 0.04, "B", "Alo", False, "flat")
    kw = dict(tick=0.1, tolerance_ticks=1, stale_quote_s=120.0, now=1000.0)
    assert slot_verdict(RestSlot(oid=1, px=1293.7, placed_at=990), want, **kw) == "keep"
    assert slot_verdict(RestSlot(oid=1, px=1293.5, placed_at=990), want, **kw) == "slow"
    assert slot_verdict(RestSlot(oid=1, px=1293.8, placed_at=800), want, **kw) == "slow"
    assert slot_verdict(RestSlot(oid=1, px=1293.8, placed_at=990, stage="unknown"), want, **kw) == "now"
    ro = RestSlot(oid=1, px=1293.8, reduce_only=True, stage="far", placed_at=990)
    assert slot_verdict(ro, want, **kw) == "now"
    zero_tol = dict(kw, tolerance_ticks=0)
    assert slot_verdict(RestSlot(oid=1, px=1293.7, placed_at=990), want, **zero_tol) == "slow"


class RejectingClient(BookKeepingClient):
    def __init__(self, error: str) -> None:
        super().__init__()
        self.error = error
        self.open_order_queries = 0

    def post_exchange(self, payload: dict[str, Any]) -> Any:
        if payload["action"]["type"] == "order" and self.error:
            self.posts.append(payload)
            st = [{"error": self.error}] * len(payload["action"]["orders"])
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": st}}}
        return super().post_exchange(payload)

    def open_orders(self, *args: object, **kwargs: object) -> list:
        self.open_order_queries += 1
        return super().open_orders()


def _rejecting(markets, monkeypatch, error):
    clock = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", clock.time)
    client = RejectingClient(error)
    q = LiveQuoter(_settings(coins=(COIN,)), {COIN: markets[COIN]}, client, FakeSigner())
    q.preflight()
    return clock, client, q


def test_margin_reject_backs_off_exponentially(markets, monkeypatch):
    clock, client, q = _rejecting(markets, monkeypatch, "Insufficient margin to place order.")
    orders = lambda: sum(1 for p in client.posts if p["action"]["type"] == "order")  # noqa: E731
    quote(q, "1293.8", "1293.9")
    assert orders() == 1
    queries = client.open_order_queries
    for _ in range(4):  # 2s 内不重试，也不反复拉挂单列表
        clock.advance(0.4)
        quote(q, "1293.8", "1293.9")
    assert orders() == 1
    assert client.open_order_queries == queries
    clock.advance(0.5)
    quote(q, "1293.8", "1293.9")
    assert orders() == 2
    clock.advance(3)
    quote(q, "1293.8", "1293.9")
    assert orders() == 2  # 第二次被拒后要等 4s
    clock.advance(1.5)
    quote(q, "1293.8", "1293.9")
    assert orders() == 3


def test_reject_streak_resets_after_success(markets, monkeypatch):
    clock, client, q = _rejecting(markets, monkeypatch, "Insufficient margin to place order.")
    quote(q, "1293.8", "1293.9")
    clock.advance(2.5)
    quote(q, "1293.8", "1293.9")
    assert q._reject_streak[(COIN, "B")] == 2
    client.error = ""
    clock.advance(4.5)
    quote(q, "1293.8", "1293.9")
    assert q._has_rest(COIN)
    assert (COIN, "B") not in q._reject_streak


def test_ten_dollar_quotes_are_not_under_the_exchange_minimum(markets):
    from entropy_bot.precision import order_size
    from entropy_bot.quoting import quote_plan

    assert order_size(10, 1293.8, 4) * 1293.8 >= 10
    assert order_size(50, 1293.8, 4) == pytest.approx(0.0386)  # 正常金额不变
    plan = quote_plan(markets[COIN], _top("1293.8", "1293.9", COIN), szi=0.0, age_ms=0, notional_usd=10)
    assert all(i.px * i.sz >= 10 for i in plan.intents)


def test_take_slippage_is_opt_in(markets):
    from entropy_bot.quoting import quote_plan

    top = _top("1293.8", "1293.9", COIN)
    default = quote_plan(markets[COIN], top, szi=0.0386, age_ms=16_000, notional_usd=50)
    assert default.intents[0].px == pytest.approx(1293.8)  # 原行为：正好打在买一
    slipped = quote_plan(markets[COIN], top, szi=0.0386, age_ms=16_000, notional_usd=50,
                         take_slippage_ticks=2)
    assert slipped.intents[0].px == pytest.approx(1293.6)
    short = quote_plan(markets[COIN], top, szi=-0.0386, age_ms=16_000, notional_usd=50,
                       take_slippage_ticks=2)
    assert short.intents[0].px == pytest.approx(1294.1)


def test_position_over_cap_skips_the_alo_ladder(setup):
    clock, client, q = setup
    client.state = {"assetPositions": [{"position": {"coin": COIN, "szi": "0.2"}}]}  # ~$259 > $150
    q.refresh_positions(force=True)
    q.pos_since[COIN] = clock.t
    plan = q.plan_for(COIN, _top("1293.8", "1293.9", COIN))
    assert plan.take and plan.tif == "Ioc"
    client.state = {"assetPositions": [{"position": {"coin": COIN, "szi": "0.03"}}]}
    q.refresh_positions(force=True)
    assert q.plan_for(COIN, _top("1293.8", "1293.9", COIN)).stage == "far"


def _gated(markets, monkeypatch, **kw):
    clock = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", clock.time)
    client = BookKeepingClient()
    q = LiveQuoter(_settings(coins=(COIN,), **kw), {COIN: markets[COIN]}, client, FakeSigner())
    q.preflight()
    q.deadman_until = clock.t + 10_000
    return clock, client, q


def test_oracle_deviation_pauses_and_resumes_entries(markets, monkeypatch):
    clock, client, q = _gated(markets, monkeypatch, oracle_max_dev_bps=50.0)
    q.on_asset_ctx(COIN, {"oraclePx": "1280.0"})  # ~107 bp away
    quote(q, "1293.8", "1293.9")
    assert not client.posts
    assert not q.entry_halt, "偏离只是暂停，不能锁死"
    q.on_asset_ctx(COIN, {"oraclePx": "1293.9"})
    clock.advance(5.1)
    quote(q, "1293.8", "1293.9")
    assert actions(client) == [("order", 2)]


def test_oracle_deviation_cancels_resting_quotes(markets, monkeypatch):
    clock, client, q = _gated(markets, monkeypatch, oracle_max_dev_bps=50.0)
    quote(q, "1293.8", "1293.9")
    client.posts.clear()
    q.on_asset_ctx(COIN, {"oraclePx": "1250.0"})
    clock.advance(1)
    quote(q, "1293.8", "1293.9")
    assert actions(client) == [("cancel", 2)]


def test_stale_or_missing_oracle_does_not_block(markets, monkeypatch):
    clock, client, q = _gated(markets, monkeypatch, oracle_max_dev_bps=50.0)
    q.on_asset_ctx(COIN, {"oraclePx": "1000.0"})
    clock.advance(31)  # 超过 30s 没更新：不拿它判断
    quote(q, "1293.8", "1293.9")
    assert actions(client) == [("order", 2)]


def test_rth_only_is_opt_in(markets, monkeypatch):
    monkeypatch.setattr("entropy_bot.live.is_rth", lambda: False)
    _, client, q = _gated(markets, monkeypatch)
    quote(q, "1293.8", "1293.9")
    assert actions(client) == [("order", 2)], "默认不限时段"
    _, client2, q2 = _gated(markets, monkeypatch, rth_only_coins=(COIN,))
    quote(q2, "1293.8", "1293.9")
    assert not client2.posts


def test_rth_gate_still_flattens_positions(markets, monkeypatch):
    monkeypatch.setattr("entropy_bot.live.is_rth", lambda: False)
    clock, client, q = _gated(markets, monkeypatch, rth_only_coins=(COIN,))
    client.state = {"assetPositions": [{"position": {"coin": COIN, "szi": "0.0386"}}]}
    q.refresh_positions(force=True)
    q.pos_since[COIN] = clock.t
    quote(q, "1293.8", "1293.9")
    placed = [spec for p in client.posts if p["action"]["type"] == "order" for spec in p["action"]["orders"]]
    assert len(placed) == 1 and placed[0][-2] is True


def test_session_loss_limit_halts_entries(markets, monkeypatch):
    _, _, q = _gated(markets, monkeypatch, max_session_loss_usd=5.0)
    q.diag.ingest_fills([], lambda _c: None, prime=True)
    q.diag.ingest_fills([{"coin": COIN, "side": "A", "px": "1290", "sz": "0.04", "tid": 9,
                          "crossed": True, "fee": "0.5", "closedPnl": "-5.0"}], lambda _c: None)
    q.step(None)
    assert "MAX_SESSION_LOSS_USD" in q.entry_halt
    assert not q.entry_allowed()


def test_session_loss_ignores_fills_from_before_start(markets, monkeypatch):
    _, _, q = _gated(markets, monkeypatch, max_session_loss_usd=5.0)
    q.diag.ingest_fills([{"coin": COIN, "side": "A", "px": "1290", "sz": "0.04", "tid": 1,
                          "fee": "0", "closedPnl": "-50"}], lambda _c: None, prime=True)
    q.step(None)
    assert not q.entry_halt
