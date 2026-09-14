"""只使用模拟交易所；不得访问网络或加载实盘凭据。"""

import pytest
import requests

from entropy_bot.errors import ExchangeOutcomeUnknown, LiveGuardError, RequestWeightLimited
from entropy_bot.live import RestSlot, positions_from_state
from entropy_bot.rest import InfoClient
from entropy_bot.safety import RequestBudget, cancel_confirmed
from test_live_throttle import Clock, FakeClient, WEIGHT_ERR, _quoter, _top, _order_posts


@pytest.fixture
def setup(markets, monkeypatch):
    clock = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", clock.time)
    client = FakeClient()
    q = _quoter(markets, clock=clock, client=client)
    return clock, client, q


def response(statuses):
    return {"status": "ok", "response": {"type": "order", "data": {"statuses": statuses}}}


@pytest.mark.parametrize("raw", [None, {}, {"nRequestsUsed": "NaN"},
    {"nRequestsCap": 100, "nRequestsUsed": -1, "nRequestsSurplus": 0},
    {"nRequestsCap": True, "nRequestsUsed": 0, "nRequestsSurplus": 0}])
def test_budget_fails_closed(raw):
    b = RequestBudget()
    assert not b.update(raw, 100)
    assert not b.can_enter(100, 1)


def test_budget_local_debit_not_refunded_by_stale_snapshot():
    b = RequestBudget()
    raw = {"nRequestsCap": 100, "nRequestsUsed": 70, "nRequestsSurplus": 0}
    assert b.update(raw, 100)
    assert b.can_enter(100, 1)
    b.debit(5)
    b.update(raw, 101)
    assert b.available == 25
    assert not b.can_enter(101, 1)
    assert not b.can_enter(130, 1)


@pytest.mark.parametrize("bad", [None, "oops", "nan", "inf"])
def test_bad_position_is_unknown_not_flat(markets, bad):
    state = {"assetPositions": [{"position": {"coin": "io:SNDK", "szi": bad}}]}
    assert positions_from_state(state, tuple(markets), markets) is None


def test_startup_deficit_sends_no_writes(setup):
    _, client, q = setup
    client.rate = {"nRequestsUsed": 19439, "nRequestsCap": 18388, "nRequestsSurplus": 0}
    with pytest.raises(LiveGuardError):
        q.preflight()
    assert not client.posts


@pytest.mark.parametrize("state,opens", [(None, []), ({"assetPositions": []}, None),
    ({"assetPositions": [{"position": {"coin": "io:ANTH", "szi": ".01"}}]}, []),
    ({"assetPositions": []}, [{"coin": "io:ANTH", "oid": 123, "side": "B"}])])
def test_startup_unknown_or_nonempty_no_writes(setup, state, opens):
    _, client, q = setup
    client.state, client.opens = state, opens
    with pytest.raises(LiveGuardError):
        q.preflight()
    assert not client.posts


def test_normal_preflight_is_read_only(setup):
    _, client, q = setup
    q.preflight()
    assert q.entry_allowed()
    assert not client.posts


def test_limit_does_not_block_paced_reduce_only_exit(setup):
    clock, client, q = setup
    q.preflight()
    q._trip_weight_backoff(WEIGHT_ERR)
    client.state = {"assetPositions": [{"position": {"coin": "io:ANTH", "szi": "-.02"}}]}
    q.pos["io:ANTH"] = -.02
    q.pos_since["io:ANTH"] = clock.t - 20
    q.requote("io:ANTH", _top("1985", "1985.1"))
    assert not _order_posts(client)
    clock.advance(11)
    q.requote("io:ANTH", _top("1985", "1985.1"))
    assert q._in_weight_backoff(), "旧全局退避仍在，但减仓单可走独立通道"
    orders = _order_posts(client)
    assert len(orders) == 1
    assert orders[0]["action"]["orders"][0][-2:] == (True, "Ioc")
    assert not q._has_rest("io:ANTH"), "filled 不能伪装成挂单"
    clock.advance(1)
    q.requote("io:ANTH", _top("1985", "1985.1"))
    assert len(_order_posts(client)) == 1


def test_cancel_during_limit_uses_only_oid_once(setup):
    _, client, q = setup
    q._trip_weight_backoff(WEIGHT_ERR)
    q.rests["io:ANTH"]["B"] = RestSlot(oid=123, cloid="0x45424f54ab")
    result = q.cancel_coin_rests()
    assert not result["rateLimited"]
    assert [p["action"]["type"] for p in client.posts] == ["cancel"]


def test_failed_cancel_preserves_cache_and_blocks_replacement(setup):
    _, client, q = setup
    q.preflight()
    q.rests["io:ANTH"]["B"] = RestSlot(oid=123, cloid="0x45424f54ab")
    client.exchange_error = response([{"error": "temporary failure"}])
    client.opens = None
    q.requote("io:ANTH", _top("1985", "1985.1"))
    assert q.rests["io:ANTH"]["B"].oid == 123
    assert not _order_posts(client)


def test_cancel_outer_ok_not_enough():
    assert not cancel_confirmed(response([{"error": "bad signature"}]), 1)
    assert not cancel_confirmed(response([]), 1)
    assert cancel_confirmed(response(["success"]), 1)
    assert cancel_confirmed(response([{"error": "Order was never placed, already canceled, or filled."}]), 1)


@pytest.mark.parametrize("first", [{"error": "Bad Alo Px"}, {"error": WEIGHT_ERR}])
def test_mixed_batch_keeps_successful_second_side(setup, first):
    _, client, q = setup
    q.preflight()
    client.exchange_error = response([first, {"resting": {"oid": 999}}])
    q._place("io:ANTH", q.plan_for("io:ANTH", _top("1985", "1985.1")))
    assert q.rests["io:ANTH"]["A"].oid == 999
    assert not q.rests["io:ANTH"]["B"].occupied()


def test_unknown_write_never_blindly_retries(setup):
    clock, client, q = setup
    q.preflight()
    client.exchange_error = ExchangeOutcomeUnknown("lost response")
    q.requote("io:ANTH", _top("1985", "1985.1"))
    assert q.unknown_write and q.entry_halt
    assert q.rests["io:ANTH"]["B"].cloid
    clock.advance(60)
    q.requote("io:ANTH", _top("1985", "1985.1"))
    assert not _order_posts(client)


def test_missing_batch_status_is_unknown(setup):
    _, client, q = setup
    q.preflight()
    client.exchange_error = response([{"resting": {"oid": 123}}])
    q._place("io:ANTH", q.plan_for("io:ANTH", _top("1985", "1985.1")))
    assert q.unknown_write
    assert q.rests["io:ANTH"]["B"].oid == 123
    assert q.rests["io:ANTH"]["A"].cloid


def test_stale_book_fallback_failure_does_not_reuse_old_book(setup, monkeypatch):
    clock, client, q = setup
    q.on_book("io:ANTH", {"levels": [[{"px": "1", "sz": "1"}], [{"px": "2", "sz": "1"}]]})
    clock.advance(16)
    monkeypatch.setattr(client, "l2_book", lambda c: (_ for _ in ()).throw(RuntimeError("offline")))
    assert q.book_for("io:ANTH", None) is None


def test_failed_deadman_never_fakes_acknowledged_deadline(setup):
    clock, client, q = setup
    q.deadman_until = 0
    client.exchange_error = {"status": "err", "response": "rejected"}
    q.schedule_deadman()
    assert q.deadman_until == 0
    assert q.entry_halt


def test_fill_notification_invalidates_position_snapshot(setup):
    _, _, q = setup
    q.preflight()
    q.on_user_fills({"fills": [{}]})
    assert not q.position_fresh()


def test_unmanaged_order_is_not_canceled(setup):
    _, client, q = setup
    client.opens = [{"coin": "io:ANTH", "oid": 55, "side": "B", "limitPx": "1"}]
    q.cancel_coin_rests()
    assert q.entry_halt
    assert not client.posts


def test_shutdown_residual_position_reports_failure_without_flatten(setup):
    _, client, q = setup
    client.state = {"assetPositions": [{"position": {"coin": "io:ANTH", "szi": ".01"}}]}
    with pytest.raises(LiveGuardError, match="NOT CLEAN"):
        q.shutdown_cancels()
    assert not _order_posts(client)


def test_transport_timeout_not_mislabeled_as_429(monkeypatch):
    client = InfoClient("https://api.hyperliquid.xyz")
    monkeypatch.setattr(client.session, "post", lambda *a, **k: (_ for _ in ()).throw(requests.Timeout()))
    with pytest.raises(ExchangeOutcomeUnknown):
        client.post_exchange({"action": {"type": "order", "orders": []}})
    client.close()


def test_cancel_fill_race_does_not_place_old_flat_plan(setup, monkeypatch):
    _, client, q = setup
    q.preflight()
    q.rests["io:ANTH"]["B"] = RestSlot(oid=123)
    original = client.post_exchange
    def fill_during_cancel(payload):
        client.state = {"assetPositions": [{"position": {"coin": "io:ANTH", "szi": ".02"}}]}
        return original(payload)
    monkeypatch.setattr(client, "post_exchange", fill_during_cancel)
    q.requote("io:ANTH", _top("1985", "1985.1"))
    assert not _order_posts(client)
    assert q.pos["io:ANTH"] == .02


def test_info_429_is_cooled_down(monkeypatch):
    from types import SimpleNamespace
    client = InfoClient("https://api.hyperliquid.xyz")
    calls = []
    def limited(*args, **kwargs):
        calls.append(1)
        return SimpleNamespace(status_code=429)
    monkeypatch.setattr(client.session, "post", limited)
    assert client.user_rate_limit("0x" + "11" * 20) is None
    assert client.user_rate_limit("0x" + "11" * 20) is None
    assert len(calls) == 1
    client.close()


@pytest.mark.parametrize("body", [None, [], {"unexpected": "shape"}])
def test_exchange_unknown_schema_is_not_success(monkeypatch, body):
    from types import SimpleNamespace
    client = InfoClient("https://api.hyperliquid.xyz")
    fake = SimpleNamespace(status_code=200, ok=True, content=b"x", json=lambda: body)
    monkeypatch.setattr(client.session, "post", lambda *a, **k: fake)
    with pytest.raises(ExchangeOutcomeUnknown):
        client.post_exchange({"action": {"type": "order", "orders": []}})
    client.close()


def test_run_live_preflight_failure_does_not_invoke_shutdown_writes(setup, markets, monkeypatch):
    from entropy_bot.live import run_live
    from test_live_throttle import FakeSigner, _settings
    from conftest import PERP_DEXS
    _, client, _ = setup
    client.state = None
    monkeypatch.setattr("entropy_bot.live.LiveSigner", lambda *a: FakeSigner())
    monkeypatch.setattr("entropy_bot.live.InfoClient", lambda *a: client)
    monkeypatch.setattr(client, "perp_dexs", lambda: PERP_DEXS, raising=False)
    monkeypatch.setattr(client, "close", lambda: None, raising=False)
    monkeypatch.setattr("entropy_bot.live.load_io_markets", lambda *a: (
        {}, {"io:ANTH": markets["io:ANTH"]}, {}))
    with pytest.raises(LiveGuardError):
        run_live(_settings(), seconds=.1)
    assert not client.posts
