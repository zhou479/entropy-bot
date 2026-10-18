"""人工值守限时运行；全部模拟，不读取真实密钥或连接网络。"""

import signal

import pytest

import entropy_bot.live as live
from entropy_bot.cli import build_parser
from entropy_bot.errors import LiveGuardError, RateLimited, RequestWeightLimited
from entropy_bot.session import SupervisedSession
from test_live_throttle import Clock, FakeClient, FakeSigner, _settings, _quoter, _top, _order_posts


@pytest.fixture
def setup(markets, monkeypatch):
    clock = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", clock.time)
    monkeypatch.setattr("entropy_bot.session.time.monotonic", clock.time)
    monkeypatch.setattr("entropy_bot.live.time.sleep", clock.advance)
    client = FakeClient()
    q = _quoter(markets, clock=clock, client=client)
    q.session = SupervisedSession(7200, started_at=clock.t)
    q.deadman_until = 0
    q.preflight()
    return clock, client, q


@pytest.mark.parametrize("seconds", [None, 0, -1, 10801, float("nan"), float("inf"), True])
def test_supervised_rejects_invalid_duration_before_any_api(seconds, monkeypatch):
    monkeypatch.setattr(live, "LiveSigner", lambda *args: pytest.fail("must not sign"))
    with pytest.raises(LiveGuardError, match="10800"):
        live.run_live(_settings(), seconds=seconds, supervised_no_deadman=True)


def test_cli_default_stays_strict():
    parser = build_parser()
    assert not parser.parse_args(["live"]).supervised_no_deadman
    args = parser.parse_args(["live", "--supervised-no-deadman", "--seconds", "7200"])
    assert args.supervised_no_deadman and args.seconds == 7200


def test_supervised_quotes_without_sending_or_faking_deadman(setup):
    _, client, q = setup
    q.requote("io:ANTH", _top("1985", "1985.1"))
    q.schedule_deadman()
    q.maybe_deadman()
    q.clear_deadman()
    assert len(_order_posts(client)) == 1
    assert q.deadman_until == 0
    assert not any(p["action"]["type"] == "scheduleCancel" for p in client.posts)
    assert all(spec[-1] == "Alo" for spec in _order_posts(client)[0]["action"]["orders"])


def test_strict_rejection_keeps_exchange_reason_and_no_entry(setup):
    _, client, q = setup
    q.session = None
    client.exchange_error = {"status": "err", "response": "Required: $1000000.\nTraded: $9424.13."}
    q.schedule_deadman()
    assert "Required: $1000000. Traded: $9424.13." in q.entry_halt
    assert q.deadman_until == 0
    q._place("io:ANTH", q.plan_for("io:ANTH", _top("1985", "1985.1")))
    assert not _order_posts(client)
    with pytest.raises(LiveGuardError, match="SAFETY STOP"):
        q.raise_if_halted_and_flat()


@pytest.mark.parametrize("failure", ["stale", "budget", "unknown", "halt"])
def test_supervised_retains_other_entry_gates(setup, failure):
    _, client, q = setup
    if failure == "stale":
        q.positions_valid = False
    elif failure == "budget":
        q.budget.available = 0
    elif failure == "unknown":
        q.unknown_write = True
    else:
        q.halt_entries("test")
    q._place("io:ANTH", q.plan_for("io:ANTH", _top("1985", "1985.1")))
    assert not _order_posts(client)


def test_cutoff_blocks_entry_even_before_runner_observes_it(setup):
    clock, client, q = setup
    clock.t = q.session.entry_cutoff
    with pytest.raises(RequestWeightLimited):
        q._post({"action": {"type": "order"}}, purpose="entry")
    assert not client.posts
    assert not q.session.stop_reason


def test_no_exit_submission_after_grace_deadline(setup):
    clock, client, q = setup
    q.session.request_stop("manual")
    clock.advance(60)
    with pytest.raises(RateLimited, match="deadline"):
        q._post({"action": {"type": "order"}}, purpose="exit")
    assert not client.posts


def test_stop_deadline_cannot_be_extended_by_repeated_requests(setup):
    clock, _, q = setup
    assert q.session.request_stop("manual")
    deadline = q.session.drain_deadline
    clock.advance(20)
    assert not q.session.request_stop("other")
    assert q.session.drain_deadline == deadline


def stop_state(**updates):
    state = {"flag": False, "signal": None, "count": 0}
    state.update(updates)
    return state


@pytest.mark.parametrize("seconds", [300, 7200, 10800])
def test_virtual_session_stops_before_duration_when_flat(setup, monkeypatch, caplog, seconds):
    clock, _, q = setup
    started = clock.t
    q.session = SupervisedSession(seconds, started_at=started)
    monkeypatch.setattr(q, "step", lambda feed: clock.advance(5))
    caplog.set_level("INFO", logger="entropy_bot.live")
    assert live._run_supervised(q, None, stop_state()) == 0
    assert started + seconds - 30 <= clock.t <= started + seconds + 60
    assert "SESSION DONE reason=duration" in caplog.text
    assert not q.entry_allowed()


def test_cancel_fill_race_uses_original_reduce_only_ioc_ladder(setup, monkeypatch):
    clock, client, q = setup
    start = clock.t
    q.pos_since["io:ANTH"] = start
    actual_post = client.post_exchange
    exit_times = []
    original_cancel = q.cancel_coin_rests
    raced = False

    def cancel_with_race(**kwargs):
        nonlocal raced
        result = original_cancel(**kwargs)
        if not raced:
            raced = True
            # 初次撤单时发生一笔成交；后续退出不能信任撤单前的空仓缓存。
            client.state = {"assetPositions": [{"position": {"coin": "io:ANTH", "szi": ".02"}}]}
        return result

    def fill_ioc(payload):
        result = actual_post(payload)
        if payload["action"]["type"] == "order":
            specs = payload["action"]["orders"]
            assert all(spec[-2] for spec in specs), "draining must never add exposure"
            if specs[0][-1] == "Ioc":
                exit_times.append(clock.t)
                # 第一次只成交部分，第二次才空仓。
                remaining = ".01" if len(exit_times) == 1 else "0"
                client.state = {"assetPositions": [{"position": {"coin": "io:ANTH", "szi": remaining}}]}
        return result

    monkeypatch.setattr(q, "cancel_coin_rests", cancel_with_race)
    monkeypatch.setattr(client, "post_exchange", fill_ioc)
    assert live._run_supervised(q, None, stop_state(flag=True, signal=signal.SIGINT, count=1)) == 0
    assert len(exit_times) == 2
    assert exit_times[0] - start >= 15
    assert exit_times[1] - exit_times[0] >= 3
    assert q.pos["io:ANTH"] == 0
    assert not any(p["action"]["type"] == "scheduleCancel" for p in client.posts)


@pytest.mark.parametrize("failure", ["cancel", "position", "unknown"])
def test_unconfirmed_cleanup_times_out_instead_of_reporting_success(setup, monkeypatch, failure):
    clock, client, q = setup
    attempts = []
    if failure == "cancel":
        def fail_cancel(**kwargs):
            attempts.append(clock.t)
            return {"rateLimited": True}
        monkeypatch.setattr(q, "cancel_coin_rests", fail_cancel)
    elif failure == "position":
        client.state = None
    else:
        q.unknown_write = True
    with pytest.raises(LiveGuardError, match="cleanup timed out"):
        live._run_supervised(q, None, stop_state(flag=True, signal=signal.SIGTERM, count=1))
    assert clock.t <= q.session.started_at + 61
    assert not _order_posts(client)
    if attempts:
        assert len(attempts) <= 13
        assert all(b - a >= 5 for a, b in zip(attempts, attempts[1:]))


def test_second_signal_does_not_start_more_orders(setup):
    _, client, q = setup
    with pytest.raises(LiveGuardError, match="second signal"):
        live._run_supervised(q, None, stop_state(flag=True, signal=signal.SIGINT, count=2))
    assert not client.posts


def test_budget_halt_stops_session_with_failure_even_when_flat(setup):
    _, client, q = setup
    q.halt_entries("request reserve too low")
    with pytest.raises(LiveGuardError, match="SAFETY STOP"):
        live._run_supervised(q, None, stop_state())
    assert not _order_posts(client)


def test_supervised_shutdown_does_not_clear_unavailable_deadman(setup):
    _, client, q = setup
    q.shutdown_cancels()
    assert not client.posts


def test_final_check_observes_fill_after_open_order_query(setup, monkeypatch):
    _, client, q = setup
    def orders_then_fill(*args, **kwargs):
        client.state = {"assetPositions": [{"position": {"coin": "io:ANTH", "szi": ".02"}}]}
        return []
    monkeypatch.setattr(client, "open_orders", orders_then_fill)
    assert not q.session_account_clear()


def test_run_live_supervised_routes_mode_and_restores_signal_handlers(setup, monkeypatch):
    from conftest import PERP_DEXS
    _, client, q = setup
    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    old_handlers = dict(handlers)

    class Feed:
        def __init__(self, *args, **kwargs):
            pass
        def start(self):
            pass
        def stop(self):
            pass
        def wait_ready(self):
            return True

    monkeypatch.setattr(live, "LiveSigner", lambda *args: FakeSigner())
    monkeypatch.setattr(live, "InfoClient", lambda *args: client)
    monkeypatch.setattr(live, "LiveQuoter", lambda *args: q)
    monkeypatch.setattr(live, "BookFeed", Feed)
    monkeypatch.setattr(live, "load_io_markets", lambda *args: ({}, q.markets, {}))
    monkeypatch.setattr(client, "perp_dexs", lambda: PERP_DEXS, raising=False)
    monkeypatch.setattr(client, "close", lambda: None, raising=False)
    monkeypatch.setattr(live.signal, "getsignal", lambda s: handlers[s])
    monkeypatch.setattr(live.signal, "signal", lambda s, h: handlers.update({s: h}))

    def session_runner(quoter, feed, stop):
        assert quoter.session is not None and quoter.session.seconds == 7200
        handlers[signal.SIGINT](signal.SIGINT, None)
        assert not quoter.entry_allowed(), "signal must block entries before next loop"
        assert stop["count"] == 1
        return 0

    monkeypatch.setattr(live, "_run_supervised", session_runner)
    assert live.run_live(_settings(), seconds=7200, supervised_no_deadman=True) == 0
    assert handlers == old_handlers
    assert q.shutting_down
