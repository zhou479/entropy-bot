"""发送前重新规划及部分拒单回归；仅使用模拟交易所，不联网。"""

from types import SimpleNamespace

import pytest

from entropy_bot.live import LiveQuoter
from entropy_bot.orders import is_alo_reject
from entropy_bot.session import SupervisedSession
from test_live_throttle import Clock, FakeClient, FakeSigner, _book_data, _order_posts, _settings, _top


COIN = "io:SNDK"
POST_ONLY_ERROR = "Post only order would have immediately matched, bbo was 1763/1763.1"


@pytest.fixture
def setup(markets, monkeypatch):
    clock = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", clock.time)
    monkeypatch.setattr("entropy_bot.session.time.monotonic", clock.time)
    client = FakeClient()
    q = LiveQuoter(_settings(coins=(COIN,), max_leverage=1),
                   {COIN: markets[COIN]}, client, FakeSigner())
    q.session = SupervisedSession(7200, started_at=clock.t)
    q.on_book(COIN, _book_data("1764", "1764.1"))
    q.preflight()
    return clock, client, q


def set_position(clock, client, q, size, age):
    client.state = {"assetPositions": [{"position": {"coin": COIN, "szi": str(size)}}]}
    q.pos[COIN] = size
    q.pos_since[COIN] = clock.t - age
    q.refresh_positions(force=True)


def response(statuses):
    return {"status": "ok", "response": {"type": "order", "data": {"statuses": statuses}}}


def change_book_during_cancel(clock, q, monkeypatch):
    original = q.cancel_coin_rests

    def cancel(**kwargs):
        result = original(**kwargs)
        clock.advance(1)
        q.on_book(COIN, _book_data("1763", "1763.1"))
        return result

    monkeypatch.setattr(q, "cancel_coin_rests", cancel)


def test_maker_uses_book_after_cancel_and_caches_actual_plan(setup, monkeypatch):
    clock, client, q = setup
    change_book_during_cancel(clock, q, monkeypatch)
    q.requote(COIN, _top("1764", "1764.1", COIN))
    specs = _order_posts(client)[0]["action"]["orders"]
    assert [s[2] for s in specs] == [1763, 1763.1]
    assert q.last_plan_key[COIN] == q.plan_for(COIN, _top("1763", "1763.1", COIN)).key()


@pytest.mark.parametrize("size,price", [(0.02, 1763), (-0.02, 1763.1)])
def test_ioc_uses_book_after_cancel(setup, monkeypatch, size, price):
    clock, client, q = setup
    set_position(clock, client, q, size, 20)
    change_book_during_cancel(clock, q, monkeypatch)
    q.requote(COIN, _top("1764", "1764.1", COIN))
    spec = _order_posts(client)[0]["action"]["orders"][0]
    assert spec[2] == price
    assert spec[-2:] == (True, "Ioc")


@pytest.mark.parametrize("size", [0.02, -0.02])
def test_crossing_take_threshold_defers_old_alo_then_takes(setup, monkeypatch, size):
    clock, client, q = setup
    set_position(clock, client, q, size, 14.5)
    change_book_during_cancel(clock, q, monkeypatch)
    q.requote(COIN, _top("1764", "1764.1", COIN))
    assert not _order_posts(client), "跨过 15 秒不能继续发旧 ALO"
    assert q.last_plan_key[COIN] == ""
    clock.advance(0.4)
    q.requote(COIN, _top("1763", "1763.1", COIN))
    assert _order_posts(client)[0]["action"]["orders"][0][-2:] == (True, "Ioc")


def test_crossing_far_to_mid_uses_new_stage(setup, monkeypatch):
    clock, client, q = setup
    set_position(clock, client, q, 0.02, 5.5)
    change_book_during_cancel(clock, q, monkeypatch)
    q.requote(COIN, _top("1764", "1764.1", COIN))
    actual = _order_posts(client)[0]["action"]["orders"][0]
    expected = q.plan_for(COIN, _top("1763", "1763.1", COIN))
    assert expected.stage == "mid"
    assert actual[2] == expected.intents[0].px
    assert q.last_plan_key[COIN] == expected.key()


def test_book_refresh_is_after_budget_query_without_extra_rest(setup, monkeypatch):
    clock, client, q = setup
    plan = q.plan_for(COIN, _top("1764", "1764.1", COIN))

    def refresh_budget():
        clock.advance(1)
        q.on_book(COIN, _book_data("1763", "1763.1"))

    monkeypatch.setattr(q, "refresh_budget", refresh_budget)
    monkeypatch.setattr(client, "l2_book", lambda *_: pytest.fail("fresh WS needs no REST"))
    q._place(COIN, plan)
    assert _order_posts(client)[0]["action"]["orders"][0][2] == 1763


def test_newer_rest_cache_is_not_overwritten_by_older_feed(setup):
    clock, _, q = setup
    q._feed = SimpleNamespace(latest=lambda _: (clock.t - 1, _book_data("1760", "1760.1")))
    assert q.book_for(COIN, q._feed).bid == 1764


@pytest.mark.parametrize("failure", ["missing", "old_server_time", "position_stales", "fill_during_sign"])
def test_invalid_presend_state_sends_nothing(setup, monkeypatch, failure):
    clock, client, q = setup
    set_position(clock, client, q, 0.02, 20)
    plan = q.plan_for(COIN, _top("1764", "1764.1", COIN))
    if failure == "missing":
        q.books.clear()
        monkeypatch.setattr(client, "l2_book", lambda *_: (_ for _ in ()).throw(RuntimeError("offline")))
    elif failure == "old_server_time":
        q.on_book(COIN, dict(_book_data("1764", "1764.1"), time=int((clock.t - 16) * 1000)))
    elif failure == "position_stales":
        q.books.clear()

        def slow_book(*_):
            clock.advance(4)
            return _book_data("1764", "1764.1")

        monkeypatch.setattr(client, "l2_book", slow_book)
    else:
        original = q.signer.signed_orders

        def fill_during_sign(specs):
            q.on_user_fills({"fills": [{}]})
            return original(specs)

        monkeypatch.setattr(q.signer, "signed_orders", fill_during_sign)
    assert q._place(COIN, plan).get("blocked")
    assert not _order_posts(client)


@pytest.mark.parametrize("error", [POST_ONLY_ERROR, "Bad Alo Px", "POST-ONLY order rejected",
                                   "post   only rejected", "would match immediately"])
def test_alo_error_spellings(error):
    assert is_alo_reject(error)


@pytest.mark.parametrize("error", ["Insufficient margin", "Invalid signature", "Too many requests", ""])
def test_non_alo_errors_not_reclassified(error):
    assert not is_alo_reject(error)


def test_partial_rejection_preserves_success_and_invalidates_plan(setup):
    clock, client, q = setup
    client.exchange_error = response([{"error": POST_ONLY_ERROR}, {"resting": {"oid": 999}}])
    q.requote(COIN, _top("1764", "1764.1", COIN))
    assert q.rests[COIN]["A"].oid == 999
    assert not q.rests[COIN]["B"].occupied()
    assert q.last_plan_key[COIN] == ""
    posts = len(client.posts)
    clock.advance(1)
    q.requote(COIN, _top("1764", "1764.1", COIN))
    assert len(client.posts) == posts, "仍有成功挂单时不能紧密重报"


def test_rejected_status_is_returned_to_caller(setup):
    _, client, q = setup
    client.exchange_error = response([{"error": POST_ONLY_ERROR}, {"resting": {"oid": 999}}])
    result = q._place(COIN, q.plan_for(COIN, _top("1764", "1764.1", COIN)))
    assert result["aloReject"] and result["rejected"]
    assert q.rests[COIN]["A"].oid == 999


def test_general_rejection_does_not_cache_successful_plan(setup):
    _, client, q = setup
    client.exchange_error = response([{"error": "Insufficient margin"}] * 2)
    q.requote(COIN, _top("1764", "1764.1", COIN))
    assert q.last_plan_key[COIN] == ""
    assert not q._has_rest(COIN)
