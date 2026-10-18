"""Stop = flatten + verify; reject backoff; startup guards; $10 minimum."""

from __future__ import annotations

from typing import Any

import pytest

from entropy_bot.errors import LiveGuardError
from entropy_bot.live import (
    LiveQuoter,
    acquire_instance_lock,
    check_clock,
    check_signer_role,
    stop_cleanly,
)
from entropy_bot.precision import order_size
from tests.test_live_throttle import Clock, FakeClient, FakeSigner, _book_data, _settings, _top

SNDK = "io:SNDK"


class PositionClient(FakeClient):
    """FakeClient with an inventory that IOC reduce-only orders actually close.

    Every clearinghouseState read advances the clock 0.5s so stop loops end.
    """

    def __init__(self, clock: Clock, szi: float, *, ioc_fills: bool = True) -> None:
        super().__init__()
        self.clock = clock
        self.szi = szi
        self.ioc_fills = ioc_fills

    def clearinghouse_state(self, *args: object, **kwargs: object) -> dict[str, Any]:
        self.clock.advance(0.5)
        rows = [{"position": {"coin": SNDK, "szi": str(self.szi)}}] if self.szi else []
        return {"assetPositions": rows}

    def l2_book(self, coin: str) -> dict[str, Any]:
        return _book_data("1293.8", "1293.9")

    def post_exchange(self, payload: dict[str, Any]) -> Any:
        action = payload.get("action") or {}
        if action.get("type") == "order":
            self.posts.append(payload)
            statuses = []
            for spec in action["orders"]:
                _m, is_buy, _px, sz, _cloid, reduce_only, tif = spec
                if tif == "Ioc" and reduce_only and self.ioc_fills:
                    self.szi += sz if is_buy else -sz
                    statuses.append({"filled": {"totalSz": str(sz), "oid": 1}})
                elif tif == "Ioc":
                    statuses.append({"error": "Order could not immediately match against any resting orders."})
                else:
                    self._oid += 1
                    statuses.append({"resting": {"oid": self._oid}})
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": statuses}}}
        return super().post_exchange(payload)


@pytest.fixture
def clock(monkeypatch) -> Clock:
    c = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", c.time)
    monkeypatch.setattr(LiveQuoter, "_pause", lambda self, seconds: None)
    return c


def _quoter(markets, client, **kw) -> LiveQuoter:
    kw.setdefault("rth_only_coins", ())
    q = LiveQuoter(_settings(coins=(SNDK,), **kw), {SNDK: markets[SNDK]}, client, FakeSigner())
    q.deadman_until = client.clock.t + 1_000.0
    return q


def _orders(client: FakeClient) -> list[tuple]:
    return [spec for p in client.posts if p["action"]["type"] == "order" for spec in p["action"]["orders"]]


def test_stop_with_open_position_flattens_then_reports_clean(markets, clock):
    client = PositionClient(clock, 0.0386)
    q = _quoter(markets, client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))  # rests a reduce-only sell
    rc = stop_cleanly(q, None, q.settings, {"flag": True, "count": 1})
    assert rc == 0
    assert client.szi == 0
    ioc = [o for o in _orders(client) if o[-1] == "Ioc"]
    assert ioc and ioc[0][3] == pytest.approx(0.0386)  # whole position, not one notional slice
    assert ioc[0][-2] is True
    assert not q._has_rest(SNDK)
    assert client.posts[-1]["action"] == {"type": "scheduleCancel"}  # dead-man cleared last


def test_stop_flatten_closes_more_than_one_notional_in_one_order(markets, clock):
    client = PositionClient(clock, -0.2)  # ~$259, five times the $50 quote size
    q = _quoter(markets, client)
    assert stop_cleanly(q, None, q.settings, None) == 0
    ioc = [o for o in _orders(client) if o[-1] == "Ioc"]
    assert len(ioc) == 1 and ioc[0][1] is True and ioc[0][3] == pytest.approx(0.2)


def test_stop_reports_not_flat_when_ioc_keeps_missing(markets, clock):
    client = PositionClient(clock, 0.0386, ioc_fills=False)
    q = _quoter(markets, client, stop_flatten_s=5.0)
    assert stop_cleanly(q, None, q.settings, None) == 3
    assert client.szi == pytest.approx(0.0386)
    assert len([o for o in _orders(client) if o[-1] == "Ioc"]) >= 2


def test_stop_flatten_disabled_still_reports_open_position(markets, clock):
    client = PositionClient(clock, 0.0386)
    q = _quoter(markets, client, stop_flatten_s=0.0)
    assert stop_cleanly(q, None, q.settings, None) == 3
    assert not _orders(client)
    assert client.szi == pytest.approx(0.0386)


def test_second_stop_signal_skips_flatten(markets, clock):
    client = PositionClient(clock, 0.0386)
    q = _quoter(markets, client)
    assert stop_cleanly(q, None, q.settings, {"flag": True, "count": 2}) == 3
    assert not _orders(client)


def test_stop_when_flat_cancels_and_is_clean(markets, clock):
    client = PositionClient(clock, 0.0)
    q = _quoter(markets, client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert q._has_rest(SNDK)
    assert stop_cleanly(q, None, q.settings, None) == 0
    assert not q._has_rest(SNDK)
    assert not [o for o in _orders(client) if o[-1] == "Ioc"]


def test_stop_unverified_position_is_not_clean(markets, clock):
    class Blind(PositionClient):
        def clearinghouse_state(self, *args: object, **kwargs: object) -> None:
            self.clock.advance(0.5)
            return None

    client = Blind(clock, 0.0)
    q = _quoter(markets, client, stop_flatten_s=3.0)
    assert stop_cleanly(q, None, q.settings, None) == 3


class RejectClient(FakeClient):
    def __init__(self, clock: Clock, _szi: float) -> None:
        super().__init__()
        self.clock = clock

    def post_exchange(self, payload: dict[str, Any]) -> Any:
        action = payload.get("action") or {}
        if action.get("type") == "order":
            self.posts.append(payload)
            err = {"error": "Insufficient margin to place order."}
            statuses = [err] * len(action["orders"])
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": statuses}}}
        return super().post_exchange(payload)


def test_generic_reject_backs_off_exponentially(markets, clock):
    client = RejectClient(clock, 0.0)
    q = _quoter(markets, client)
    top = _top("1293.8", "1293.9", SNDK)
    q.requote(SNDK, top)
    assert len(_orders(client)) == 2
    for _ in range(3):  # within 2s: nothing new
        clock.advance(0.5)
        q.requote(SNDK, top)
    assert len(_orders(client)) == 2
    clock.advance(1.0)
    q.requote(SNDK, top)
    assert len(_orders(client)) == 4  # retried after 2s
    clock.advance(2.5)
    q.requote(SNDK, top)
    assert len(_orders(client)) == 4  # second reject: now waits 4s
    clock.advance(2.0)
    q.requote(SNDK, top)
    assert len(_orders(client)) == 6


def test_min_notional_sizes_round_up_to_ten_dollars():
    assert order_size(10, 1293.8, 4) * 1293.8 >= 10
    assert order_size(10, 1293.8, 4) == pytest.approx(0.0078)
    assert order_size(50, 1293.8, 4) == pytest.approx(0.0386)  # normal sizes still floor


class RoleClient:
    def __init__(self, data: Any) -> None:
        self.data = data

    def post_info(self, payload: dict[str, Any], **_kw: Any) -> Any:
        assert payload["type"] == "userRole"
        return self.data


class _Signer:
    def __init__(self, account: str, signer: str) -> None:
        self.account = account
        self.signer_address = signer
        self.is_agent = account.lower() != signer.lower()


MASTER = "0x" + "11" * 20
AGENT = "0x" + "ab" * 20


def test_signer_role_checks():
    check_signer_role(RoleClient({"role": "agent", "data": {"user": MASTER}}), _Signer(MASTER, AGENT))
    check_signer_role(RoleClient({"role": "user"}), _Signer(MASTER, MASTER))
    with pytest.raises(LiveGuardError):  # HYPERLIQUID_ACCOUNT missing: would read the agent's empty account
        check_signer_role(RoleClient({"role": "agent", "data": {"user": MASTER}}), _Signer(AGENT, AGENT))
    with pytest.raises(LiveGuardError):  # key is not an agent of the configured account
        check_signer_role(RoleClient({"role": "user"}), _Signer(MASTER, AGENT))
    check_signer_role(RoleClient(None), _Signer(MASTER, AGENT))  # unknown: warn only


def test_clock_skew_refused(monkeypatch):
    class Book:
        def l2_book(self, coin: str) -> dict[str, Any]:
            return {"time": 1_000_000_000_000}

    monkeypatch.setattr("entropy_bot.live.time.time", lambda: 1_000_000_000.5)
    check_clock(Book(), SNDK)  # 0.5s: fine
    monkeypatch.setattr("entropy_bot.live.time.time", lambda: 1_000_000_030.0)
    with pytest.raises(LiveGuardError):
        check_clock(Book(), SNDK)


def test_single_instance_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("ENTROPY_BOT_LOCK_DIR", str(tmp_path))
    first = acquire_instance_lock(MASTER)
    with pytest.raises(LiveGuardError):
        acquire_instance_lock(MASTER)
    first.close()
    acquire_instance_lock(MASTER).close()


def test_wrong_kind_rest_is_canceled_even_when_reducer_cannot_go_out(markets, clock):
    client = PositionClient(clock, 0.0)
    q = _quoter(markets, client)
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))  # flat: opening bid + opening ask
    client.posts.clear()
    # Partial bid fill → long; the reducer (A side) is in a reject cool-down.
    q.pos[SNDK] = 0.0155
    q.pos_since[SNDK] = clock.t
    q._reject_until[(SNDK, "A")] = clock.t + 30
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    assert not q.rests[SNDK]["A"].occupied(), "full-size opening ask must not stay while long"
    assert not q.rests[SNDK]["B"].occupied()
    assert not _orders(client)


def test_reducer_still_placed_during_weight_backoff(markets, clock):
    from tests.test_live_throttle import WEIGHT_ERR

    client = PositionClient(clock, 0.0)
    q = _quoter(markets, client)
    q._trip_weight_backoff(WEIGHT_ERR)
    q.pos[SNDK] = 0.0386
    q.pos_since[SNDK] = clock.t
    client.szi = 0.0386
    q.requote(SNDK, _top("1293.8", "1293.9", SNDK))
    placed = _orders(client)
    assert len(placed) == 1 and placed[0][-2] is True and placed[0][-1] == "Alo"


def test_uncertain_deadman_renew_is_not_shortened(markets, clock):
    from entropy_bot.errors import ExchangeUncertain

    client = PositionClient(clock, 0.0)
    client.exchange_error = ExchangeUncertain("read timed out")
    q = _quoter(markets, client)
    q.deadman_until = 0.0
    q.maybe_deadman()
    assert not [p for p in client.posts if p["action"].get("type") == "scheduleCancel"]
    clock.advance(10.5)
    q.maybe_deadman()
    sent = [p["action"] for p in client.posts if p["action"].get("type") == "scheduleCancel"]
    assert len(sent) == 1 and sent[0]["time"] >= int(clock.t * 1000) + 59_000


def test_flatten_cli_covers_all_coins_and_checks_cleanup(markets, clock, monkeypatch, tmp_path):
    import entropy_bot.flatten as flat_mod
    from entropy_bot.coins import ALLOWED_COINS

    class AnthClient(PositionClient):
        def clearinghouse_state(self, *args: object, **kwargs: object) -> dict[str, Any]:
            self.clock.advance(0.5)
            rows = [{"position": {"coin": "io:ANTH", "szi": str(self.szi)}}] if self.szi else []
            return {"assetPositions": rows}

        def l2_book(self, coin: str) -> dict[str, Any]:
            return _book_data("1985.0", "1985.1")

        def post_info(self, payload: dict[str, Any], **_kw: Any) -> Any:
            return {"role": "agent", "data": {"user": FakeSigner.account}}

        def close(self) -> None:
            pass

    client = AnthClient(clock, 0.02)
    seen: dict[str, Any] = {}

    def fake_markets(_client, coins):
        seen["coins"] = coins
        return {}, {c: markets[c] for c in coins}, []

    monkeypatch.setenv("ENTROPY_BOT_LOCK_DIR", str(tmp_path))
    monkeypatch.setattr(flat_mod, "InfoClient", lambda *a, **k: client)
    monkeypatch.setattr(flat_mod, "load_io_markets", fake_markets)
    monkeypatch.setattr(flat_mod, "LiveSigner", lambda *a, **k: FakeSigner())
    monkeypatch.setattr(flat_mod, "install_stop_handlers", lambda: {"flag": False, "count": 0})
    settings = _settings(coins=(SNDK,))  # COINS is SNDK only; the ANTH position must still close
    assert flat_mod.run_flatten(settings) == 0
    assert seen["coins"] == ALLOWED_COINS
    assert client.szi == 0

    class NoOpens(AnthClient):
        def open_orders(self, *args: object, **kwargs: object) -> None:
            return None

    client2 = NoOpens(clock, 0.0)
    monkeypatch.setattr(flat_mod, "InfoClient", lambda *a, **k: client2)
    assert flat_mod.run_flatten(settings) == 3  # cannot confirm bot orders are gone
