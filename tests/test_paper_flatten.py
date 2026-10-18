"""Paper 模拟交易所与紧急平仓命令；只用模拟对象，不联网。"""

from __future__ import annotations

from typing import Any

import pytest

from entropy_bot.errors import LiveGuardError
from entropy_bot.live import LiveQuoter
from entropy_bot.paper import PaperExchange, PaperSigner
from test_live_throttle import Clock, FakeClient, FakeSigner, _book_data, _settings

SNDK = "io:SNDK"
ANTH = "io:ANTH"


def book(bid: str, ask: str, bid_sz: str = "1", ask_sz: str = "1") -> dict[str, Any]:
    return {"coin": SNDK, "levels": [[{"px": bid, "sz": bid_sz, "n": 1}], [{"px": ask, "sz": ask_sz, "n": 1}]]}


def _paper(markets) -> PaperExchange:
    ex = PaperExchange({SNDK: markets[SNDK]}, None, _settings(coins=(SNDK,)))
    ex.on_book(SNDK, book("1293.8", "1293.9"))
    return ex


def _order(markets, is_buy: bool, px: float, sz: float, *, ro=False, tif="Alo"):
    signer = PaperSigner()
    return signer.signed_orders([(markets[SNDK], is_buy, px, sz, signer.next_cloid(SNDK, "B"), ro, tif)])


def statuses(resp):
    return resp["response"]["data"]["statuses"]


def test_paper_signer_never_signs():
    payload = PaperSigner().signed_cancel_oids([(200002, 1)])
    assert payload["paper"] is True and "signature" not in payload


def test_alo_crossing_rejected_and_trade_through_fills_partially(markets):
    ex = _paper(markets)
    assert "error" in statuses(ex.post_exchange(_order(markets, True, 1293.9, 0.04)))[0]
    oid = statuses(ex.post_exchange(_order(markets, True, 1293.8, 0.04)))[0]["resting"]["oid"]
    ex.on_book(SNDK, book("1293.6", "1293.8", ask_sz="0.01"))
    assert ex.pos[SNDK] == pytest.approx(0.01)
    assert ex.orders[oid]["sz"] == pytest.approx(0.03)


def test_min_notional_and_reduce_only_exact_close(markets):
    ex = _paper(markets)
    assert "minimum value" in statuses(ex.post_exchange(_order(markets, True, 1293.8, 0.0077)))[0]["error"]
    ex.pos[SNDK], ex.entry[SNDK] = 0.005, 1294.0  # 约 $6.5，小于 $10
    resp = ex.post_exchange(_order(markets, False, 1293.6, 0.005, ro=True, tif="Ioc"))
    assert "filled" in statuses(resp)[0], "刚好平掉整仓的 reduce-only 不受 $10 限制"
    assert ex.pos[SNDK] == 0
    assert ex.realized[SNDK] == pytest.approx((1293.8 - 1294.0) * 0.005)
    again = ex.post_exchange(_order(markets, False, 1293.6, 0.005, ro=True, tif="Ioc"))
    assert "Reduce only" in statuses(again)[0]["error"]


def test_cancel_statuses_match_exchange_wording(markets):
    from entropy_bot.safety import cancel_confirmed

    ex = _paper(markets)
    oid = statuses(ex.post_exchange(_order(markets, True, 1293.8, 0.04)))[0]["resting"]["oid"]
    signer = PaperSigner()
    resp = ex.post_exchange(signer.signed_cancel_oids([(200002, oid), (200002, 12345)]))
    assert cancel_confirmed(resp, 2)


def test_live_quoter_runs_against_paper_exchange(markets, monkeypatch):
    clock = Clock()
    monkeypatch.setattr("entropy_bot.live.time.time", clock.time)
    ex = PaperExchange({SNDK: markets[SNDK]}, None, _settings(coins=(SNDK,)))
    q = LiveQuoter(_settings(coins=(SNDK,)), {SNDK: markets[SNDK]}, ex, PaperSigner())  # type: ignore[arg-type]
    ex.listener = q
    data = dict(book("1293.8", "1293.9"), time=int(clock.t * 1000))
    ex.on_book(SNDK, data)
    q.on_book(SNDK, data)
    q.preflight()
    q.deadman_until = clock.t + 10_000
    q.step(None)
    assert len(ex.orders) == 2
    traded = dict(book("1293.7", "1293.8", ask_sz="5"), time=int(clock.t * 1000))
    ex.on_book(SNDK, traded)  # 卖一跌到我们的买价：买单成交
    assert ex.pos[SNDK] > 0
    clock.advance(6)
    traded["time"] = int(clock.t * 1000)
    q.on_book(SNDK, traded)
    q.step(None)
    resting = list(ex.orders.values())
    assert len(resting) == 1 and resting[0]["ro"] and not resting[0]["is_buy"]


class FlattenClient(FakeClient):
    def __init__(self, szi: str) -> None:
        super().__init__()
        self.state = {"assetPositions": [{"position": {"coin": ANTH, "szi": szi}}]}

    def post_exchange(self, payload: dict[str, Any]) -> Any:
        resp = super().post_exchange(payload)
        if payload["action"]["type"] == "order":
            assert all(spec[-2] is True and spec[-1] == "Ioc" for spec in payload["action"]["orders"])
            self.state = {"assetPositions": []}
        return resp

    def close(self) -> None:
        pass


def _patch_flatten(monkeypatch, client, markets, tmp_path):
    import entropy_bot.flatten as mod

    seen: dict[str, Any] = {}

    def fake_markets(_client, coins):
        seen["coins"] = coins
        return {}, {c: markets[c] for c in coins}, []

    monkeypatch.setenv("ENTROPY_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(mod, "InfoClient", lambda *a, **k: client)
    monkeypatch.setattr(mod, "LiveSigner", lambda *a, **k: FakeSigner())
    monkeypatch.setattr(mod, "load_io_markets", fake_markets)
    monkeypatch.setattr(mod, "check_signer_role", lambda *a: None)
    ticks = iter(range(0, 1_000_000))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.time, "monotonic", lambda: float(next(ticks)))  # 每次调用 +1s
    return mod, seen


def test_flatten_closes_positions_on_every_coin(markets, monkeypatch, tmp_path):
    from entropy_bot.coins import ALLOWED_COINS

    client = FlattenClient("0.02")
    mod, seen = _patch_flatten(monkeypatch, client, markets, tmp_path)
    assert mod.run_flatten(_settings(coins=(SNDK,)), assume_yes=True) == 0
    assert seen["coins"] == ALLOWED_COINS, "COINS 只配了 SNDK，ANTH 的仓位也要平"
    orders = [p for p in client.posts if p["action"]["type"] == "order"]
    assert orders and orders[0]["action"]["orders"][0][3] == pytest.approx(0.02)


def test_flatten_leaves_foreign_orders_alone(markets, monkeypatch, tmp_path):
    client = FlattenClient("0")
    client.state = {"assetPositions": []}
    client.opens = [{"coin": SNDK, "oid": 7, "side": "B", "cloid": None}]
    mod, _ = _patch_flatten(monkeypatch, client, markets, tmp_path)
    assert mod.run_flatten(_settings(coins=(SNDK,)), assume_yes=True) == 0
    assert not client.posts


def test_flatten_needs_confirmation(markets, monkeypatch, tmp_path):
    client = FlattenClient("0.02")
    mod, _ = _patch_flatten(monkeypatch, client, markets, tmp_path)
    monkeypatch.setattr(mod.sys.stdin, "isatty", lambda: False)
    assert mod.run_flatten(_settings(coins=(SNDK,))) == 2
    assert not client.posts


def test_flatten_refuses_while_a_session_holds_the_lock(markets, monkeypatch, tmp_path):
    import fcntl

    client = FlattenClient("0.02")
    mod, _ = _patch_flatten(monkeypatch, client, markets, tmp_path)
    held = open(tmp_path / "live.lock", "w")
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(LiveGuardError):
            mod.run_flatten(_settings(coins=(SNDK,)), assume_yes=True)
    finally:
        held.close()
    assert not client.posts


def test_flatten_reports_failure_when_position_stays(markets, monkeypatch, tmp_path):
    class Stuck(FlattenClient):
        def post_exchange(self, payload: dict[str, Any]) -> Any:
            return FakeClient.post_exchange(self, payload)  # 永远不成交

    client = Stuck("0.02")
    mod, _ = _patch_flatten(monkeypatch, client, markets, tmp_path)
    assert mod.run_flatten(_settings(coins=(SNDK,)), assume_yes=True) == 3
