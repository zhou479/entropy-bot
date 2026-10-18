"""Config defaults, URL allowlist, NYSE calendar, nonces, diagnostics, paper exchange."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from entropy_bot.config import Settings, load_settings
from entropy_bot.diagnostics import MAX_ROWS, FillDiagnostics, utc_day
from entropy_bot.errors import ConfigError
from entropy_bot.orders import NonceClock
from entropy_bot.paper import PaperExchange, PaperSigner
from entropy_bot.session import nyse_session

NY = ZoneInfo("America/New_York")
SNDK = "io:SNDK"


def _clear_env(monkeypatch) -> None:
    for name in (
        "COINS",
        "RTH_ONLY_COINS",
        "HYPERLIQUID_API_URL",
        "HYPERLIQUID_WS_URL",
        "DEADMAN_AHEAD_S",
        "MAX_POSITION_USD",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("entropy_bot.config.load_dotenv", lambda *a, **k: False)


def test_defaults_trade_sndk_only_in_rth(monkeypatch):
    _clear_env(monkeypatch)
    s = load_settings()
    assert s.coins == (SNDK,)
    assert s.rth_only_coins == (SNDK,)
    assert s.deadman_ahead_s == 60.0


def test_rth_only_coins_can_be_disabled(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("RTH_ONLY_COINS", "")
    assert load_settings().rth_only_coins == ()


@pytest.mark.parametrize(
    "api,ws",
    [
        ("https://evil.example/hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws"),
        ("https://api.hyperliquid.xyz.evil.example", "wss://api.hyperliquid.xyz/ws"),
        ("http://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws"),
        ("https://api.hyperliquid.xyz", "wss://evil.example/?h=hyperliquid.xyz"),
    ],
)
def test_url_allowlist_checks_hostname(monkeypatch, api, ws):
    _clear_env(monkeypatch)
    monkeypatch.setenv("HYPERLIQUID_API_URL", api)
    monkeypatch.setenv("HYPERLIQUID_WS_URL", ws)
    with pytest.raises(ConfigError):
        load_settings()


def test_deadman_horizon_floor(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("DEADMAN_AHEAD_S", "5")
    with pytest.raises(ConfigError):
        load_settings()


def test_nyse_calendar_holidays_and_early_close():
    assert nyse_session(datetime(2026, 10, 19, 10, 0, tzinfo=NY)) == "rth"  # Monday
    assert nyse_session(datetime(2026, 10, 17, 10, 0, tzinfo=NY)) == "ah"  # Saturday
    assert nyse_session(datetime(2026, 11, 26, 10, 0, tzinfo=NY)) == "ah"  # Thanksgiving
    assert nyse_session(datetime(2026, 11, 27, 12, 59, tzinfo=NY)) == "rth"
    assert nyse_session(datetime(2026, 11, 27, 13, 0, tzinfo=NY)) == "ah"  # early close
    assert nyse_session(datetime(2026, 12, 25, 11, 0, tzinfo=NY)) == "ah"


def test_nonce_strictly_increases_within_one_ms(monkeypatch):
    clock = NonceClock()
    monkeypatch.setattr("entropy_bot.orders.time.time", lambda: 1_700_000_000.0)
    first = clock.next()
    assert [clock.next() for _ in range(3)] == [first + 1, first + 2, first + 3]


def test_diag_counts_maker_orders_once_and_tracks_pnl():
    diag = FillDiagnostics((SNDK,))
    diag.ingest_fills([], lambda _c: None, prime=True)
    when = datetime(2026, 10, 19, 10, 0, tzinfo=NY)
    rows = [
        {"coin": SNDK, "side": "B", "px": "1293.8", "sz": "0.02", "oid": 7, "tid": 1, "fee": "0", "closedPnl": "0"},
        {"coin": SNDK, "side": "B", "px": "1293.8", "sz": "0.02", "oid": 7, "tid": 2, "fee": "0", "closedPnl": "0"},
        {
            "coin": SNDK,
            "side": "A",
            "px": "1293.5",
            "sz": "0.04",
            "oid": 8,
            "tid": 3,
            "crossed": True,
            "fee": "0.01",
            "closedPnl": "-0.012",
        },
    ]
    diag.ingest_fills(rows, lambda _c: None, when=when)
    assert diag.fills[(SNDK, "rth")] == 1  # one maker order, two partial fills
    assert diag.taker_fills[SNDK] == 1
    assert diag.day_pnl[utc_day(when)] == pytest.approx(-0.022)


def test_diag_rows_are_bounded():
    diag = FillDiagnostics((SNDK,))
    for _ in range(MAX_ROWS + 50):
        diag.note_quotes(SNDK, 1)
    assert len(diag.rows) == MAX_ROWS


def _paper(markets) -> PaperExchange:
    settings = Settings(
        live=False,
        private_key=None,
        account=None,
        coins=(SNDK,),
        quote_notional_usd=50.0,
        quote_offset_ticks=2,
        max_leverage=2,
        api_url="https://api.hyperliquid.xyz",
        ws_url="wss://api.hyperliquid.xyz/ws",
    )
    ex = PaperExchange({SNDK: markets[SNDK]}, info=None, settings=settings)  # type: ignore[arg-type]
    ex.on_book(SNDK, {"levels": [[{"px": "1293.8", "sz": "1"}], [{"px": "1293.9", "sz": "1"}]]})
    return ex


def _order(signer: PaperSigner, markets, is_buy: bool, px: float, sz: float, *, ro=False, tif="Alo"):
    return signer.signed_orders([(markets[SNDK], is_buy, px, sz, signer.next_cloid(SNDK, "B"), ro, tif)])


def _statuses(resp):
    return resp["response"]["data"]["statuses"]


def test_paper_alo_rejects_cross_and_fills_on_trade_through(markets):
    ex = _paper(markets)
    signer = PaperSigner()
    assert "error" in _statuses(ex.post_exchange(_order(signer, markets, True, 1293.9, 0.04)))[0]
    oid = _statuses(ex.post_exchange(_order(signer, markets, True, 1293.8, 0.04)))[0]["resting"]["oid"]
    ex.on_book(SNDK, {"levels": [[{"px": "1293.6", "sz": "1"}], [{"px": "1293.8", "sz": "0.01"}]]})
    assert ex.pos[SNDK] == pytest.approx(0.01)
    assert ex.orders[oid]["sz"] == pytest.approx(0.03)


def test_paper_reduce_only_ioc_clamped_and_realizes_pnl(markets):
    ex = _paper(markets)
    signer = PaperSigner()
    ex.pos[SNDK] = 0.02
    ex.entry[SNDK] = 1294.0
    resp = ex.post_exchange(_order(signer, markets, False, 1293.6, 0.05, ro=True, tif="Ioc"))
    assert "filled" in _statuses(resp)[0]
    assert ex.pos[SNDK] == 0.0
    assert ex.realized[SNDK] == pytest.approx((1293.8 - 1294.0) * 0.02)
    assert ex.fills[-1]["crossed"] is True
    again = ex.post_exchange(_order(signer, markets, False, 1293.6, 0.05, ro=True, tif="Ioc"))
    assert "error" in _statuses(again)[0]


def test_paper_signer_never_signs():
    payload = PaperSigner().signed_schedule_cancel(None)
    assert "signature" not in payload and payload["paper"] is True
