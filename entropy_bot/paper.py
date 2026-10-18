"""Paper mode: the real `LiveQuoter` against an in-process simulated exchange.

Same quoting, per-side updates, flatten ladder, throttles, risk gates and
dead-man as `live` — only `/exchange` is replaced. Nothing is signed or sent.

Fill model (deliberately simple; treat results as optimistic for queue
position, conservative for adverse selection):

- ALO that would cross the live BBO is rejected (like "Post only order would
  have immediately matched").
- A resting buy fills when the live best ask trades down to its price (sell:
  best bid up to its price), up to the touch size, at the order's price.
- IOC fills against the live touch up to the touch size, or misses.
- Reduce-only is clamped to the open position.
- Orders under $10 are rejected unless they are a reduce-only exact close
  (same rule as the exchange).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from entropy_bot.coins import ALLOWED_DEX, Market, perp_dex_info
from entropy_bot.config import Settings
from entropy_bot.fees import estimate_from_settings
from entropy_bot.live import LiveQuoter, install_stop_handlers, log_startup, run_quoter, stop_cleanly
from entropy_bot.orders import LiveSigner, next_nonce
from entropy_bot.precision import MIN_NOTIONAL_USD
from entropy_bot.quoting import BookTop, book_top
from entropy_bot.rest import InfoClient
from entropy_bot.status import load_io_markets
from entropy_bot.ws import BookFeed

log = logging.getLogger("entropy_bot.paper")

PAPER_ACCOUNT = "0x" + "00" * 19 + "01"
EPS = 1e-12


class PaperSigner(LiveSigner):
    """Builds the exact live action dicts but never signs them."""

    def __init__(self, account: str = PAPER_ACCOUNT) -> None:  # noqa: D107 - no key on purpose
        self.wallet = None  # type: ignore[assignment]
        self.signer_address = account
        self.account = account
        self.vault_address = None
        self.is_agent = False
        self._seq = int(time.time() * 1000) & 0xFFFFFFFFFFFFFF

    def wrap(self, action: dict[str, Any]) -> dict[str, Any]:
        return {"action": action, "nonce": next_nonce(), "paper": True}


class PaperExchange:
    """Implements the InfoClient surface LiveQuoter uses, plus `/exchange`."""

    def __init__(self, markets: dict[str, Market], info: InfoClient, settings: Settings) -> None:
        self.markets = markets
        self.by_asset = {m.asset_id: coin for coin, m in markets.items()}
        self.info = info
        self.listener: LiveQuoter | None = None
        self.tops: dict[str, BookTop] = {}
        self.orders: dict[int, dict[str, Any]] = {}
        self.pos: dict[str, float] = {coin: 0.0 for coin in markets}
        self.entry: dict[str, float] = {coin: 0.0 for coin in markets}
        self.realized: dict[str, float] = {coin: 0.0 for coin in markets}
        self.fees_paid = 0.0
        self.fills: list[dict[str, Any]] = []
        self.actions: dict[str, int] = {}
        self.deadman_ms: int | None = None
        self.deadman_fired = 0
        self._taker_rate = {
            coin: estimate_from_settings(m, settings).gross_taker_pct / 100.0 for coin, m in markets.items()
        }
        self._oid = 9_000_000_000_000
        self._tid = 0
        self._lock = threading.RLock()

    # ----------------------------------------------------------- info surface

    def clearinghouse_state(self, _user: str, _dex: str = ALLOWED_DEX, **_kw: Any) -> dict[str, Any]:
        with self._lock:
            rows = [
                {"position": {"coin": coin, "szi": str(szi), "entryPx": str(self.entry[coin])}}
                for coin, szi in self.pos.items()
                if abs(szi) > EPS
            ]
        return {"assetPositions": rows, "withdrawable": "paper"}

    def open_orders(self, _user: str, _dex: str = ALLOWED_DEX, **_kw: Any) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {
                    "coin": o["coin"],
                    "side": "B" if o["is_buy"] else "A",
                    "limitPx": str(o["px"]),
                    "sz": str(o["sz"]),
                    "oid": oid,
                    "cloid": o["cloid"],
                    "reduceOnly": o["ro"],
                    "tif": o["tif"],
                }
                for oid, o in self.orders.items()
            ]

    def user_fills(self, _user: str, **_kw: Any) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.fills)

    def user_fills_by_time(self, _user: str, start_ms: int, **_kw: Any) -> list[dict[str, Any]]:
        with self._lock:
            return [f for f in self.fills if f["time"] >= start_ms]

    def user_rate_limit(self, _user: str, **_kw: Any) -> None:
        return None

    def extra_agents(self, _user: str, **_kw: Any) -> list[Any]:
        return []

    def l2_book(self, coin: str) -> dict[str, Any]:
        return self.info.l2_book(coin)

    # ------------------------------------------------------- exchange surface

    def post_exchange(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = payload.get("action") or {}
        kind = str(action.get("type"))
        events: list[tuple[str, Any]] = []
        with self._lock:
            self.actions[kind] = self.actions.get(kind, 0) + 1
            if kind == "order":
                statuses = [self._order(w, events) for w in action.get("orders") or []]
            elif kind == "cancel":
                statuses = [self._cancel_oid(int(c["o"]), events) for c in action.get("cancels") or []]
            elif kind == "cancelByCloid":
                statuses = [self._cancel_cloid(str(c["cloid"]), events) for c in action.get("cancels") or []]
            elif kind == "scheduleCancel":
                self.deadman_ms = action.get("time")
                statuses = []
            else:
                statuses = []
        self._emit(events)
        if kind in {"order", "cancel", "cancelByCloid"}:
            return {"status": "ok", "response": {"type": kind, "data": {"statuses": statuses}}}
        return {"status": "ok", "response": {"type": "default"}}

    def _new_oid(self) -> int:
        self._oid += 1
        return self._oid

    def _order(self, wire: dict[str, Any], events: list[tuple[str, Any]]) -> Any:
        coin = self.by_asset.get(int(wire["a"]))
        top = self.tops.get(coin or "")
        if coin is None or top is None or top.bid is None or top.ask is None:
            return {"error": "paper: no book"}
        is_buy = bool(wire["b"])
        px = float(wire["p"])
        sz = float(wire["s"])
        ro = bool(wire.get("r"))
        tif = str(((wire.get("t") or {}).get("limit") or {}).get("tif") or "Alo")
        exact_close = False
        if ro:
            room = max(0.0, -self.pos[coin]) if is_buy else max(0.0, self.pos[coin])
            if room <= EPS:
                return {"error": "Reduce only order would increase position."}
            exact_close = sz >= room - EPS
            sz = min(sz, room)
        if px * sz < MIN_NOTIONAL_USD - 1e-9 and not exact_close:
            return {"error": "Order must have minimum value of $10."}
        crosses = px >= top.ask if is_buy else px <= top.bid
        oid = self._new_oid()
        if tif == "Alo":
            if crosses:
                return {"error": f"Post only order would have immediately matched, bbo was {top.bid}@{top.ask}"}
            self.orders[oid] = {
                "coin": coin,
                "is_buy": is_buy,
                "px": px,
                "sz": sz,
                "ro": ro,
                "tif": tif,
                "cloid": wire.get("c"),
            }
            return {"resting": {"oid": oid}}
        if not crosses:
            return {"error": "Order could not immediately match against any resting orders."}
        touch_px = top.ask if is_buy else top.bid
        touch_sz = (top.ask_sz if is_buy else top.bid_sz) or sz
        fill_sz = min(sz, touch_sz)
        self._fill(coin, oid, is_buy, touch_px, fill_sz, crossed=True, events=events)
        return {"filled": {"totalSz": str(fill_sz), "avgPx": str(touch_px), "oid": oid}}

    def _cancel_oid(self, oid: int, events: list[tuple[str, Any]]) -> Any:
        order = self.orders.pop(oid, None)
        if order is None:
            return {"error": "Order was never placed, already canceled, or filled."}
        events.append(("orders", [self._update_row(oid, order, "canceled")]))
        return "success"

    def _cancel_cloid(self, cloid: str, events: list[tuple[str, Any]]) -> Any:
        for oid, order in list(self.orders.items()):
            if order["cloid"] == cloid:
                return self._cancel_oid(oid, events)
        return {"error": "Order was never placed, already canceled, or filled."}

    def _update_row(self, oid: int, order: dict[str, Any], status: str) -> dict[str, Any]:
        return {
            "order": {
                "coin": order["coin"],
                "side": "B" if order["is_buy"] else "A",
                "limitPx": str(order["px"]),
                "sz": str(order["sz"]),
                "oid": oid,
                "cloid": order["cloid"],
            },
            "status": status,
            "statusTimestamp": int(time.time() * 1000),
        }

    def _fill(
        self,
        coin: str,
        oid: int,
        is_buy: bool,
        px: float,
        sz: float,
        *,
        crossed: bool,
        events: list[tuple[str, Any]],
    ) -> None:
        signed = sz if is_buy else -sz
        before = self.pos[coin]
        closed = 0.0
        if before and (before > 0) != (signed > 0):
            reduce = min(abs(before), abs(signed))
            closed = (px - self.entry[coin]) * reduce * (1 if before > 0 else -1)
        after = before + signed
        if abs(after) <= EPS:
            after = 0.0
            self.entry[coin] = 0.0
        elif before == 0 or (before > 0) != (after > 0):
            self.entry[coin] = px
        elif (before > 0) == (signed > 0):
            self.entry[coin] = (self.entry[coin] * abs(before) + px * sz) / abs(after)
        self.pos[coin] = after
        fee = px * sz * self._taker_rate[coin] if crossed else 0.0
        self.realized[coin] += closed
        self.fees_paid += fee
        self._tid += 1
        row = {
            "coin": coin,
            "px": str(px),
            "sz": str(sz),
            "side": "B" if is_buy else "A",
            "time": int(time.time() * 1000),
            "startPosition": str(before),
            "closedPnl": str(closed),
            "oid": oid,
            "crossed": crossed,
            "fee": str(fee),
            "tid": self._tid,
        }
        self.fills.append(row)
        events.append(("fills", [row]))
        log.info(
            "PAPER FILL %s %s sz=%s px=%s %s pos=%s closedPnl=%.4f",
            coin,
            "BUY" if is_buy else "SELL",
            sz,
            px,
            "taker" if crossed else "maker",
            after,
            closed,
        )

    def on_book(self, coin: str, data: dict[str, Any]) -> None:
        events: list[tuple[str, Any]] = []
        with self._lock:
            top = book_top(coin, data)
            self.tops[coin] = top
            self._check_deadman(events)
            for oid, order in list(self.orders.items()):
                if order["coin"] != coin:
                    continue
                if order["is_buy"] and top.ask is not None and top.ask <= order["px"]:
                    touch = top.ask_sz or order["sz"]
                elif not order["is_buy"] and top.bid is not None and top.bid >= order["px"]:
                    touch = top.bid_sz or order["sz"]
                else:
                    continue
                fill_sz = min(order["sz"], touch)
                if order["ro"]:
                    room = max(0.0, -self.pos[coin]) if order["is_buy"] else max(0.0, self.pos[coin])
                    fill_sz = min(fill_sz, room)
                if fill_sz <= EPS:
                    self.orders.pop(oid)
                    events.append(("orders", [self._update_row(oid, order, "reduceOnlyCanceled")]))
                    continue
                self._fill(coin, oid, order["is_buy"], order["px"], fill_sz, crossed=False, events=events)
                order["sz"] -= fill_sz
                if order["sz"] <= EPS:
                    self.orders.pop(oid)
                    events.append(("orders", [self._update_row(oid, order, "filled")]))
                else:
                    events.append(("orders", [self._update_row(oid, order, "open")]))
        self._emit(events)

    def _check_deadman(self, events: list[tuple[str, Any]]) -> None:
        if self.deadman_ms is None or time.time() * 1000 < self.deadman_ms:
            return
        log.warning("PAPER dead-man fired: cancel all %s open orders", len(self.orders))
        self.deadman_fired += 1
        self.deadman_ms = None
        for oid, order in list(self.orders.items()):
            self.orders.pop(oid)
            events.append(("orders", [self._update_row(oid, order, "canceled")]))

    def _emit(self, events: list[tuple[str, Any]]) -> None:
        listener = self.listener
        if listener is None:
            return
        for kind, rows in events:
            if kind == "fills":
                listener.on_user_fills({"fills": rows, "isSnapshot": False})
            else:
                listener.on_order_updates(rows)

    def summary(self) -> str:
        with self._lock:
            maker = sum(1 for f in self.fills if not f["crossed"])
            taker = len(self.fills) - maker
            volume = sum(float(f["px"]) * float(f["sz"]) for f in self.fills)
            writes = sum(self.actions.values())
            return (
                f"paper fills={len(self.fills)} (maker {maker} / taker {taker}) volume=${volume:.2f} "
                f"positions={self.pos} realized={ {c: round(v, 4) for c, v in self.realized.items()} } "
                f"taker fees=${self.fees_paid:.4f} actions={self.actions} "
                f"(= {writes} signed actions if live) dead-man fired={self.deadman_fired}"
            )


def run_paper(settings: Settings, *, seconds: float | None = None) -> int:
    client = InfoClient(settings.api_url, pause_s=0.0)
    feed: BookFeed | None = None
    quoter: LiveQuoter | None = None
    paper: PaperExchange | None = None
    stop: dict[str, Any] | None = None
    rc = 1
    try:
        _meta, markets, _ctxs = load_io_markets(client, settings.coins)
        dex = perp_dex_info(client.perp_dexs(), ALLOWED_DEX)
        log.info(
            "PAPER (no signing, simulated /exchange)  dex=%s/%s  coins=%s  notional=$%s",
            dex.get("name"),
            dex.get("fullName"),
            ",".join(settings.coins),
            settings.quote_notional_usd,
        )
        log_startup(settings, markets)
        paper = PaperExchange(markets, client, settings)
        quoter = LiveQuoter(settings, markets, paper, PaperSigner())  # type: ignore[arg-type]
        paper.listener = quoter
        stop = install_stop_handlers()
        quoter.bootstrap_isolated()

        def on_book(coin: str, data: dict[str, Any]) -> None:
            paper.on_book(coin, data)
            quoter.on_book(coin, data)

        feed = BookFeed(settings.ws_url, settings.coins, on_book, on_asset_ctx=quoter.on_asset_ctx)
        quoter._feed = feed
        log.info("paper quoting on one WS connection; Ctrl-C to stop")
        rc = run_quoter(quoter, feed, seconds=seconds, stop=stop)
    finally:
        if quoter is not None:
            stop_rc = stop_cleanly(quoter, feed, settings, stop)
            rc = rc or stop_rc
        elif feed is not None:
            feed.stop()
        if paper is not None:
            log.info("%s", paper.summary())
        client.close()
    return rc
