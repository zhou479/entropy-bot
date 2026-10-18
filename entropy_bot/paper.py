"""Paper 模式：实盘用的 `LiveQuoter` + 人工值守会话，只把 `/exchange` 换成本地模拟撮合。

报价、两侧管理、平仓阶梯、节流、风控闸门、会话收尾都和 `live --supervised-no-deadman`
走同一套代码。不签名、不发送任何交易请求；行情来自官方 WS。

撮合模型（刻意简单：对排队位置偏乐观，对逆向选择偏保守）：

- ALO 会穿过当前买一/卖一时拒单（同交易所 "Post only order would have immediately matched"）。
- 挂着的买单在卖一价跌到它的价位时按挂单价成交，数量不超过卖一挂单量；卖单同理。
- IOC 按当前买一/卖一成交，数量不超过该档挂单量；够不着就失败。
- reduce-only 不会超过当前仓位；$10 以下的单被拒，刚好平掉整仓的 reduce-only 除外。
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from typing import Any

from entropy_bot.coins import ALLOWED_DEX, Market, perp_dex_info
from entropy_bot.config import Settings
from entropy_bot.errors import LiveGuardError
from entropy_bot.fees import ALO_TIF_NOTE, describe_fee_model, estimate_from_settings
from entropy_bot.live import LiveQuoter, _run_supervised
from entropy_bot.orders import LiveSigner, next_nonce
from entropy_bot.precision import MIN_NOTIONAL_USD
from entropy_bot.quoting import BookTop, book_top
from entropy_bot.rest import InfoClient
from entropy_bot.session import MAX_SESSION_S, SupervisedSession
from entropy_bot.status import load_io_markets
from entropy_bot.ws import BookFeed

log = logging.getLogger("entropy_bot.paper")

PAPER_ACCOUNT = "0x" + "00" * 19 + "01"
PAPER_DEFAULT_S = 600.0
EPS = 1e-12


class PaperSigner(LiveSigner):
    """生成和实盘完全相同的 action 结构，但从不签名。"""

    def __init__(self, account: str = PAPER_ACCOUNT) -> None:  # 故意不需要私钥
        self.wallet = None  # type: ignore[assignment]
        self.signer_address = account
        self.account = account
        self.vault_address = None
        self.is_agent = False
        self._seq = int(time.time() * 1000) & 0xFFFFFFFFFFFFFF

    def wrap(self, action: dict[str, Any]) -> dict[str, Any]:
        return {"action": action, "nonce": next_nonce(), "paper": True}


class PaperExchange:
    """实现 LiveQuoter 用到的 InfoClient 接口，外加模拟的 `/exchange`。"""

    def __init__(self, markets: dict[str, Market], info: InfoClient | None, settings: Settings) -> None:
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
        fees = {coin: estimate_from_settings(m, settings) for coin, m in markets.items()}
        # 按成交当时扣的毛费率算（返佣事后才到账）。
        self._taker_rate = {coin: f.gross_taker_pct / 100.0 for coin, f in fees.items()}
        self._maker_rate = {coin: f.gross_maker_pct / 100.0 for coin, f in fees.items()}
        self._oid = 9_000_000_000_000
        self._tid = 0
        self._lock = threading.RLock()

    # ---------------------------------------------------------------- 查询接口

    def clearinghouse_state(self, *_a: Any, **_kw: Any) -> dict[str, Any]:
        with self._lock:
            rows = [
                {"position": {"coin": coin, "szi": repr(szi), "entryPx": str(self.entry[coin])}}
                for coin, szi in self.pos.items()
                if abs(szi) > EPS
            ]
        return {"assetPositions": rows, "withdrawable": "paper"}

    def open_orders(self, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {"coin": o["coin"], "side": "B" if o["is_buy"] else "A", "limitPx": str(o["px"]),
                 "sz": str(o["sz"]), "oid": oid, "cloid": o["cloid"], "reduceOnly": o["ro"]}
                for oid, o in self.orders.items()
            ]

    def user_fills(self, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.fills)

    def user_rate_limit(self, *_a: Any, **_kw: Any) -> dict[str, Any]:
        return {"nRequestsCap": 10**9, "nRequestsUsed": 0, "nRequestsSurplus": 0, "cumVlm": "0"}

    def extra_agents(self, *_a: Any, **_kw: Any) -> list[Any]:
        return []

    def l2_book(self, coin: str) -> dict[str, Any]:
        if self.info is None:
            raise RuntimeError("paper: no REST client")
        return self.info.l2_book(coin)

    # ---------------------------------------------------------------- 交易接口

    def post_exchange(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = payload.get("action") or {}
        kind = str(action.get("type"))
        events: list[dict[str, Any]] = []
        with self._lock:
            self.actions[kind] = self.actions.get(kind, 0) + 1
            if kind == "order":
                statuses = [self._order(w, events) for w in action.get("orders") or []]
            elif kind == "cancel":
                statuses = [self._cancel_oid(int(c["o"])) for c in action.get("cancels") or []]
            elif kind == "cancelByCloid":
                statuses = [self._cancel_cloid(str(c["cloid"])) for c in action.get("cancels") or []]
            else:
                statuses = []
        self._emit(events)
        if kind in {"order", "cancel", "cancelByCloid"}:
            return {"status": "ok", "response": {"type": kind, "data": {"statuses": statuses}}}
        return {"status": "ok", "response": {"type": "default"}}

    def _order(self, wire: dict[str, Any], events: list[dict[str, Any]]) -> Any:
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
        self._oid += 1
        oid = self._oid
        if tif == "Alo":
            if crosses:
                return {"error": f"Post only order would have immediately matched, bbo was {top.bid}@{top.ask}"}
            self.orders[oid] = {"coin": coin, "is_buy": is_buy, "px": px, "sz": sz, "ro": ro,
                                "cloid": wire.get("c")}
            return {"resting": {"oid": oid}}
        if not crosses:
            return {"error": "Order could not immediately match against any resting orders."}
        touch_px = top.ask if is_buy else top.bid
        touch_sz = (top.ask_sz if is_buy else top.bid_sz) or sz
        fill_sz = min(sz, touch_sz)
        self._fill(coin, oid, is_buy, touch_px, fill_sz, crossed=True, events=events)
        return {"filled": {"totalSz": str(fill_sz), "avgPx": str(touch_px), "oid": oid}}

    def _cancel_oid(self, oid: int) -> Any:
        if self.orders.pop(oid, None) is None:
            return {"error": "Order was never placed, already canceled, or filled."}
        return "success"

    def _cancel_cloid(self, cloid: str) -> Any:
        for oid, order in list(self.orders.items()):
            if order["cloid"] == cloid:
                return self._cancel_oid(oid)
        return {"error": "Order was never placed, already canceled, or filled."}

    def _fill(self, coin: str, oid: int, is_buy: bool, px: float, sz: float, *,
              crossed: bool, events: list[dict[str, Any]]) -> None:
        signed = sz if is_buy else -sz
        before = self.pos[coin]
        closed = 0.0
        if before and (before > 0) != (signed > 0):
            closed = (px - self.entry[coin]) * min(abs(before), abs(signed)) * (1 if before > 0 else -1)
        # 交易所的仓位是整数个最小单位；浮点累加会留下 0.0000999… 这种假零头。
        after = round(before + signed, self.markets[coin].sz_decimals)
        if abs(after) <= 1e-9:
            after = 0.0
            self.entry[coin] = 0.0
        elif before == 0 or (before > 0) != (after > 0):
            self.entry[coin] = px
        elif (before > 0) == (signed > 0):
            self.entry[coin] = (self.entry[coin] * abs(before) + px * sz) / abs(after)
        self.pos[coin] = after
        fee = px * sz * (self._taker_rate[coin] if crossed else self._maker_rate[coin])
        self.realized[coin] += closed
        self.fees_paid += fee
        self._tid += 1
        row = {"coin": coin, "px": str(px), "sz": str(sz), "side": "B" if is_buy else "A",
               "time": int(time.time() * 1000), "startPosition": str(before), "closedPnl": str(closed),
               "oid": oid, "crossed": crossed, "fee": str(fee), "tid": self._tid}
        self.fills.append(row)
        events.append(row)
        log.info("PAPER FILL %s %s sz=%s px=%s %s pos=%s closedPnl=%.4f", coin,
                 "BUY" if is_buy else "SELL", sz, px, "taker" if crossed else "maker", after, closed)

    def on_book(self, coin: str, data: dict[str, Any]) -> None:
        events: list[dict[str, Any]] = []
        with self._lock:
            top = book_top(coin, data)
            self.tops[coin] = top
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
                    self.orders.pop(oid)  # 仓位归零后交易所会撤掉剩下的 reduce-only 单
                    continue
                self._fill(coin, oid, order["is_buy"], order["px"], fill_sz, crossed=False, events=events)
                order["sz"] -= fill_sz
                if order["sz"] <= EPS:
                    self.orders.pop(oid)
        self._emit(events)

    def _emit(self, fills: list[dict[str, Any]]) -> None:
        listener = self.listener
        if listener is not None and fills:
            listener.on_user_fills({"fills": fills, "isSnapshot": False})

    def summary(self) -> str:
        with self._lock:
            maker = sum(1 for f in self.fills if not f["crossed"])
            volume = sum(float(f["px"]) * float(f["sz"]) for f in self.fills)
            realized = sum(self.realized.values())
            return (
                f"PAPER SUMMARY fills={len(self.fills)} (maker {maker} / taker {len(self.fills) - maker}) "
                f"volume=${volume:.2f} positions={self.pos} realized=${realized:.4f} "
                f"fees=${self.fees_paid:.4f} net=${realized - self.fees_paid:.4f} "
                f"signed_actions_if_live={sum(self.actions.values())} {self.actions}"
            )


def run_paper(settings: Settings, *, seconds: float | None = None) -> int:
    seconds = PAPER_DEFAULT_S if seconds is None else seconds
    if not 0 < seconds <= MAX_SESSION_S:
        raise LiveGuardError(f"paper --seconds must be in (0, {MAX_SESSION_S}]")
    client = InfoClient(settings.api_url)
    feed: BookFeed | None = None
    quoter: LiveQuoter | None = None
    paper: PaperExchange | None = None
    previous: dict[int, Any] = {}
    rc = 1
    try:
        _meta, markets, _ctxs = load_io_markets(client, settings.coins)
        dex = perp_dex_info(client.perp_dexs(), ALLOWED_DEX)
        log.info("PAPER (supervised session, simulated /exchange, nothing signed)  dex=%s/%s  "
                 "coins=%s  notional=$%s  seconds=%s", dex.get("name"), dex.get("fullName"),
                 ",".join(settings.coins), settings.quote_notional_usd, seconds)
        log.info("%s", describe_fee_model(settings))
        log.info("%s", ALO_TIF_NOTE)
        paper = PaperExchange(markets, client, settings)
        quoter = LiveQuoter(settings, markets, paper, PaperSigner())  # type: ignore[arg-type]
        quoter.session = SupervisedSession(seconds)
        paper.listener = quoter
        sim = paper
        q = quoter

        stop: dict[str, Any] = {"flag": False, "signal": None, "count": 0}

        def _handle(signum: int, _frame: object) -> None:
            stop["signal"] = signum
            stop["count"] += 1
            stop["flag"] = True
            if q.session is not None:
                q.session.request_stop(f"signal:{signal.Signals(signum).name}")

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, _handle)

        def on_book(coin: str, data: dict[str, Any]) -> None:
            sim.on_book(coin, data)
            q.on_book(coin, data)

        feed = BookFeed(settings.ws_url, settings.coins, on_book, on_asset_ctx=quoter.on_asset_ctx)
        quoter._feed = feed
        feed.start()
        if not feed.wait_ready():
            log.error("websocket did not become ready")
            return 1
        # 先等到每个币的第一份盘口，模拟交易所才有价格可撮合。
        deadline = time.time() + 15
        while not all(c in paper.tops for c in markets) and time.time() < deadline:
            time.sleep(0.2)
        quoter.preflight()
        quoter.managed_started = True
        quoter.bootstrap_isolated()
        rc = _run_supervised(quoter, feed, stop)
    finally:
        if feed is not None:
            feed.stop()
        if quoter is not None and quoter.managed_started:
            quoter.flush_markouts()
            try:
                quoter.shutdown_cancels()
            except Exception as exc:  # noqa: BLE001
                log.error("%s", exc)
                rc = rc or 3
        if paper is not None:
            log.info("%s", paper.summary())
        client.close()
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    return rc
