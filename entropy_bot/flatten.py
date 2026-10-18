"""紧急手动平仓：撤本机器人的挂单，用 reduce-only IOC 把 io:ANTH / io:SNDK 仓位平掉，再核对。

用于值守会话收尾超时（`SESSION cleanup timed out` / `SHUTDOWN NOT CLEAN`）之类的情况。
- 只撤 EBOT 标记的订单；你手动挂的单、止盈止损单一律不动（会列出来提醒）。
- 只发 reduce-only 单，不会开仓；不申请 dead-man。
- 和 scripts/run-supervised.sh 共用同一把运行锁：机器人还在跑时拒绝执行。
- 执行前要输入 FLATTEN 确认（--yes 跳过）。
- 退出码 0 只代表交易所已报告：无仓位、无本机器人挂单。
"""

from __future__ import annotations

import fcntl
import logging
import os
import sys
import time
from typing import Any

from entropy_bot.coins import ALLOWED_COINS, ALLOWED_DEX
from entropy_bot.config import Settings, require_signer
from entropy_bot.errors import LiveGuardError, RateLimited, RequestWeightLimited
from entropy_bot.live import check_signer_role, positions_from_state
from entropy_bot.orders import LiveSigner, is_bot_cloid, resp_statuses, status_error
from entropy_bot.quoting import FLAT_TAKE_MS, book_top, quote_plan
from entropy_bot.rest import InfoClient
from entropy_bot.safety import LIMITED_EXIT_GAP_S, cancel_confirmed
from entropy_bot.status import load_io_markets

log = logging.getLogger("entropy_bot.flatten")

FLATTEN_TIMEOUT_S = 60.0
FLATTEN_GAP_S = 3.0  # 和实盘退出重试间隔一致
FLATTEN_SLIPPAGE_TICKS = 2


def _acquire_run_lock() -> Any:
    state_dir = os.environ.get("ENTROPY_STATE_DIR", "/var/lib/entropy-bot")
    path = os.path.join(state_dir, "live.lock")
    if not os.path.isdir(state_dir):
        log.warning("run lock dir %s missing; cannot check for a running bot — make sure none is", state_dir)
        return None
    handle = open(path, "a")  # noqa: SIM115 - 进程结束前一直持有
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise LiveGuardError(f"a supervised session holds {path}; stop it first (Ctrl+C in its tmux)") from exc
    return handle


def _confirm(positions: dict[str, float], foreign: int) -> bool:
    print(f"将用 reduce-only IOC 平掉：{positions or '无仓位'}；撤销本机器人挂单。")
    if foreign:
        print(f"另有 {foreign} 张不是本机器人的挂单（手动单/止盈止损），不会撤，请自己处理。")
    try:
        answer = input("确认请输入 FLATTEN：")
    except EOFError:
        return False
    return answer.strip() == "FLATTEN"


def run_flatten(settings: Settings, *, assume_yes: bool = False) -> int:
    require_signer(settings)
    lock = _acquire_run_lock()
    signer = LiveSigner(settings.private_key or "", settings.account)
    client = InfoClient(settings.api_url)
    try:
        check_signer_role(client, signer)
        _meta, markets, _ctxs = load_io_markets(client, ALLOWED_COINS)  # 不看 COINS：两个币都查
        state = client.clearinghouse_state(signer.account, ALLOWED_DEX, optional=True)
        opens = client.open_orders(signer.account, ALLOWED_DEX, optional=True)
        pos = positions_from_state(state, tuple(markets), markets)
        if pos is None or opens is None:
            raise LiveGuardError("account state unavailable; nothing sent")
        open_pos = {c: q for c, q in pos.items() if q}
        foreign = sum(1 for o in opens if o.get("coin") in markets and not is_bot_cloid(o.get("cloid")))
        if not assume_yes and not (sys.stdin.isatty() and _confirm(open_pos, foreign)):
            print("未确认，没有发送任何操作。", file=sys.stderr)
            return 2
        deadline = time.monotonic() + FLATTEN_TIMEOUT_S
        next_try = 0.0
        while time.monotonic() < deadline:
            if time.monotonic() < next_try:
                time.sleep(0.2)
                continue
            next_try = time.monotonic() + FLATTEN_GAP_S
            opens = client.open_orders(signer.account, ALLOWED_DEX, optional=True)
            state = client.clearinghouse_state(signer.account, ALLOWED_DEX, optional=True)
            pos = positions_from_state(state, tuple(markets), markets)
            if opens is None or pos is None:
                log.warning("account state unavailable; retrying")
                continue
            bot = [o for o in opens if o.get("coin") in markets and is_bot_cloid(o.get("cloid"))]
            open_pos = {c: q for c, q in pos.items() if q}
            if not bot and not open_pos:
                log.info("FLATTEN DONE: exchange reports no position and no bot orders on %s",
                         ",".join(markets))
                return 0
            try:
                for coin in markets:
                    oids = [int(o["oid"]) for o in bot if o.get("coin") == coin]
                    if oids:
                        resp = client.post_exchange(
                            signer.signed_cancel_oids([(markets[coin].asset_id, oid) for oid in oids]))
                        log.info("cancel %s %s confirmed=%s", coin, oids, cancel_confirmed(resp, len(oids)))
                for coin, szi in open_pos.items():
                    top = book_top(coin, client.l2_book(coin))
                    if top.bid is None or top.ask is None:
                        log.warning("no book for %s; retrying", coin)
                        continue
                    px = max(top.ask, top.bid)
                    plan = quote_plan(markets[coin], top, szi=szi, age_ms=FLAT_TAKE_MS,
                                      notional_usd=abs(szi) * px * 2 + 1.0,
                                      take_slippage_ticks=max(FLATTEN_SLIPPAGE_TICKS, settings.take_slippage_ticks))
                    specs = [(markets[coin], i.is_buy, i.px, i.sz, signer.next_cloid(coin, i.side), True, "Ioc")
                             for i in plan.intents]
                    if not specs:
                        continue
                    resp = client.post_exchange(signer.signed_orders(specs))
                    log.info("FLATTEN IOC %s szi=%s -> %s", coin, szi,
                             [status_error(st) or st for st in resp_statuses(resp)])
            except RequestWeightLimited as exc:
                log.warning("request weight limited; next try in %.0fs: %s", LIMITED_EXIT_GAP_S, exc)
                next_try = time.monotonic() + LIMITED_EXIT_GAP_S
            except RateLimited as exc:
                log.warning("rate limited; retrying: %s", exc)
            # ExchangeOutcomeUnknown 不捕获：结果未知时不盲目重发，直接退出让人工核对。
        log.error("FLATTEN NOT CONFIRMED after %.0fs: position %s; check the site manually",
                  FLATTEN_TIMEOUT_S, open_pos)
        return 3
    finally:
        client.close()
        if lock is not None:
            lock.close()
