"""执行层保护；不参与价格、方向或仓位大小的计算。

平台额度以 userRateLimit 为准；本地预扣只会更保守，不会增加平台额度。
发生累计限额或未知写入结果后，本次进程锁定禁止开仓，需人工核对后重启。
"""

from dataclasses import dataclass
from math import isfinite
from typing import Any

BUDGET_POLL_S = 15.0
BUDGET_FRESH_S = 20.0
EXIT_RESERVE_PER_MARKET = 25  # 工程缓冲，不是官方阈值，更不是保证平仓的额度。
LIMITED_EXIT_GAP_S = 11.0  # 官方降级通道 10 秒；留 1 秒余量。
HTTP_BACKOFF_S = 10.0


@dataclass
class RequestBudget:
    checked_at: float = 0.0
    available: float = 0.0
    last_cap: int = 0
    last_used: int = 0
    valid: bool = False

    def update(self, raw: Any, now: float) -> bool:
        self.valid = False
        try:
            values = [raw[k] for k in ("nRequestsCap", "nRequestsUsed", "nRequestsSurplus")]
            if any(isinstance(x, bool) or not isfinite(float(x)) or float(x) < 0
                   or int(x) != float(x) for x in values):
                return False
            cap, used, surplus = map(int, values)
        except (KeyError, TypeError, ValueError, OverflowError):
            return False
        observed = cap - used + surplus
        if self.checked_at:
            # 已发送动作在 API 统计中可能延迟出现。不能因旧快照而归还预扣。
            projected = self.available + max(0, cap - self.last_cap)
            self.available = min(observed, projected)
        else:
            self.available = observed
        self.last_cap, self.last_used = cap, used
        self.checked_at, self.valid = now, True
        return True

    def debit(self, actions: int) -> None:
        self.available -= actions

    def can_enter(self, now: float, markets: int, actions: int = 2) -> bool:
        return (self.valid and 0 <= now - self.checked_at <= BUDGET_FRESH_S
                and self.available >= EXIT_RESERVE_PER_MARKET * markets + actions)


def cancel_confirmed(response: Any, count: int) -> bool:
    """HTTP 200 / 外层 ok 不代表批量撤单全部成功。"""
    try:
        statuses = response["response"]["data"]["statuses"]
        if response.get("status") != "ok" or len(statuses) != count:
            return False
        return all(s == "success" or (
            isinstance(s, dict) and isinstance(s.get("error"), str)
            and s["error"].startswith("Order was never placed, already canceled, or filled.")
        ) for s in statuses)
    except (TypeError, KeyError):
        return False
