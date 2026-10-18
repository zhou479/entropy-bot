"""人工值守会话的时间边界，不参与报价和仓位大小计算。"""

from dataclasses import dataclass, field
from math import isfinite
import time

from entropy_bot.errors import LiveGuardError

MAX_SESSION_S = 3 * 60 * 60
DRAIN_LEAD_S = 30.0
DRAIN_GRACE_S = 60.0


@dataclass
class SupervisedSession:
    seconds: float
    started_at: float = field(default_factory=lambda: time.monotonic())
    stop_reason: str = ""
    drain_deadline: float | None = None

    def __post_init__(self) -> None:
        if (isinstance(self.seconds, bool) or self.seconds is None
                or not isfinite(self.seconds) or not 0 < self.seconds <= MAX_SESSION_S):
            raise LiveGuardError(
                "supervised mode requires finite --seconds in (0, 10800]; no unlimited runs"
            )

    @property
    def end_at(self) -> float:
        return self.started_at + self.seconds

    @property
    def entry_cutoff(self) -> float:
        return self.end_at - min(DRAIN_LEAD_S, self.seconds / 2)

    def can_enter(self) -> bool:
        return not self.stop_reason and time.monotonic() < self.entry_cutoff

    def can_exit(self) -> bool:
        deadline = self.drain_deadline if self.drain_deadline is not None else self.end_at + DRAIN_GRACE_S
        return time.monotonic() < deadline

    def request_stop(self, reason: str, *, scheduled: bool = False) -> bool:
        if self.stop_reason:
            return False
        self.stop_reason = reason
        # 正常到时最多给结束时刻后 60s；手动/安全停机最多给当前时刻后 60s。
        latest = self.end_at + DRAIN_GRACE_S
        self.drain_deadline = latest if scheduled else min(latest, time.monotonic() + DRAIN_GRACE_S)
        return True

    def tick(self) -> bool:
        if time.monotonic() >= self.entry_cutoff:
            return self.request_stop("duration", scheduled=True)
        return False
