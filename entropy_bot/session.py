"""NYSE regular trading hours for equity-backed HIP-3 perps (io:SNDK).

`rth` = Mon–Fri 09:30–16:00 America/New_York, minus NYSE full-day holidays,
with 13:00 early closes. Everything else is `ah` (oracle is stale or thin).

The holiday table is static. Extend it each year from
https://www.nyse.com/markets/hours-calendars — a missing date only means a
holiday is treated as a normal session, never the reverse.
"""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
RTH_START = time(9, 30)
RTH_END = time(16, 0)
EARLY_CLOSE = time(13, 0)

NYSE_HOLIDAYS: frozenset[date] = frozenset(
    {
        # 2025
        date(2025, 1, 1),
        date(2025, 1, 9),
        date(2025, 1, 20),
        date(2025, 2, 17),
        date(2025, 4, 18),
        date(2025, 5, 26),
        date(2025, 6, 19),
        date(2025, 7, 4),
        date(2025, 9, 1),
        date(2025, 11, 27),
        date(2025, 12, 25),
        # 2026
        date(2026, 1, 1),
        date(2026, 1, 19),
        date(2026, 2, 16),
        date(2026, 4, 3),
        date(2026, 5, 25),
        date(2026, 6, 19),
        date(2026, 7, 3),
        date(2026, 9, 7),
        date(2026, 11, 26),
        date(2026, 12, 25),
        # 2027
        date(2027, 1, 1),
        date(2027, 1, 18),
        date(2027, 2, 15),
        date(2027, 3, 26),
        date(2027, 5, 31),
        date(2027, 6, 18),
        date(2027, 7, 5),
        date(2027, 9, 6),
        date(2027, 11, 25),
        date(2027, 12, 24),
    }
)

NYSE_EARLY_CLOSES: frozenset[date] = frozenset(
    {
        date(2025, 7, 3),
        date(2025, 11, 28),
        date(2025, 12, 24),
        date(2026, 11, 27),
        date(2026, 12, 24),
        date(2027, 11, 26),
    }
)


def _ny(when: datetime | None) -> datetime:
    dt = datetime.now(NY_TZ) if when is None else when
    if dt.tzinfo is None:
        return dt.replace(tzinfo=NY_TZ)
    return dt.astimezone(NY_TZ)


def nyse_session(when: datetime | None = None) -> str:
    """`rth` inside NYSE regular hours, else `ah`."""
    dt = _ny(when)
    if dt.weekday() >= 5 or dt.date() in NYSE_HOLIDAYS:
        return "ah"
    end = EARLY_CLOSE if dt.date() in NYSE_EARLY_CLOSES else RTH_END
    clock = dt.timetz().replace(tzinfo=None)
    if RTH_START <= clock < end:
        return "rth"
    return "ah"


def is_rth(when: datetime | None = None) -> bool:
    return nyse_session(when) == "rth"
