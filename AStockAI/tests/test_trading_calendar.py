from __future__ import annotations

from datetime import date, time, timedelta

from conftest import bj

from trading_calendar import SessionPhase, TradingCalendar, phase_of_time


def _calendar(dates: list[date], fallback: bool = True) -> TradingCalendar:
    cal = TradingCalendar(lambda: dates, db=None, allow_weekday_fallback=fallback)
    cal.refresh(date(2026, 10, 1))
    return cal


def _weekdays(start: date, end: date, holidays: set[date]) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d += timedelta(days=1)
    return out


HOLIDAYS = {date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7)}
DATES = _weekdays(date(2026, 1, 1), date(2026, 12, 31), HOLIDAYS)


def test_phase_boundaries() -> None:
    assert phase_of_time(time(9, 14)) is SessionPhase.PRE_OPEN
    assert phase_of_time(time(9, 15)) is SessionPhase.CALL_AUCTION
    assert phase_of_time(time(9, 25)) is SessionPhase.PRE_CONTINUOUS
    assert phase_of_time(time(9, 30)) is SessionPhase.MORNING
    assert phase_of_time(time(11, 30)) is SessionPhase.LUNCH_BREAK
    assert phase_of_time(time(13, 0)) is SessionPhase.AFTERNOON
    assert phase_of_time(time(15, 0)) is SessionPhase.POST_CLOSE


def test_holiday_is_not_trading_day() -> None:
    cal = _calendar(DATES)
    assert cal.is_trading_day(date(2026, 10, 1)) == (False, "verified")
    phase, basis = cal.phase(bj(2026, 10, 5, 10, 0))
    assert phase is SessionPhase.NON_TRADING_DAY and basis == "verified"
    phase, _ = cal.phase(bj(2026, 10, 8, 10, 0))
    assert phase is SessionPhase.MORNING


def test_previous_next_and_latest_completed() -> None:
    cal = _calendar(DATES)
    assert cal.previous_trading_day(date(2026, 10, 8)) == date(2026, 9, 30)
    assert cal.next_trading_day(date(2026, 9, 30)) == date(2026, 10, 8)
    assert cal.latest_completed_trading_day(bj(2026, 10, 8, 14, 0)) == date(2026, 9, 30)
    assert cal.latest_completed_trading_day(bj(2026, 10, 8, 15, 30)) == date(2026, 10, 8)


def test_fallback_outside_coverage() -> None:
    cal = _calendar(DATES, fallback=True)
    assert cal.is_trading_day(date(2027, 1, 4)) == (True, "fallback_weekday")
    strict = _calendar(DATES, fallback=False)
    assert strict.is_trading_day(date(2027, 1, 4)) == (None, "unavailable")
    phase, _ = strict.phase(bj(2027, 1, 4, 10, 0))
    assert phase is SessionPhase.UNKNOWN_DAY


def test_refresh_failure_keeps_cache_and_records_error() -> None:
    def broken() -> list[date]:
        raise ConnectionError("offline")

    cal = TradingCalendar(broken, db=None)
    assert cal.refresh(date(2026, 10, 1)) is False
    assert "offline" in (cal.last_error or "")
