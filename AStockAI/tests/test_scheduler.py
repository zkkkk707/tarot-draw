from __future__ import annotations

from datetime import date, datetime, timedelta

from conftest import bj

from config import MB, MemoryConfig
from database import Database
from market_data import DataSourceError
from memory_guard import MemoryGuard, MemorySample, TaskKind
from scheduler import Job, JobResult, Scheduler
from trading_calendar import SessionPhase, TradingCalendar


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _sampler(mb: float):  # type: ignore[no-untyped-def]
    def f() -> MemorySample:
        b = int(mb * MB)
        return MemorySample(0.0, b, b, 0, 0, 0)
    return f


def _weekday_calendar() -> TradingCalendar:
    dates = []
    d = date(2026, 1, 1)
    while d <= date(2026, 12, 31):
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)
    cal = TradingCalendar(lambda: dates, db=None)
    cal.refresh(date(2026, 10, 1))
    return cal


def _scheduler(db: Database, now: datetime, mem_mb: float = 300.0) -> tuple[Scheduler, Clock]:
    guard = MemoryGuard(MemoryConfig(require_hard_limit=False, check_interval_sec=0.0), sampler=_sampler(mem_mb))
    clock = Clock()
    sch = Scheduler(db, _weekday_calendar(), guard, tick_sec=1.0, max_backoff_sec=900.0,
                    daily_max_attempts=3, clock=clock, now_fn=lambda: now)
    return sch, clock


def test_context_phase_and_target_day(db: Database) -> None:
    sch, _ = _scheduler(db, bj(2026, 10, 29, 10, 0))  # 周四上午
    ctx = sch.context()
    assert ctx.phase is SessionPhase.MORNING
    assert ctx.target_day == date(2026, 10, 28)


def test_daily_job_runs_once_and_is_persisted(db: Database) -> None:
    sch, clock = _scheduler(db, bj(2026, 10, 29, 16, 0))
    calls: list[int] = []

    def fn(ctx) -> JobResult:  # type: ignore[no-untyped-def]
        calls.append(1)
        return JobResult.DONE

    job = Job("daily_test", fn, TaskKind.HEAVY, 60.0, frozenset({SessionPhase.POST_CLOSE}),
              daily=True, day_key=lambda ctx: ctx.target_day.isoformat())
    sch.add(job)
    ctx = sch.context()
    assert sch.is_due(job, ctx)
    sch.run_job(job, ctx)
    clock.t += 1000
    assert not sch.is_due(job, sch.context())
    assert db.get_state("done:daily_test:2026-10-29") is not None
    assert calls == [1]


def test_more_result_reschedules_quickly(db: Database) -> None:
    sch, clock = _scheduler(db, bj(2026, 10, 29, 16, 0))
    job = Job("batch", lambda ctx: JobResult.MORE, TaskKind.HEAVY, 60.0, frozenset(SessionPhase))
    sch.run_job(job, sch.context())
    assert job.next_run_mono == clock.t + job.more_delay_sec


def test_data_source_error_backs_off_and_daily_attempts_capped(db: Database) -> None:
    sch, clock = _scheduler(db, bj(2026, 10, 29, 16, 0))

    def fail(ctx) -> JobResult:  # type: ignore[no-untyped-def]
        raise DataSourceError("x", "down")

    job = Job("daily_fail", fail, TaskKind.HEAVY, 60.0, frozenset({SessionPhase.POST_CLOSE}),
              daily=True, day_key=lambda ctx: "k")
    sch.add(job)
    delays = []
    for _ in range(3):
        clock.t = job.next_run_mono
        assert sch.is_due(job, sch.context())
        sch.run_job(job, sch.context())
        delays.append(job.next_run_mono - clock.t)
    assert delays == [120.0, 240.0, 480.0]  # 指数退避
    clock.t = job.next_run_mono
    assert not sch.is_due(job, sch.context())  # 当日已达最大尝试次数
    assert db.get_state("failed:daily_fail:k") is not None


def test_unexpected_exception_is_contained(db: Database) -> None:
    sch, _ = _scheduler(db, bj(2026, 10, 29, 16, 0))

    def boom(ctx) -> JobResult:  # type: ignore[no-untyped-def]
        raise KeyError("bug")

    job = Job("boom", boom, TaskKind.ESSENTIAL, 60.0, frozenset(SessionPhase))
    sch.run_job(job, sch.context())
    assert job.last_status == "error" and "KeyError" in (job.last_error or "")


def test_memory_level_skips_nonessential(db: Database) -> None:
    sch, _ = _scheduler(db, bj(2026, 10, 29, 16, 0), mem_mb=800.0)  # WARNING
    ran: list[int] = []
    job = Job("n", lambda ctx: ran.append(1) or JobResult.DONE, TaskKind.NONESSENTIAL, 60.0,
              frozenset(SessionPhase))
    sch.run_job(job, sch.context())
    assert ran == [] and job.last_status.startswith("skipped_memory")


def test_phase_gating(db: Database) -> None:
    sch, _ = _scheduler(db, bj(2026, 10, 31, 10, 0))  # 周六
    job = Job("t", lambda ctx: JobResult.DONE, TaskKind.ESSENTIAL, 60.0,
              frozenset({SessionPhase.MORNING, SessionPhase.AFTERNOON}))
    assert not sch.is_due(job, sch.context())
