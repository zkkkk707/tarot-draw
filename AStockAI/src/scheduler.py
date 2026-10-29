"""轻量级单线程调度器。

为什么是单线程
- 内存可控：任意时刻只有一个任务在处理数据，峰值内存约等于“基础占用 + 单个任务占用”。
- 无需线程同步，SQLite 单连接即可。
- 长任务（如收盘后全A日K更新）被拆成小批次：每次运行处理一批后返回 MORE，
  调度器在批次之间轮转其它任务、检查内存与停止信号。

任务类型
- 周期任务：interval_sec 为间隔，按交易时段（phases）与自定义条件（eligible）放行。
- 每日任务：daily=True，day_key 给出“目标日”键；成功返回 DONE 后在 job_state 表记录完成，
  当天不再运行。程序重启后仍能识别已完成的每日任务。

失败处理
- 数据源异常：指数退避（interval × 2^失败次数，上限 schedule.max_backoff_sec），不会无限快速重试。
- 每日任务同一目标日最多尝试 schedule.daily_job_max_attempts 次，超过后当日放弃并记录。
- MemoryError：通知内存守护释放缓存；多次发生则请求安全退出。
- 其它未预期异常：完整记录堆栈（logger.exception），不静默吞掉。
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable

from database import Database, WritePausedError
from market_data import DataSourceError
from memory_guard import MemoryBudgetExceeded, MemoryGuard, TaskKind
from timeutil import BEIJING_TZ, now_utc, to_bj_iso
from trading_calendar import SessionPhase, TradingCalendar

logger = logging.getLogger(__name__)


class JobResult(Enum):
    DONE = "done"        # 本轮完成
    MORE = "more"        # 还有剩余批次，尽快再次调度
    SKIPPED = "skipped"  # 条件不满足（如缺少前置数据），按正常间隔再试


@dataclass(frozen=True)
class JobContext:
    now: datetime            # UTC
    now_bj: datetime         # 北京时间
    today: date              # 北京日期
    phase: SessionPhase
    calendar_basis: str      # verified / fallback_weekday / unavailable
    target_day: date | None  # 最近一个已收盘交易日


@dataclass
class Job:
    name: str
    func: Callable[[JobContext], JobResult]
    kind: TaskKind
    interval_sec: float
    phases: frozenset[SessionPhase]
    eligible: Callable[[JobContext], bool] | None = None
    daily: bool = False
    day_key: Callable[[JobContext], str | None] | None = None
    more_delay_sec: float = 0.5
    # 返回 SKIPPED（前置条件未满足）时的重试间隔；None 表示使用 interval_sec
    skip_retry_sec: float | None = None
    # ---- 运行期状态 ----
    next_run_mono: float = 0.0
    failures: int = 0
    last_run_at: datetime | None = None
    last_status: str = "never_run"
    last_error: str | None = None
    last_duration_sec: float | None = None
    _last_skip_log: float = field(default=-1e18, repr=False)

    def status_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "last_run_at": to_bj_iso(self.last_run_at) if self.last_run_at else None,
            "last_status": self.last_status,
            "last_error": self.last_error,
            "last_duration_sec": self.last_duration_sec,
            "consecutive_failures": self.failures,
        }


class Scheduler:
    def __init__(self, db: Database, calendar: TradingCalendar, guard: MemoryGuard,
                 tick_sec: float, max_backoff_sec: float, daily_max_attempts: int,
                 clock: Callable[[], float] = time.monotonic,
                 now_fn: Callable[[], datetime] = now_utc) -> None:
        self.db = db
        self.calendar = calendar
        self.guard = guard
        self.tick_sec = tick_sec
        self.max_backoff_sec = max_backoff_sec
        self.daily_max_attempts = daily_max_attempts
        self._clock = clock
        self._now = now_fn
        self.jobs: list[Job] = []
        self._daily_attempts: dict[tuple[str, str], int] = {}
        self.stop_reason = ""

    def add(self, job: Job) -> None:
        if any(j.name == job.name for j in self.jobs):
            raise ValueError(f"任务名重复: {job.name}")
        if job.daily and job.day_key is None:
            raise ValueError(f"每日任务 {job.name} 必须提供 day_key")
        self.jobs.append(job)

    # -- 上下文 ---------------------------------------------------------------
    def context(self) -> JobContext:
        now = self._now()
        now_bj = now.astimezone(BEIJING_TZ)
        phase, basis = self.calendar.phase(now)
        target = self.calendar.latest_completed_trading_day(now)
        return JobContext(now=now, now_bj=now_bj, today=now_bj.date(), phase=phase,
                          calendar_basis=basis, target_day=target)

    # -- 判定与执行 -----------------------------------------------------------
    @staticmethod
    def done_key(job_name: str, key: str) -> str:
        return f"done:{job_name}:{key}"

    def is_due(self, job: Job, ctx: JobContext) -> bool:
        if self._clock() < job.next_run_mono:
            return False
        if ctx.phase not in job.phases:
            return False
        if job.eligible is not None and not job.eligible(ctx):
            return False
        if job.daily:
            key = job.day_key(ctx) if job.day_key else None
            if key is None:
                return False
            if self.db.get_state(self.done_key(job.name, key)) is not None:
                return False
            if self._daily_attempts.get((job.name, key), 0) >= self.daily_max_attempts:
                return False
        return True

    def _backoff(self, job: Job) -> float:
        delay = job.interval_sec * (2 ** min(job.failures, 6))
        return max(job.interval_sec, min(delay, self.max_backoff_sec))

    def run_job(self, job: Job, ctx: JobContext) -> None:
        mono = self._clock()
        if not self.guard.allow(job.kind):
            job.last_status = f"skipped_memory_{self.guard.level.name.lower()}"
            job.next_run_mono = mono + min(job.interval_sec, 60.0)
            if mono - job._last_skip_log > 300:
                logger.warning("内存级别 %s，跳过任务 %s（%s）", self.guard.level.name, job.name, job.kind.value)
                job._last_skip_log = mono
            return

        key = job.day_key(ctx) if (job.daily and job.day_key) else None
        job.last_run_at = ctx.now
        started = self._clock()
        try:
            result = job.func(ctx)
        except MemoryBudgetExceeded as exc:
            job.last_status = "aborted_memory_budget"
            job.last_error = str(exc)
            job.next_run_mono = self._clock() + 60.0
            logger.warning("任务 %s 因内存预算中止: %s", job.name, exc)
        except MemoryError:
            job.failures += 1
            job.last_status = "memory_error"
            job.last_error = "MemoryError"
            job.next_run_mono = self._clock() + self._backoff(job)
            self.guard.on_memory_error(job.name)
            self._count_daily_attempt(job, key)
        except WritePausedError as exc:
            job.last_status = "write_paused"
            job.last_error = str(exc)
            job.next_run_mono = self._clock() + max(job.interval_sec, 300.0)
            logger.warning("任务 %s 因数据库写入暂停而跳过: %s", job.name, exc)
        except DataSourceError as exc:
            job.failures += 1
            job.last_status = "data_source_error"
            job.last_error = str(exc)[:500]
            delay = self._backoff(job)
            job.next_run_mono = self._clock() + delay
            logger.warning("任务 %s 数据源异常（第 %d 次），%.0f 秒后重试: %s", job.name, job.failures, delay, exc)
            self._count_daily_attempt(job, key)
        except Exception as exc:
            job.failures += 1
            job.last_status = "error"
            job.last_error = f"{type(exc).__name__}: {exc}"[:500]
            delay = self._backoff(job)
            job.next_run_mono = self._clock() + delay
            logger.exception("任务 %s 发生未预期异常（第 %d 次），%.0f 秒后重试", job.name, job.failures, delay)
            self._count_daily_attempt(job, key)
        else:
            job.failures = 0
            job.last_error = None
            job.last_status = result.value
            if result is JobResult.MORE:
                job.next_run_mono = self._clock() + job.more_delay_sec
            elif result is JobResult.SKIPPED and job.skip_retry_sec is not None:
                job.next_run_mono = self._clock() + job.skip_retry_sec
            else:
                job.next_run_mono = self._clock() + job.interval_sec
            if job.daily and key is not None and result is JobResult.DONE:
                self.db.set_state(self.done_key(job.name, key), to_bj_iso(ctx.now))
                logger.info("每日任务 %s 已完成（目标日 %s）", job.name, key)
        finally:
            job.last_duration_sec = round(self._clock() - started, 3)

    def _count_daily_attempt(self, job: Job, key: str | None) -> None:
        if not job.daily or key is None:
            return
        n = self._daily_attempts.get((job.name, key), 0) + 1
        self._daily_attempts[(job.name, key)] = n
        if n >= self.daily_max_attempts:
            logger.error("每日任务 %s 目标日 %s 已失败 %d 次，今日不再重试", job.name, key, n)
            try:
                self.db.set_state(f"failed:{job.name}:{key}", to_bj_iso(now_utc()))
            except Exception:
                logger.exception("记录每日任务失败状态时出错")
        # 防止字典无限增长：只保留最近的键
        if len(self._daily_attempts) > 200:
            for k in list(self._daily_attempts)[:100]:
                self._daily_attempts.pop(k, None)

    # -- 主循环 ---------------------------------------------------------------
    def run_forever(self, stop_event: threading.Event,
                    on_tick: Callable[[JobContext], None] | None = None) -> None:
        logger.info("调度器启动，共 %d 个任务", len(self.jobs))
        while not stop_event.is_set():
            self.guard.check()
            if self.guard.shutdown_requested:
                self.stop_reason = f"memory_guard: {self.guard.shutdown_reason}"
                break
            ctx = self.context()
            for job in self.jobs:
                if stop_event.is_set() or self.guard.shutdown_requested:
                    break
                try:
                    if self.is_due(job, ctx):
                        self.run_job(job, ctx)
                        ctx = self.context()  # 任务可能运行较久，刷新时间与时段
                except Exception:
                    # 调度器自身的异常（如记录状态时数据库出错）也必须可见，且不应拖垮主循环
                    logger.exception("调度任务 %s 时发生异常", job.name)
                    job.next_run_mono = self._clock() + max(job.interval_sec, 60.0)
            if on_tick is not None:
                try:
                    on_tick(ctx)
                except Exception:
                    logger.exception("on_tick 回调异常")
            stop_event.wait(self.tick_sec)
        if not self.stop_reason:
            self.stop_reason = "stop_event"
        logger.info("调度器退出: %s", self.stop_reason)

    def status(self) -> dict[str, Any]:
        return {job.name: job.status_dict() for job in self.jobs}
