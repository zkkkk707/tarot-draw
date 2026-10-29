"""AStockAI V1.0 主程序入口（仅由用户手动启动；不注册开机自启、计划任务或系统服务）。

启动顺序（顺序本身是内存控制的一部分）
1. 只导入标准库与轻量模块，读取并校验配置。
2. 设置本进程数值计算库线程数（不修改系统环境变量）。
3. 创建 Windows Job Object 并把本进程加入（内存硬限制）。失败且 require_hard_limit=true 时拒绝启动。
4. 之后才导入 pandas / numpy / akshare 等重型模块，使其内存全部计入 Job 限额。
5. 单实例锁：防止重复启动导致内存翻倍。
6. 进入单线程调度循环；Ctrl+C、Ctrl+Break 或在 run 目录创建 STOP 文件均可正常退出。

退出码：0 正常退出；1 未预期错误；2 配置错误；3 无法建立内存硬限制；4 已有实例在运行；
5 内存守护请求的安全退出。
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import platform
import signal
import sys
import threading
from datetime import datetime, time
from pathlib import Path
from typing import Any, TextIO

from config import DEFAULT_PROJECT_ROOT, ConfigError, Settings, load_settings
from memory_guard import HardLimitUnavailable, MemoryGuard, limit_numeric_threads
from timeutil import BEIJING_TZ, now_utc, parse_hhmm, to_bj_iso

APP_VERSION = "1.0.0"
STOP_FILE_NAME = "STOP"
LOCK_FILE_NAME = "astock.lock"

logger = logging.getLogger("astock")


# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------

class BeijingFormatter(logging.Formatter):
    """日志时间统一显示为北京时间（与运行电脑的时区设置无关）。"""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        dt = datetime.fromtimestamp(record.created, tz=BEIJING_TZ)
        return dt.isoformat(timespec="milliseconds")


def setup_logging(settings: Settings) -> None:
    fmt = BeijingFormatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    root = logging.getLogger()
    root.setLevel(settings.logging.level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)
    file_handler = logging.handlers.TimedRotatingFileHandler(
        settings.logs_path / settings.logging.file_name, when="midnight",
        backupCount=settings.logging.retention_days, encoding="utf-8", delay=True)
    file_handler.setFormatter(fmt)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(file_handler)
    root.addHandler(console)
    for noisy in ("urllib3", "requests", "chardet", "charset_normalizer", "matplotlib"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# 单实例锁
# ---------------------------------------------------------------------------

class InstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: TextIO | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # 锁文件需在整个进程生命周期内保持打开，故不使用 with 语句
        fh = open(self.path, "a+", encoding="utf-8")  # noqa: SIM115
        try:
            if sys.platform == "win32":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self._fh = fh
        return True

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            logger.warning("释放实例锁失败（进程退出时系统会自动释放）")
        finally:
            self._fh.close()
            self._fh = None


# ---------------------------------------------------------------------------
# 应用
# ---------------------------------------------------------------------------

class App:
    """持有所有组件并注册调度任务。重型模块在 __init__ 中导入（此时内存硬限制已生效）。"""

    def __init__(self, settings: Settings, guard: MemoryGuard) -> None:
        from anomaly_detector import AnomalyService
        from cleanup import CleanupService, log_dir_size
        from collector import MarketCollector
        from config import GB
        from database import Database
        from indicators import IndicatorService
        from market_data import build_adapter
        from scheduler import Scheduler
        from screener import ScreeningService
        from state import RuntimeState
        from trading_calendar import TradingCalendar
        from work_data import WorkDataExporter

        self.s = settings
        self.guard = guard
        self.stop_event = threading.Event()
        self._log_dir_size = log_dir_size

        st = settings.storage
        self.db = Database(settings.db_path, cache_kib=st.sqlite_cache_kib,
                           max_bytes=int(st.db_max_gb * GB), soft_bytes=int(st.db_soft_limit_gb * GB))
        self.db.connect()
        self.db.init_schema()
        self.db.refresh_space_state()

        # 重试等待使用 stop_event.wait，保证退出请求能及时打断等待
        self.adapter = build_adapter(settings.datasource, sleeper=self.stop_event.wait)
        self.calendar = TradingCalendar(self.adapter.fetch_trade_calendar, self.db,
                                        allow_weekday_fallback=settings.datasource.calendar_weekday_fallback)
        self.state = RuntimeState()
        self.collector = MarketCollector(settings, self.db, self.adapter, guard, self.state)
        self.indicators = IndicatorService(settings, self.db, guard, self.state)
        self.screening = ScreeningService(settings, self.db, guard, self.state)
        self.anomalies = AnomalyService(settings, self.db, self.state)
        self.cleanup = CleanupService(settings, self.db, guard)
        self.scheduler = Scheduler(self.db, self.calendar, guard, settings.schedule.tick_sec,
                                   settings.schedule.max_backoff_sec, settings.schedule.daily_job_max_attempts)
        self.exporter = WorkDataExporter(settings, self.db, self.state, self.calendar, self.status)
        self.last_ctx: Any = None
        guard.register_cache("runtime_optional_caches", self.state.release_optional_caches)

    # -- 状态文件内容 ---------------------------------------------------------
    def status(self) -> dict[str, Any]:
        ctx = self.last_ctx
        size = self.db.size_bytes()
        return {
            "program": {
                "name": "AStockAI",
                "version": APP_VERSION,
                "pid": os.getpid(),
                "started_at": to_bj_iso(self.state.started_at),
                "python": sys.version.split()[0],
                "platform": platform.platform(),
            },
            "session": None if ctx is None else {
                "phase": ctx.phase.value,
                "calendar_basis": ctx.calendar_basis,
                "today": ctx.today.isoformat(),
                "latest_completed_trading_day": ctx.target_day.isoformat() if ctx.target_day else None,
            },
            "memory": self.guard.status_dict(),
            "database": {
                "path": str(self.db.path),
                "size_mb": round(size / 1e6, 1),
                "max_gb": self.s.storage.db_max_gb,
                "soft_limit_gb": self.s.storage.db_soft_limit_gb,
                "write_paused": self.db.write_paused,
                "pause_reason": self.db.pause_reason,
            },
            "logs_dir_size_mb": round(self._log_dir_size(self.s.logs_path) / 1e6, 2),
            "trade_calendar": self.calendar.status(),
            "data_sources": {k: v.as_dict() for k, v in self.state.health.items()},
            "adapter": self.adapter.status(),
            "jobs": self.scheduler.status(),
            "recent_errors": list(self.state.recent_errors),
            "watchlist": list(self.s.watchlist.codes),
            "latest_screen_run": self.state.latest_screen_run,
            "spot_crosscheck": self.state.crosscheck,
            "notes": [
                "本文件只描述程序运行状态，不包含任何投资建议。",
                "memory.job.hard_limit_active=false 时，内存上限仅由软监测保证，不是硬限制。",
            ],
        }

    # -- 任务注册 -------------------------------------------------------------
    def build_jobs(self) -> None:
        from memory_guard import TaskKind
        from scheduler import Job, JobContext, JobResult
        from trading_calendar import SessionPhase as P

        sc = self.s.schedule
        trading = frozenset({P.MORNING, P.AFTERNOON})
        everywhere = frozenset(P)
        off_session = frozenset({P.PRE_OPEN, P.LUNCH_BREAK, P.POST_CLOSE, P.NON_TRADING_DAY})
        post_start = parse_hhmm(sc.post_close_start)
        cleanup_start, cleanup_end = parse_hhmm(sc.cleanup_start), parse_hhmm(sc.cleanup_end)

        def after_close_ready(ctx: JobContext) -> bool:
            return ctx.phase is not P.POST_CLOSE or ctx.now_bj.time() >= post_start

        def target_key(ctx: JobContext) -> str | None:
            return ctx.target_day.isoformat() if ctx.target_day else None

        def today_key(ctx: JobContext) -> str | None:
            return ctx.today.isoformat()

        def done(fn: Any) -> Any:
            def wrapper(ctx: JobContext) -> JobResult:
                fn(ctx)
                return JobResult.DONE
            return wrapper

        def refresh_calendar(ctx: JobContext) -> JobResult:
            self.calendar.refresh(ctx.today)
            return JobResult.DONE

        E, H, N = TaskKind.ESSENTIAL, TaskKind.HEAVY, TaskKind.NONESSENTIAL
        add = self.scheduler.add
        add(Job("calendar_refresh", refresh_calendar, E, 3600.0, everywhere))
        add(Job("spot", done(self.collector.collect_spot), E, sc.spot_interval_sec, trading))
        add(Job("anomalies", self.anomalies.run, E, 5.0, trading))
        if self.s.watchlist.codes:
            add(Job("watchlist_minutes", self.collector.collect_watchlist_minutes, E,
                    sc.watchlist_interval_sec, trading))
        add(Job("index_spot", done(self.collector.collect_indexes), E, sc.index_interval_sec, trading))
        add(Job("industry_boards", done(self.collector.collect_boards), E, sc.industry_interval_sec, trading))
        if sc.call_auction_snapshot:
            add(Job("spot_auction", done(lambda ctx: self.collector.collect_spot(ctx, purpose="auction")), E,
                    30.0, frozenset({P.PRE_CONTINUOUS}), daily=True, day_key=today_key))
        add(Job("screening_intraday", lambda ctx: self.screening.run(ctx, "intraday"), H,
                sc.screening_interval_sec, trading, skip_retry_sec=60.0))
        add(Job("close_snapshot", self.collector.collect_close, E, 60.0, frozenset({P.POST_CLOSE}),
                eligible=lambda ctx: ctx.now_bj.time() >= time(15, 1), daily=True, day_key=today_key))
        add(Job("export", self.exporter.export, E, sc.export_interval_sec, everywhere))
        add(Job("db_size_watch", self.cleanup.size_watch_job, E, 600.0, everywhere))
        add(Job("cleanup", self.cleanup.scheduled_job, H, 300.0, frozenset({P.POST_CLOSE, P.NON_TRADING_DAY}),
                eligible=lambda ctx: cleanup_start <= ctx.now_bj.time() < cleanup_end,
                daily=True, day_key=today_key))
        add(Job("index_daily", self.collector.update_index_daily, H, 300.0, off_session,
                eligible=after_close_ready, daily=True, day_key=target_key))
        add(Job("daily_bars", self.collector.update_daily_bars, H, 60.0, off_session,
                eligible=after_close_ready, daily=True, day_key=target_key))
        add(Job("indicators", self.indicators.run, H, 120.0, off_session,
                eligible=after_close_ready, daily=True, day_key=target_key))
        add(Job("screening_close", lambda ctx: self.screening.run(ctx, "close"), H, 120.0,
                frozenset({P.POST_CLOSE}), eligible=after_close_ready, daily=True, day_key=target_key))
        add(Job("industry_members", self.collector.refresh_industry_members, N, 120.0, off_session,
                eligible=after_close_ready, daily=True, day_key=today_key))

    # -- 运行 -----------------------------------------------------------------
    def _on_tick(self, ctx: Any) -> None:
        self.last_ctx = ctx
        stop_file = self.s.run_path / STOP_FILE_NAME
        if stop_file.exists():
            logger.info("检测到停止标记文件 %s，准备正常退出", stop_file)
            try:
                stop_file.unlink()
            except OSError:
                logger.warning("无法删除停止标记文件 %s", stop_file)
            self.stop_event.set()

    def run(self) -> str:
        self.calendar.load_cached()
        self.calendar.refresh(datetime.now(tz=BEIJING_TZ).date())
        self.exporter.startup()
        self.build_jobs()
        self.last_ctx = self.scheduler.context()
        self.scheduler.run_forever(self.stop_event, on_tick=self._on_tick)
        return self.scheduler.stop_reason

    def shutdown(self) -> None:
        logger.info("正在安全退出：写出最终状态文件并关闭数据库")
        try:
            ctx = self.scheduler.context()
            self.last_ctx = ctx
            self.exporter.export(ctx, final=True)
        except Exception:
            logger.exception("退出时写出状态文件失败")
        try:
            self.db.close()
        except Exception:
            logger.exception("关闭数据库失败")


def install_signal_handlers(stop_event: threading.Event) -> None:
    def handler(signum: int, _frame: Any) -> None:
        logger.info("收到信号 %s，准备正常退出", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handler)
    if hasattr(signal, "SIGBREAK"):  # Windows: Ctrl+Break
        signal.signal(signal.SIGBREAK, handler)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AStockAI V1.0 A股行情采集与量化数据系统")
    parser.add_argument("--config", default=str(DEFAULT_PROJECT_ROOT / "config" / "settings.yaml"),
                        help="配置文件路径（默认 config/settings.yaml）")
    parser.add_argument("--check-config", action="store_true",
                        help="只校验配置文件并退出，不连接数据源、不创建数据库")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        print(f"[配置错误] {exc}", file=sys.stderr)
        return 2
    if args.check_config:
        print(f"配置文件校验通过: {args.config}")
        print(f"项目根目录: {settings.root_path}")
        return 0

    limit_numeric_threads(settings.memory.numeric_threads)
    settings.ensure_directories()
    setup_logging(settings)
    logger.info("AStockAI %s 启动，项目目录 %s，开始时间 %s", APP_VERSION, settings.root_path,
                to_bj_iso(now_utc()))

    guard = MemoryGuard(settings.memory)
    try:
        guard.install_hard_limit()
    except HardLimitUnavailable as exc:
        logger.critical(str(exc))
        return 3

    lock = InstanceLock(settings.run_path / LOCK_FILE_NAME)
    if not lock.acquire():
        logger.critical("已有一个 AStockAI 实例在运行（锁文件 %s），本次启动退出", lock.path)
        return 4
    stale_stop = settings.run_path / STOP_FILE_NAME
    if stale_stop.exists():
        logger.warning("删除上次遗留的停止标记文件 %s", stale_stop)
        stale_stop.unlink()

    app: App | None = None
    exit_code = 0
    try:
        app = App(settings, guard)
        install_signal_handlers(app.stop_event)
        reason = app.run()
        if guard.shutdown_requested:
            exit_code = 5
        logger.info("调度结束原因: %s", reason)
    except MemoryError:
        logger.critical("初始化或运行中发生 MemoryError，安全退出")
        exit_code = 5
    except KeyboardInterrupt:
        logger.info("收到 KeyboardInterrupt，正常退出")
    except Exception:
        logger.exception("未预期错误导致退出")
        exit_code = 1
    finally:
        if app is not None:
            app.shutdown()
        lock.release()
        logger.info("AStockAI 已退出，退出码 %d", exit_code)
        logging.shutdown()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
