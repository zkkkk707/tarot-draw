"""数据库与日志自动清理（只在用户手动启动程序后、由调度器在北京时间 15:30 之后运行）。

保留规则（settings.yaml 的 storage 段可调整）
| 数据                         | 表                                            | 默认保留 |
|------------------------------|-----------------------------------------------|----------|
| 实时行情快照                 | spot_snapshots / index_snapshots /            | 3 天     |
|                              | industry_snapshots / breadth_snapshots        |          |
| 分钟K线                      | minute_bars                                   | 7 天     |
| 日K线（含指数日K、指标）     | daily_bars / index_daily / indicator_values   | 365 天   |
| 异动记录                     | anomalies                                     | 30 天    |
| 筛选结果                     | screen_results / screen_runs                  | 90 天    |
| 程序日志                     | logs/*.log*                                   | 7 天     |
| 运行记录、进度、行业成分等   | collection_runs / daily_update_progress /     | 30 天    |
|                              | job_state / industry_members                  |          |
| 长期统计（不自动删除）       | daily_stats / cleanup_runs / trade_calendar / | 永久     |
|                              | stocks / meta                                 |          |

保护措施
1. 交易时段（09:15–15:00）拒绝执行常规清理；仅“容量紧急清理”可在盘中对高频快照做小批量删除。
2. 清理前执行 PRAGMA quick_check，数据库异常时中止，不做任何删除。
3. 删除条件全部参数化；截止时间按“当前时间 - 保留天数”计算，保留天数最小为 1 天。
4. 受保护表不在删除计划中，代码层面也会拒绝。
5. 分批删除（每批 cleanup_batch_rows 行），每批独立事务，避免长时间锁库。
6. 结果写入 cleanup_runs 表（长期保存）与日志。

空间回收：WAL checkpoint(TRUNCATE) → PRAGMA incremental_vacuum；空闲页比例仍很高时，
在非交易时段执行一次全量 VACUUM（需要约等于数据库大小的临时磁盘空间）。

文件夹总容量预算（StorageBudget，默认 2GB = 2,000,000,000 字节，含 .venv、数据库、日志、导出文件）
- 数据库实际允许大小 = min(db_max_gb, 预算 - 安全余量 - 其它文件大小)，每 60 秒检查一次；
  达到该值即暂停行情写入，回落到其 90% 以下才恢复。
- 一旦超出，按“从最旧开始”自动删除：比较最旧的日志文件与数据库中最旧的一天，删除更旧的那一份；
  数据库每删一天就回收空间并重新测量，直到数据库回到允许大小的 90% 以下且总量在预算内。
- 不删除当天（UTC 日期）的数据；不删除受保护的长期小表（每日统计、清理记录、交易日历、股票列表）；
  不删除正在写入的当前日志文件。全部可删内容删完仍超标（例如 .venv 本身过大）时，保持暂停写入并告警。
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from config import GB, MB, Settings
from database import Database
from memory_guard import MemoryGuard, TaskKind
from timeutil import UTC, date_cutoff_days, now_utc, to_db_ts, utc_cutoff_days
from trading_calendar import SessionPhase

if TYPE_CHECKING:
    from scheduler import JobContext, JobResult

logger = logging.getLogger(__name__)

PROTECTED_TABLES = frozenset({"daily_stats", "cleanup_runs", "trade_calendar", "stocks", "meta"})
# 盘中不允许常规清理的时段
BUSY_PHASES = frozenset({SessionPhase.CALL_AUCTION, SessionPhase.PRE_CONTINUOUS,
                         SessionPhase.MORNING, SessionPhase.AFTERNOON})


@dataclass(frozen=True)
class RetentionRule:
    table: str
    column: str
    kind: str              # "ts"（UTC 时间戳）或 "date"（YYYY-MM-DD 交易日期）
    days_attr: str         # StorageConfig 中的保留天数字段
    emergency_attr: str | None = None  # 容量紧急时使用的更短保留天数（仅高频数据）


RETENTION_PLAN: tuple[RetentionRule, ...] = (
    RetentionRule("spot_snapshots", "fetched_at", "ts", "retention_spot_days", "emergency_spot_days"),
    RetentionRule("index_snapshots", "fetched_at", "ts", "retention_spot_days", "emergency_spot_days"),
    RetentionRule("industry_snapshots", "fetched_at", "ts", "retention_spot_days", "emergency_spot_days"),
    RetentionRule("breadth_snapshots", "fetched_at", "ts", "retention_spot_days", "emergency_spot_days"),
    RetentionRule("minute_bars", "bar_time", "ts", "retention_minute_days", "emergency_minute_days"),
    RetentionRule("daily_bars", "trade_date", "date", "retention_daily_days"),
    RetentionRule("index_daily", "trade_date", "date", "retention_daily_days"),
    RetentionRule("indicator_values", "trade_date", "date", "retention_daily_days"),
    RetentionRule("anomalies", "triggered_at", "ts", "retention_anomaly_days"),
    RetentionRule("screen_results", "run_at", "ts", "retention_screen_days"),
    RetentionRule("screen_runs", "run_at", "ts", "retention_screen_days"),
    RetentionRule("collection_runs", "started_at", "ts", "retention_runs_days"),
    RetentionRule("daily_update_progress", "updated_at", "ts", "retention_runs_days"),
    RetentionRule("industry_members", "updated_at", "ts", "retention_runs_days"),
    RetentionRule("job_state", "updated_at", "ts", "retention_runs_days"),
)

assert not any(r.table in PROTECTED_TABLES for r in RETENTION_PLAN), "受保护表不得出现在清理计划中"


MAX_TRIM_STEPS = 1000  # 单次超量处理最多删除的“天数”步数，防止无限循环


def folder_size_bytes(root: Path, exclude: set[str] | None = None) -> int:
    """递归统计文件夹内全部普通文件大小（不跟随符号链接）。exclude 为需排除文件的规范化绝对路径。"""
    excluded = exclude or set()
    total = 0
    stack = [str(root)]
    while stack:
        current = stack.pop()
        try:
            entries = os.scandir(current)
        except OSError:
            continue
        with entries:
            for entry in entries:
                try:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        if _norm(entry.path) in excluded:
                            continue
                        total += entry.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
    return total


def _norm(path: str | Path) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


class StorageBudget:
    """项目文件夹总容量预算：测量、换算数据库允许大小、驱动写入暂停。"""

    def __init__(self, settings: Settings, db: Database,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.s = settings
        self.db = db
        self._clock = clock
        st = settings.storage
        self.budget_bytes = int(st.folder_budget_gb * GB)
        self.margin_bytes = int(st.folder_safety_margin_mb * MB)
        self._non_db: int | None = None
        self._scanned_at = -1e18
        self.last: dict[str, Any] = {}

    def _db_files(self) -> set[str]:
        base = str(self.db.path)
        return {_norm(base), _norm(base + "-wal"), _norm(base + "-shm"), _norm(base + "-journal")}

    def invalidate(self) -> None:
        self._non_db = None

    def non_db_bytes(self, force: bool = False) -> int:
        if (force or self._non_db is None
                or self._clock() - self._scanned_at >= self.s.storage.folder_scan_interval_sec):
            self._non_db = folder_size_bytes(self.s.root_path, self._db_files())
            self._scanned_at = self._clock()
        return self._non_db

    @property
    def limit_bytes(self) -> int:
        """总量必须保持在该值以下（预算 - 安全余量）。"""
        return self.budget_bytes - self.margin_bytes

    def apply(self, force_scan: bool = False) -> dict[str, Any]:
        non_db = self.non_db_bytes(force_scan)
        allowed = min(int(self.s.storage.db_max_gb * GB), self.limit_bytes - non_db)
        allowed = max(allowed, 0)
        self.db.max_bytes = allowed
        self.db.soft_bytes = int(allowed * 0.9)
        db_bytes = self.db.refresh_space_state()
        total = non_db + db_bytes
        self.last = {
            "folder": str(self.s.root_path),
            "budget_bytes": self.budget_bytes,
            "safety_margin_bytes": self.margin_bytes,
            "non_db_bytes": non_db,
            "db_bytes": db_bytes,
            "total_bytes": total,
            "db_allowed_bytes": allowed,
            "total_mb": round(total / MB, 1),
            "budget_mb": round(self.budget_bytes / MB, 1),
            "over_limit": total >= self.limit_bytes or db_bytes >= allowed,
            "write_paused": self.db.write_paused,
        }
        return self.last

    def needs_trim(self) -> bool:
        info = self.last
        return bool(info) and (info["total_bytes"] >= self.limit_bytes
                               or info["db_bytes"] >= self.db.soft_bytes)


class CleanupService:
    def __init__(self, settings: Settings, db: Database, guard: MemoryGuard,
                 budget: StorageBudget | None = None) -> None:
        self.s = settings
        self.db = db
        self.guard = guard
        self.budget = budget if budget is not None else (StorageBudget(settings, db) if db is not None else None)
        self.last_trim: dict[str, Any] | None = None
        self._integrity_checked_at = -1e18
        self._integrity_ok = True

    # -- 截止条件 -------------------------------------------------------------
    def _cutoff(self, rule: RetentionRule, emergency: bool) -> tuple[str, int]:
        attr = rule.emergency_attr if (emergency and rule.emergency_attr) else rule.days_attr
        days = int(getattr(self.s.storage, attr))
        if days < 1:
            raise ValueError(f"{rule.table} 的保留天数非法: {days}")
        cutoff = utc_cutoff_days(days) if rule.kind == "ts" else date_cutoff_days(days)
        return cutoff, days

    def _delete_rule(self, rule: RetentionRule, emergency: bool) -> dict[str, Any]:
        if rule.table in PROTECTED_TABLES:
            raise RuntimeError(f"拒绝清理受保护表 {rule.table}")
        cutoff, days = self._cutoff(rule, emergency)
        deleted = self.db.delete_in_batches(rule.table, f"{rule.column} < ?", (cutoff,),
                                            self.s.storage.cleanup_batch_rows)
        return {"deleted": deleted, "cutoff": cutoff, "retention_days": days}

    # -- 日志与临时文件 -------------------------------------------------------
    def clean_log_files(self) -> int:
        logs_dir = self.s.logs_path
        if not logs_dir.is_dir():
            return 0
        limit = time.time() - self.s.storage.retention_log_days * 86400
        removed = 0
        for p in logs_dir.iterdir():
            # 只处理日志目录下的普通文件，不跟随符号链接，不递归
            if not p.is_file() or p.is_symlink() or ".log" not in p.name:
                continue
            try:
                if p.stat().st_mtime < limit:
                    p.unlink()
                    removed += 1
            except OSError as exc:
                logger.warning("删除过期日志 %s 失败: %s", p, exc)
        return removed

    def clean_stale_temp_files(self) -> int:
        """删除 work_data 中残留的临时文件（异常退出时可能遗留），只删除本程序命名格式的文件。"""
        wd = self.s.work_data_path
        if not wd.is_dir():
            return 0
        limit = time.time() - 3600
        removed = 0
        for p in wd.iterdir():
            if p.is_file() and p.name.startswith(".") and p.name.endswith(".tmp"):
                try:
                    if p.stat().st_mtime < limit:
                        p.unlink()
                        removed += 1
                except OSError as exc:
                    logger.warning("删除临时文件 %s 失败: %s", p, exc)
        return removed

    # -- 主流程 ---------------------------------------------------------------
    def run(self, ctx: "JobContext", emergency: bool = False) -> dict[str, Any]:
        """执行一次清理。emergency=True 时只清理高频数据（使用紧急保留天数）。"""
        started = to_db_ts(now_utc())
        if ctx.phase in BUSY_PHASES and not emergency:
            logger.warning("交易时段内拒绝执行常规清理（%s）", ctx.phase.value)
            return {"status": "refused_trading_hours"}
        size_before = self.db.size_bytes()
        integrity = self.db.quick_check()
        if integrity != "ok":
            logger.critical("数据库完整性检查未通过（%s），中止清理且不删除任何数据", integrity)
            self._record(started, "aborted_integrity", size_before, size_before, {}, integrity)
            return {"status": "aborted_integrity", "detail": integrity}

        deleted: dict[str, Any] = {}
        rules = [r for r in RETENTION_PLAN if not emergency or r.emergency_attr]
        status = "success"
        try:
            for rule in rules:
                self.guard.checkpoint(TaskKind.HEAVY if not emergency else TaskKind.ESSENTIAL,
                                      f"cleanup:{rule.table}")
                deleted[rule.table] = self._delete_rule(rule, emergency)
            if not emergency:
                deleted["log_files"] = {"deleted": self.clean_log_files()}
                deleted["temp_files"] = {"deleted": self.clean_stale_temp_files()}
            self._reclaim(ctx, allow_full_vacuum=not emergency and ctx.phase not in BUSY_PHASES)
        except Exception as exc:
            status = f"failed:{type(exc).__name__}"
            logger.exception("清理过程中出错，已完成部分保留")
            size_after = self.db.size_bytes()
            self._record(started, status, size_before, size_after, deleted, str(exc)[:500])
            raise
        size_after = self.db.refresh_space_state()
        self._record(started, status, size_before, size_after, deleted,
                     "emergency" if emergency else "scheduled")
        logger.info("清理完成（%s）：数据库 %.1f MB → %.1f MB，明细 %s", "紧急" if emergency else "常规",
                    size_before / 1e6, size_after / 1e6,
                    {k: v.get("deleted") for k, v in deleted.items()})
        return {"status": status, "size_before": size_before, "size_after": size_after, "deleted": deleted}

    def _reclaim(self, ctx: "JobContext", allow_full_vacuum: bool) -> None:
        self.db.checkpoint()
        stats = self.db.page_stats()
        if stats["auto_vacuum"] == 2 and stats["freelist_count"] > 0:  # 2 = INCREMENTAL
            self.db.incremental_vacuum(self.s.storage.incremental_vacuum_pages)
            stats = self.db.page_stats()
        ratio = stats["freelist_count"] / stats["page_count"] if stats["page_count"] else 0.0
        if allow_full_vacuum and ratio >= self.s.storage.full_vacuum_freelist_ratio:
            free_disk = _free_disk_bytes(self.db.path.parent)
            if free_disk is not None and free_disk < self.db.size_bytes() * 1.2:
                logger.warning("磁盘剩余空间不足以安全执行 VACUUM（剩余 %.1f MB），跳过", free_disk / 1e6)
                return
            logger.info("空闲页比例 %.0f%%，执行全量 VACUUM", ratio * 100)
            self.db.full_vacuum()
            self.db.checkpoint()

    def _record(self, started: str, status: str, before: int, after: int, deleted: dict[str, Any],
                note: str) -> None:
        try:
            self.db.execute(
                "INSERT INTO cleanup_runs(started_at, finished_at, status, db_size_before, db_size_after,"
                " deleted_json, note) VALUES (?,?,?,?,?,?,?)",
                (started, to_db_ts(now_utc()), status, before, after,
                 json.dumps(deleted, ensure_ascii=False), note))
        except Exception:
            logger.exception("写入清理记录失败")

    # -- 调度入口 -------------------------------------------------------------
    def scheduled_job(self, ctx: "JobContext") -> "JobResult":
        from scheduler import JobResult

        result = self.run(ctx, emergency=False)
        if result.get("status") != "success":
            raise RuntimeError(f"常规清理未完成: {result.get('status')}")
        return JobResult.DONE

    def size_watch_job(self, ctx: "JobContext") -> "JobResult":
        """每 60 秒检查文件夹总容量；超出预算时从最旧的内容开始自动删除。"""
        from scheduler import JobResult

        if self.budget is None:
            return JobResult.SKIPPED
        self.budget.apply()
        if not self.budget.needs_trim():
            return JobResult.DONE
        info = self.budget.last
        logger.warning("容量超出预算：文件夹合计 %.1f MB（上限 %.1f MB，含余量），数据库 %.1f MB / 允许 %.1f MB，"
                       "开始从最旧数据删除", info["total_bytes"] / MB, self.budget.limit_bytes / MB,
                       info["db_bytes"] / MB, info["db_allowed_bytes"] / MB)
        self.trim_oldest_until_within_budget()
        return JobResult.DONE

    # -- 超预算时：从最旧开始删除 -------------------------------------------------
    def _integrity_ok_cached(self) -> bool:
        """完整性检查较慢，超量处理时最多每 6 小时做一次。"""
        if time.monotonic() - self._integrity_checked_at >= 6 * 3600:
            result = self.db.quick_check()
            self._integrity_ok = result == "ok"
            self._integrity_checked_at = time.monotonic()
            if not self._integrity_ok:
                logger.critical("数据库完整性检查未通过（%s），不执行自动删除，保持写入暂停", result)
        return self._integrity_ok

    def _oldest_db_day(self) -> str | None:
        """所有可删除表中最旧数据所在的日期（时间戳列按 UTC 日期，交易日期列直接取值）。"""
        oldest: str | None = None
        for rule in RETENTION_PLAN:
            row = self.db.query_one(f"SELECT MIN({rule.column}) AS m FROM {rule.table}")
            value = None if row is None else row["m"]
            if not value:
                continue
            day = str(value)[:10]
            if oldest is None or day < oldest:
                oldest = day
        return oldest

    def _oldest_log_file(self) -> tuple[Path, str] | None:
        logs_dir = self.s.logs_path
        if not logs_dir.is_dir():
            return None
        active = self.s.logging.file_name
        candidates: list[tuple[float, Path]] = []
        for p in logs_dir.iterdir():
            if p.is_file() and not p.is_symlink() and ".log" in p.name and p.name != active:
                try:
                    candidates.append((p.stat().st_mtime, p))
                except OSError:
                    continue
        if not candidates:
            return None
        mtime, path = min(candidates)
        return path, datetime.fromtimestamp(mtime, tz=UTC).date().isoformat()

    def _delete_db_day(self, day: str) -> int:
        next_midnight = to_db_ts(datetime.combine(date.fromisoformat(day) + timedelta(days=1),
                                                  dtime(0, 0), tzinfo=UTC))
        deleted = 0
        for rule in RETENTION_PLAN:
            if rule.table in PROTECTED_TABLES:
                raise RuntimeError(f"拒绝清理受保护表 {rule.table}")
            if rule.kind == "ts":
                deleted += self.db.delete_in_batches(rule.table, f"{rule.column} < ?", (next_midnight,),
                                                     self.s.storage.cleanup_batch_rows)
            else:
                deleted += self.db.delete_in_batches(rule.table, f"{rule.column} <= ?", (day,),
                                                     self.s.storage.cleanup_batch_rows)
        return deleted

    def _reclaim_all(self) -> None:
        self.db.checkpoint()
        if self.db.page_stats()["auto_vacuum"] == 2:
            self.db.incremental_vacuum(0)  # 回收全部空闲页
        else:
            self.db.full_vacuum()  # 旧库未启用增量回收时只能全量 VACUUM
        self.db.checkpoint()

    def trim_oldest_until_within_budget(self) -> dict[str, Any]:
        assert self.budget is not None
        started = to_db_ts(now_utc())
        before = self.budget.last.get("total_bytes", 0)
        db_before = self.db.size_bytes()
        removed_days: list[str] = []
        removed_logs: list[str] = []
        rows = 0
        status = "within_budget"
        today_utc = now_utc().date().isoformat()
        if not self._integrity_ok_cached():
            status = "aborted_integrity"
        else:
            for _ in range(MAX_TRIM_STEPS):
                info = self.budget.apply()
                if not self.budget.needs_trim():
                    status = "within_budget"
                    break
                db_day = self._oldest_db_day()
                log = self._oldest_log_file()
                db_day = db_day if (db_day is not None and db_day < today_utc) else None
                if log is not None and (db_day is None or log[1] <= db_day):
                    path, _day = log
                    try:
                        path.unlink()
                        removed_logs.append(path.name)
                    except OSError as exc:
                        logger.warning("删除旧日志 %s 失败: %s", path, exc)
                        status = "log_delete_failed"
                        break
                    self.budget.invalidate()
                    continue
                if db_day is None:
                    status = "nothing_left_to_delete"
                    logger.critical("可删除的历史数据已全部删除，总量仍为 %.1f MB，超过上限 %.1f MB；"
                                    "保持暂停写入。请检查 .venv 等非数据文件是否过大",
                                    info["total_bytes"] / MB, self.budget.limit_bytes / MB)
                    break
                rows += self._delete_db_day(db_day)
                removed_days.append(db_day)
                self._reclaim_all()
            else:
                status = "step_limit_reached"
        after = self.budget.apply()
        self.last_trim = {
            "at": started, "status": status, "removed_days": removed_days[:50],
            "removed_day_count": len(removed_days), "removed_rows": rows, "removed_logs": removed_logs[:50],
            "total_bytes_before": before, "total_bytes_after": after["total_bytes"],
        }
        self._record(started, f"budget_trim:{status}", db_before, after["db_bytes"],
                     {"removed_days": removed_days, "removed_rows": rows, "removed_logs": removed_logs},
                     json.dumps({"total_before": before, "total_after": after["total_bytes"],
                                 "limit": self.budget.limit_bytes}))
        if removed_days or removed_logs:
            logger.warning("容量预算处理完成（%s）：删除最旧 %d 天数据（%s ~ %s）共 %d 行、%d 个旧日志；"
                           "文件夹合计 %.1f MB → %.1f MB", status, len(removed_days),
                           removed_days[0] if removed_days else "-", removed_days[-1] if removed_days else "-",
                           rows, len(removed_logs), before / MB, after["total_bytes"] / MB)
        return self.last_trim


def _free_disk_bytes(path: Path) -> int | None:
    try:
        import shutil

        return int(shutil.disk_usage(str(path)).free)
    except OSError:
        return None


def log_dir_size(path: Path) -> int:
    total = 0
    if path.is_dir():
        for p in path.iterdir():
            try:
                if p.is_file():
                    total += os.path.getsize(p)
            except OSError:
                continue
    return total
