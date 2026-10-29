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
容量控制：数据库（含 -wal/-shm）≥ db_soft_limit_gb 时先按“紧急保留天数”清理高频数据；
仍 ≥ db_max_gb 时暂停行情写入（不会删除日K、统计等重要数据）。
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from config import GB, Settings
from database import Database
from memory_guard import MemoryGuard, TaskKind
from timeutil import date_cutoff_days, now_utc, to_db_ts, utc_cutoff_days
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


class CleanupService:
    def __init__(self, settings: Settings, db: Database, guard: MemoryGuard) -> None:
        self.s = settings
        self.db = db
        self.guard = guard

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
        """周期检查数据库容量；超过软上限时执行紧急清理，仍超过硬上限则保持写入暂停。"""
        from scheduler import JobResult

        size = self.db.refresh_space_state()
        soft = int(self.s.storage.db_soft_limit_gb * GB)
        if size < soft:
            return JobResult.DONE
        logger.warning("数据库大小 %.2f GB 超过软上限 %.2f GB，执行紧急清理", size / GB,
                       self.s.storage.db_soft_limit_gb)
        self.run(ctx, emergency=True)
        size = self.db.refresh_space_state()
        if self.db.write_paused:
            logger.critical("紧急清理后数据库仍为 %.2f GB，继续暂停行情写入；重要数据不会被自动删除，"
                            "请检查保留天数配置或手动归档", size / GB)
        return JobResult.DONE


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
