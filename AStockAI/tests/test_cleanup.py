from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta

import pytest

from cleanup import PROTECTED_TABLES, RETENTION_PLAN, CleanupService, StorageBudget, folder_size_bytes
from config import MB, Settings
from database import Database
from memory_guard import MemoryGuard, MemorySample
from scheduler import JobContext
from timeutil import BEIJING_TZ, now_utc, to_db_ts
from trading_calendar import SessionPhase


def _ctx(phase: SessionPhase) -> JobContext:
    now = now_utc()
    return JobContext(now=now, now_bj=now.astimezone(BEIJING_TZ), today=now.astimezone(BEIJING_TZ).date(),
                      phase=phase, calendar_basis="verified", target_day=None)


def _service(settings: Settings, db: Database) -> CleanupService:
    def low_memory() -> MemorySample:  # 固定的低内存读数，保证测试结果与运行环境无关
        return MemorySample(0.0, 100 * MB, 100 * MB, 0, 0, 0)

    return CleanupService(settings, db, MemoryGuard(settings.memory, sampler=low_memory))


def _spot_row(ts: str, code: str) -> tuple:
    return (f"spot-{ts}", code, "n", 1.0, ts, "test", "fetch_time_only", "unknown_time")


SPOT_COLS = ["snapshot_id", "code", "name", "price", "fetched_at", "source", "time_basis", "data_status"]


def test_protected_tables_not_in_plan() -> None:
    assert not any(r.table in PROTECTED_TABLES for r in RETENTION_PLAN)


def test_scheduled_cleanup_deletes_only_expired(settings: Settings, db: Database) -> None:
    old = to_db_ts(now_utc() - timedelta(days=5))
    new = to_db_ts(now_utc() - timedelta(hours=1))
    db.insert_rows("spot_snapshots", SPOT_COLS, [_spot_row(old, "600000"), _spot_row(new, "600000")])
    old_day = (date.today() - timedelta(days=400)).isoformat()
    new_day = (date.today() - timedelta(days=10)).isoformat()
    cols = ["code", "adjust", "trade_date", "close", "fetched_at", "source"]
    db.insert_rows("daily_bars", cols, [("600000", "qfq", old_day, 1.0, new, "t"),
                                        ("600000", "qfq", new_day, 1.0, new, "t")])
    db.execute("INSERT INTO daily_stats(trade_date, computed_at, source, data_status) VALUES (?,?,?,?)",
               ("2000-01-04", old, "t", "historical"))

    result = _service(settings, db).run(_ctx(SessionPhase.POST_CLOSE))
    assert result["status"] == "success"
    assert [r["fetched_at"] for r in db.query("SELECT fetched_at FROM spot_snapshots")] == [new]
    assert [r["trade_date"] for r in db.query("SELECT trade_date FROM daily_bars")] == [new_day]
    # 长期统计数据不受影响
    assert db.query_one("SELECT COUNT(*) AS n FROM daily_stats")["n"] == 1
    assert db.query_one("SELECT COUNT(*) AS n FROM cleanup_runs WHERE status = 'success'")["n"] == 1


def test_refuses_during_trading_hours(settings: Settings, db: Database) -> None:
    old = to_db_ts(now_utc() - timedelta(days=5))
    db.insert_rows("spot_snapshots", SPOT_COLS, [_spot_row(old, "600000")])
    result = _service(settings, db).run(_ctx(SessionPhase.MORNING))
    assert result["status"] == "refused_trading_hours"
    assert db.query_one("SELECT COUNT(*) AS n FROM spot_snapshots")["n"] == 1


def test_emergency_cleanup_only_touches_high_frequency(settings: Settings, db: Database) -> None:
    two_days = to_db_ts(now_utc() - timedelta(days=2))
    db.insert_rows("spot_snapshots", SPOT_COLS, [_spot_row(two_days, "600000")])
    db.execute("INSERT INTO anomalies(dedup_key, trade_date, code, event_type, triggered_at, time_basis, source,"
               " metrics_json, thresholds_json, data_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
               ("k", "2000-01-01", "600000", "X", to_db_ts(now_utc() - timedelta(days=60)), "t", "s", "{}", "{}",
                "unknown_time"))
    result = _service(settings, db).run(_ctx(SessionPhase.AFTERNOON), emergency=True)
    assert result["status"] == "success"
    # 紧急保留 1 天 → 2 天前的快照被删除；异动记录不属于高频数据，不在紧急清理范围
    assert db.query_one("SELECT COUNT(*) AS n FROM spot_snapshots")["n"] == 0
    assert db.query_one("SELECT COUNT(*) AS n FROM anomalies")["n"] == 1


def test_log_cleanup_only_old_log_files(settings: Settings) -> None:
    logs = settings.logs_path
    old_log = logs / "astock.log.2026-01-01"
    new_log = logs / "astock.log"
    other = logs / "notes.txt"
    for p in (old_log, new_log, other):
        p.write_text("x", encoding="utf-8")
    past = time.time() - 30 * 86400
    os.utime(old_log, (past, past))
    os.utime(other, (past, past))
    svc = CleanupService(settings, None, None)  # type: ignore[arg-type]
    assert svc.clean_log_files() == 1
    assert not old_log.exists() and new_log.exists() and other.exists()


def test_integrity_failure_aborts(settings: Settings, db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db, "quick_check", lambda: "*** corrupted")
    old = to_db_ts(now_utc() - timedelta(days=5))
    db.insert_rows("spot_snapshots", SPOT_COLS, [_spot_row(old, "600000")])
    result = _service(settings, db).run(_ctx(SessionPhase.POST_CLOSE))
    assert result["status"] == "aborted_integrity"
    assert db.query_one("SELECT COUNT(*) AS n FROM spot_snapshots")["n"] == 1


def test_cutoff_never_in_future(settings: Settings, db: Database) -> None:
    svc = _service(settings, db)
    for rule in RETENTION_PLAN:
        cutoff, days = svc._cutoff(rule, emergency=False)
        assert days >= 1
        if rule.kind == "ts":
            assert datetime.fromisoformat(cutoff) < now_utc()
        else:
            assert date.fromisoformat(cutoff) < datetime.now(tz=BEIJING_TZ).date()


def test_folder_size_excludes_given_files(tmp_path) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "a.bin").write_bytes(b"x" * 100)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.bin").write_bytes(b"x" * 50)
    assert folder_size_bytes(tmp_path) == 150
    import os
    assert folder_size_bytes(tmp_path, {os.path.normcase(os.path.abspath(sub / "b.bin"))}) == 100


def test_budget_shrinks_db_allowance(settings: Settings, db: Database) -> None:
    budget = StorageBudget(settings, db)
    info = budget.apply(force_scan=True)
    expected = min(int(settings.storage.db_max_gb * 1e9), budget.limit_bytes - info["non_db_bytes"])
    assert db.max_bytes == expected
    assert db.soft_bytes == int(expected * 0.9)
    assert info["total_bytes"] == info["non_db_bytes"] + info["db_bytes"]


def test_budget_trim_deletes_oldest_day_first(settings: Settings, db: Database,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
    today = now_utc()
    recent = to_db_ts(today - timedelta(days=1))
    db.insert_rows("spot_snapshots", SPOT_COLS, [_spot_row(recent, "600000")])
    cols = ["code", "adjust", "trade_date", "close", "fetched_at", "source"]
    db.insert_rows("daily_bars", cols, [("600000", "qfq", "2025-01-02", 1.0, recent, "t"),
                                        ("600000", "qfq", "2025-01-03", 1.0, recent, "t")])
    svc = _service(settings, db)
    answers = iter([True, False])  # 第一次检查超量，删除一天后回到预算内
    monkeypatch.setattr(svc.budget, "needs_trim", lambda: next(answers))
    result = svc.trim_oldest_until_within_budget()
    assert result["removed_days"] == ["2025-01-02"]
    assert [r["trade_date"] for r in db.query("SELECT trade_date FROM daily_bars")] == ["2025-01-03"]
    assert db.query_one("SELECT COUNT(*) AS n FROM spot_snapshots")["n"] == 1
    assert db.query_one("SELECT COUNT(*) AS n FROM cleanup_runs WHERE status LIKE 'budget_trim:%'")["n"] == 1


def test_budget_trim_never_deletes_today(settings: Settings, db: Database,
                                         monkeypatch: pytest.MonkeyPatch) -> None:
    now = to_db_ts(now_utc())
    db.insert_rows("spot_snapshots", SPOT_COLS, [_spot_row(now, "600000")])
    svc = _service(settings, db)
    monkeypatch.setattr(svc.budget, "needs_trim", lambda: True)
    result = svc.trim_oldest_until_within_budget()
    assert result["status"] == "nothing_left_to_delete"
    assert db.query_one("SELECT COUNT(*) AS n FROM spot_snapshots")["n"] == 1
