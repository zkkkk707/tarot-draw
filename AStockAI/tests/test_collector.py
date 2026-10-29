from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from conftest import FakeAdapter, bj, make_daily_bars, make_raw_spot, make_spot_result

from collector import MarketCollector, compute_breadth, spot_signature
from config import MB, Settings
from database import Database
from market_data import DataStatus, DataValidationError, normalize_spot
from memory_guard import MemoryGuard, MemorySample
from scheduler import JobContext, JobResult
from state import RuntimeState
from timeutil import UTC
from trading_calendar import SessionPhase


def _low_memory() -> MemorySample:
    return MemorySample(0.0, 100 * MB, 100 * MB, 0, 0, 0)


def _collector(settings: Settings, db: Database, adapter: FakeAdapter) -> tuple[MarketCollector, RuntimeState]:
    state = RuntimeState()
    guard = MemoryGuard(settings.memory, sampler=_low_memory)
    return MarketCollector(settings, db, adapter, guard, state), state


def _ctx(now_bj: datetime, phase: SessionPhase, target: date | None = None) -> JobContext:
    return JobContext(now=now_bj.astimezone(UTC), now_bj=now_bj, today=now_bj.date(), phase=phase,
                      calendar_basis="verified", target_day=target)


def test_compute_breadth_counts_and_limit_approx() -> None:
    raw = make_raw_spot(4)
    raw["代码"] = ["600000", "300001", "600002", "600003"]
    raw["昨收"] = [10.0, 10.0, 10.0, 10.0]
    raw["最新价"] = [11.0, 12.0, 9.5, np.nan]       # 主板涨停、创业板涨停、下跌、停牌
    raw["最高"] = [11.0, 12.0, 10.1, np.nan]
    raw["最低"] = [10.0, 10.0, 9.4, np.nan]
    raw["涨跌幅"] = [10.0, 20.0, -5.0, np.nan]
    df, _ = normalize_spot(raw)
    b = compute_breadth(df)
    assert b["total"] == 4 and b["traded"] == 3 and b["suspended"] == 1
    assert b["up"] == 2 and b["down"] == 1
    assert b["limit_up_approx"] == 2 and b["limit_down_approx"] == 0
    assert b["advance_ratio"] == pytest.approx(2 / 3)


def test_spot_signature_detects_change() -> None:
    a, _ = normalize_spot(make_raw_spot(3))
    b = a.copy()
    assert spot_signature(a) == spot_signature(b)
    b.loc[0, "price"] = b.loc[0, "price"] + 0.01
    assert spot_signature(a) != spot_signature(b)


def test_collect_spot_marks_stale_when_unchanged_in_session(settings: Settings, db: Database) -> None:
    adapter = FakeAdapter()
    t0 = bj(2026, 10, 29, 10, 0).astimezone(UTC)
    raw = make_raw_spot(5)
    adapter.spot_queue = [make_spot_result(raw, fetched_at=t0),
                          make_spot_result(raw, fetched_at=t0 + timedelta(seconds=200))]
    col, state = _collector(settings, db, adapter)
    ctx = _ctx(bj(2026, 10, 29, 10, 0), SessionPhase.MORNING)
    first = col.collect_spot(ctx)
    assert first.data_status == DataStatus.UNKNOWN_TIME
    second = col.collect_spot(ctx)
    assert second.data_status == DataStatus.STALE
    assert state.latest_spot is second
    # 首次全量入库 5 行，第二次只写入自选股（600000）
    assert db.query_one("SELECT COUNT(*) AS n FROM spot_snapshots")["n"] == 6
    statuses = {r["data_status"] for r in db.query("SELECT data_status FROM spot_snapshots")}
    assert statuses == {DataStatus.UNKNOWN_TIME, DataStatus.STALE}
    assert db.query_one("SELECT COUNT(*) AS n FROM stocks")["n"] == 5
    assert db.query_one("SELECT COUNT(*) AS n FROM breadth_snapshots")["n"] == 2


def test_collect_spot_invalid_is_recorded_and_not_used(settings: Settings, db: Database) -> None:
    adapter = FakeAdapter()
    adapter.spot_queue = [make_spot_result(status=DataStatus.INVALID)]
    col, state = _collector(settings, db, adapter)
    with pytest.raises(DataValidationError):
        col.collect_spot(_ctx(bj(2026, 10, 29, 10, 0), SessionPhase.MORNING))
    assert state.latest_spot is None
    row = db.query_one("SELECT status, error_type FROM collection_runs WHERE dataset = 'spot'")
    assert row["status"] == "failed" and row["error_type"] == "DataValidationError"
    assert db.query_one("SELECT COUNT(*) AS n FROM spot_snapshots")["n"] == 0


def test_daily_update_batches_and_bounded_retries(settings: Settings, db: Database) -> None:
    adapter = FakeAdapter()
    bars_a = make_daily_bars("600000", 60, seed=1)
    bars_b = make_daily_bars("600001", 60, seed=2)
    adapter.daily = {"600000": bars_a, "600001": bars_b}
    target = date.fromisoformat(bars_a["trade_date"].iloc[-1])
    settings.schedule.daily_retry_delay_sec = 0.0  # 测试中不等待重试间隔
    col, state = _collector(settings, db, adapter)
    # 收盘后快照：600000~600004 当日均有成交
    close_time = datetime(target.year, target.month, target.day, 15, 5, tzinfo=bj(2026, 1, 1).tzinfo)
    state.latest_spot = make_spot_result(fetched_at=close_time.astimezone(UTC))
    ctx = _ctx(close_time.replace(hour=16), SessionPhase.POST_CLOSE, target)

    assert col.update_daily_bars(ctx) is JobResult.MORE
    status = {r["code"]: r["status"] for r in db.query("SELECT code, status FROM daily_update_progress")}
    assert status["600000"] == "done" and status["600001"] == "done"
    assert status["000001"] == "done_empty"  # 自选股中无成交记录的代码，不伪造K线
    assert status["600002"] == "retry"       # 当日有成交却拿不到日K → 重试，而不是标记完成
    assert db.query_one("SELECT COUNT(*) AS n FROM daily_bars WHERE code = '600000'")["n"] == 60

    assert col.update_daily_bars(ctx) is JobResult.MORE
    assert col.update_daily_bars(ctx) is JobResult.MORE
    assert col.update_daily_bars(ctx) is JobResult.DONE
    failed = db.query_one("SELECT attempts, status FROM daily_update_progress WHERE code = '600002'")
    assert failed["status"] == "failed" and failed["attempts"] == 3


def test_adjustment_change_triggers_full_refetch(settings: Settings, db: Database) -> None:
    adapter = FakeAdapter()
    bars = make_daily_bars("600000", 60, seed=5)
    adapter.daily = {"600000": bars}
    target = date.fromisoformat(bars["trade_date"].iloc[-1])
    # 库中已有前50根，但复权价与数据源当前返回值相差10%（模拟除权除息后前复权价格整体变化）
    old = bars.iloc[:50].copy()
    old["close"] = old["close"] * 1.1
    old["adjust"] = "qfq"
    old["fetched_at"] = "2026-01-01T00:00:00+00:00"
    old["source"] = "old"
    db.insert_df("daily_bars", old, ["code", "adjust", "trade_date", "open", "high", "low", "close",
                                     "volume_lots", "fetched_at", "source"])
    col, _ = _collector(settings, db, adapter)
    status, err = col._update_one_daily("600000", target, None)
    assert status == "done" and err is None
    stored = db.query_df("SELECT trade_date, close, source FROM daily_bars WHERE code = '600000' ORDER BY trade_date")
    assert len(stored) == 60
    assert np.allclose(stored["close"].to_numpy(), bars["close"].to_numpy())
    assert (stored["source"] == "test:fake_daily").all()
    # 先增量请求，发现不一致后再请求完整历史
    assert [c[0] for c in adapter.calls].count("daily") == 2


def test_retry_waits_for_delay(settings: Settings, db: Database) -> None:
    adapter = FakeAdapter()
    adapter.daily = {"600000": make_daily_bars("600000", 30, seed=1)}
    target = date.fromisoformat(adapter.daily["600000"]["trade_date"].iloc[-1])
    col, state = _collector(settings, db, adapter)  # 默认重试间隔 300 秒
    close_time = datetime(target.year, target.month, target.day, 15, 5, tzinfo=bj(2026, 1, 1).tzinfo)
    state.latest_spot = make_spot_result(fetched_at=close_time.astimezone(UTC))
    ctx = _ctx(close_time.replace(hour=16), SessionPhase.POST_CLOSE, target)
    assert col.update_daily_bars(ctx) is JobResult.MORE
    # 失败的股票尚未到重试时间：不立即再请求，返回 SKIPPED 而不是 DONE
    n_calls = len(adapter.calls)
    assert col.update_daily_bars(ctx) is JobResult.SKIPPED
    assert len(adapter.calls) == n_calls


def test_up_to_date_code_makes_no_request(settings: Settings, db: Database) -> None:
    adapter = FakeAdapter()
    bars = make_daily_bars("600000", 5)
    bars["adjust"] = "qfq"
    bars["fetched_at"] = "2026-01-01T00:00:00+00:00"
    bars["source"] = "t"
    db.insert_df("daily_bars", bars, ["code", "adjust", "trade_date", "close", "fetched_at", "source"])
    col, _ = _collector(settings, db, adapter)
    target = date.fromisoformat(bars["trade_date"].iloc[-1])
    assert col._update_one_daily("600000", target, None) == ("done", None)
    assert adapter.calls == []


def test_breadth_counts_ignore_missing_values() -> None:
    df = pd.DataFrame({"code": [], "name": [], "price": [], "pct_change": [], "volume_lots": [],
                       "prev_close": [], "amount_yuan": []})
    assert compute_breadth(df)["advance_ratio"] is None
