from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from database import Database, WritePausedError


def test_schema_created_with_incremental_auto_vacuum(db: Database) -> None:
    stats = db.page_stats()
    assert stats["auto_vacuum"] == 2  # INCREMENTAL
    tables = {r["name"] for r in db.query("SELECT name FROM sqlite_master WHERE type = 'table'")}
    for t in ("spot_snapshots", "daily_bars", "anomalies", "screen_results", "daily_stats", "cleanup_runs"):
        assert t in tables
    assert db.quick_check() == "ok"


def test_insert_df_writes_null_for_nan(db: Database) -> None:
    df = pd.DataFrame({"code": ["600000"], "adjust": ["qfq"], "trade_date": ["2026-10-28"],
                       "open": [np.nan], "high": [np.float32(10.5)], "low": [9.5], "close": [10.0],
                       "volume_lots": [np.int64(100)], "amount_yuan": [None], "amplitude_pct": [1.0],
                       "pct_change": [0.5], "change": [0.05], "turnover_rate": [np.nan],
                       "fetched_at": ["2026-10-28T08:00:00+00:00"], "source": ["test"]})
    cols = list(df.columns)
    assert db.insert_df("daily_bars", df, cols) == 1
    row = db.query_one("SELECT open, amount_yuan, turnover_rate, volume_lots FROM daily_bars")
    assert row["open"] is None and row["amount_yuan"] is None and row["turnover_rate"] is None
    assert row["volume_lots"] == 100


def test_unknown_table_and_bad_identifier_rejected(db: Database) -> None:
    with pytest.raises(ValueError):
        db.insert_rows("not_a_table", ["a"], [[1]])
    with pytest.raises(ValueError):
        db.insert_rows("meta", ["key; DROP TABLE meta"], [["x"]])


def test_write_pause_blocks_market_tables_but_not_control(db: Database) -> None:
    db.write_paused = True
    db.pause_reason = "test"
    with pytest.raises(WritePausedError):
        db.insert_rows("anomalies", ["dedup_key"], [["x"]])
    db.set_state("k", "v")  # 控制表允许写入
    assert db.get_state("k") == "v"
    db.record_run("spot", "test", "2026-10-28T00:00:00+00:00", "failed")  # 降级为日志，不抛异常
    assert db.query_one("SELECT COUNT(*) AS n FROM collection_runs")["n"] == 0


def test_delete_in_batches_parameterized(db: Database) -> None:
    rows = [(f"2026-10-{d:02d}T00:00:00+00:00", "ds", "src", f"2026-10-{d:02d}T00:00:00+00:00", "success")
            for d in range(1, 29)]
    db.insert_rows("collection_runs", ["started_at", "dataset", "source", "finished_at", "status"], rows)
    n = db.delete_in_batches("collection_runs", "started_at < ?", ("2026-10-15T00:00:00+00:00",), batch_rows=3)
    assert n == 14
    assert db.query_one("SELECT MIN(started_at) AS m FROM collection_runs")["m"] == "2026-10-15T00:00:00+00:00"
    with pytest.raises(ValueError):
        db.delete_in_batches("collection_runs", "1 = 1", (), batch_rows=3)


def test_replace_partition_is_atomic(db: Database) -> None:
    cols = ["board_code", "board_name", "code", "name", "updated_at", "source"]
    db.insert_rows("industry_members", cols, [("BK1", "板块", "600000", "a", "t", "s")])
    with pytest.raises(ValueError):
        # 第二行列数错误 → 整个事务回滚，原有数据必须保留
        db.replace_partition("industry_members", "board_code = ?", ("BK1",), cols,
                             [("BK1", "板块", "600001", "b", "t", "s"), ("BK1",)])
    remaining = [r["code"] for r in db.query("SELECT code FROM industry_members")]
    assert remaining == ["600000"]


def test_upsert_stocks_keeps_first_seen(db: Database) -> None:
    db.upsert_stocks([("600000", "旧名", "SH", "2026-10-01T00:00:00+00:00", "s")])
    db.upsert_stocks([("600000", "新名", "SH", "2026-10-28T00:00:00+00:00", "s")])
    row = db.query_one("SELECT * FROM stocks WHERE code = ?", ("600000",))
    assert row["name"] == "新名"
    assert row["first_seen_at"] == "2026-10-01T00:00:00+00:00"
    assert row["last_seen_at"] == "2026-10-28T00:00:00+00:00"


def test_refresh_space_state_pauses_and_resumes(tmp_path) -> None:  # type: ignore[no-untyped-def]
    d = Database(tmp_path / "m.db", max_bytes=1, soft_bytes=1)
    d.connect()
    d.init_schema()
    d.refresh_space_state()
    assert d.write_paused
    d.max_bytes = 10**12
    d.soft_bytes = 10**12
    d.refresh_space_state()
    assert not d.write_paused
    d.close()
