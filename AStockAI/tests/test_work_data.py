from __future__ import annotations

import hashlib
import io
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from conftest import bj, make_spot_result

from config import Settings
from database import Database
from scheduler import JobContext
from state import RuntimeState
from timeutil import UTC
from trading_calendar import SessionPhase, TradingCalendar
from work_data import FILES, MANIFEST_FILE, WorkDataExporter, atomic_write_bytes, json_safe


def test_atomic_write_replaces_and_leaves_no_temp(tmp_path: Path) -> None:
    target = tmp_path / "a.json"
    target.write_bytes(b"old")
    atomic_write_bytes(target, b"new")
    assert target.read_bytes() == b"new"
    assert [p.name for p in tmp_path.iterdir()] == ["a.json"]


def test_atomic_write_failure_keeps_old_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "a.json"
    target.write_bytes(b"old")

    def deny(src: str, dst: str) -> None:
        raise PermissionError("locked by reader")

    monkeypatch.setattr("work_data.os.replace", deny)
    with pytest.raises(PermissionError):
        atomic_write_bytes(target, b"new", retries=2, wait_sec=0.0)
    assert target.read_bytes() == b"old"
    assert [p.name for p in tmp_path.iterdir()] == ["a.json"]


def test_json_safe_converts_nan_and_times() -> None:
    out = json_safe({"a": float("nan"), "b": datetime(2026, 10, 29, 1, 0, tzinfo=UTC), "c": pd.NaT,
                     "d": date(2026, 10, 29), "e": [math.inf, 1.5]})
    assert out == {"a": None, "b": "2026-10-29T09:00:00+08:00", "c": None, "d": "2026-10-29",
                   "e": [None, 1.5]}
    json.dumps(out, allow_nan=False)


def _calendar() -> TradingCalendar:
    dates, d = [], date(2026, 1, 1)
    while d <= date(2026, 12, 31):
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)
    cal = TradingCalendar(lambda: dates, db=None)
    cal.refresh(date(2026, 10, 1))
    return cal


def _ctx(now_bj: datetime) -> JobContext:
    return JobContext(now=now_bj.astimezone(UTC), now_bj=now_bj, today=now_bj.date(),
                      phase=SessionPhase.MORNING, calendar_basis="verified", target_day=date(2026, 10, 28))


def test_export_writes_files_and_consistent_manifest(settings: Settings, db: Database) -> None:
    state = RuntimeState()
    fetched = bj(2026, 10, 29, 10, 0)
    state.latest_spot = make_spot_result(fetched_at=fetched.astimezone(UTC))
    state.dirty.update({"stocks_latest", "market_latest", "candidates", "indicators", "alerts"})
    exporter = WorkDataExporter(settings, db, state, _calendar(), lambda: {"status": "ok"})
    exporter.export(_ctx(fetched))

    wd = settings.work_data_path
    manifest = json.loads((wd / MANIFEST_FILE).read_text(encoding="utf-8"))
    assert manifest["writer"]["running"] is True
    for key in ("stocks_latest", "market_latest", "market_status"):
        entry = manifest["files"][key]
        data = (wd / FILES[key]).read_bytes()
        assert entry["sha256"] == hashlib.sha256(data).hexdigest()
        assert entry["size_bytes"] == len(data)
        assert entry["write_status"] == "ok"
    # 数据库中没有筛选结果/异动时，不生成空的伪文件，而是在清单中标明
    assert manifest["files"]["candidates"]["write_status"] == "not_generated_yet"

    stocks = pd.read_csv(io.BytesIO((wd / FILES["stocks_latest"]).read_bytes()), dtype={"code": str})
    assert stocks["code"].iloc[0] == "600000"
    assert (stocks["data_status"] == "unknown_time").all()
    assert stocks["fetched_at"].iloc[0] == "2026-10-29T10:00:00+08:00"

    entry = manifest["files"]["stocks_latest"]
    # 交易时段内的快照有效期 = 获取时间 + spot_valid_sec
    assert entry["expires_at"] == "2026-10-29T10:03:00+08:00"

    exporter.export(_ctx(fetched), final=True)
    manifest2 = json.loads((wd / MANIFEST_FILE).read_text(encoding="utf-8"))
    assert manifest2["writer"]["running"] is False
    assert manifest2["manifest_seq"] == manifest["manifest_seq"] + 1


def test_lunch_break_data_expires_at_afternoon_open(settings: Settings, db: Database) -> None:
    exporter = WorkDataExporter(settings, db, RuntimeState(), _calendar(), lambda: {})
    exp = exporter._market_expiry(bj(2026, 10, 29, 11, 35).astimezone(UTC))
    assert exp == bj(2026, 10, 29, 13, 0)
    exp_close = exporter._market_expiry(bj(2026, 10, 30, 15, 5).astimezone(UTC))  # 周五收盘后
    assert exp_close == bj(2026, 11, 2, 9, 15)  # 下周一集合竞价开始
