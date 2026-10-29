from __future__ import annotations

import json
from datetime import datetime, timedelta

import pandas as pd
from conftest import make_raw_spot, make_spot_result

from config import AnomalyConfig
from anomaly_detector import (
    ANOMALY_DB_COLUMNS,
    detect_breadth_event,
    detect_sector_events,
    detect_stock_events,
)
from market_data import DataStatus, FetchResult, TimeBasis
from timeutil import UTC

TRADE_DATE = "2026-10-29"


def _ind(code: str, **kw: float) -> pd.DataFrame:
    base = {"code": code, "trade_date": "2026-10-28", "ma10": 10.0, "ma20": 9.5, "high_20": 11.0,
            "low_20": 9.0, "ma20_slope_5": 0.01}
    base.update(kw)
    return pd.DataFrame([base])


def test_volume_breakout_event_has_required_fields() -> None:
    raw = make_raw_spot(1)
    raw.loc[0, ["最新价", "最高", "最低", "涨跌幅", "量比"]] = [11.5, 11.6, 10.0, 5.0, 2.5]
    spot = make_spot_result(raw)
    events = detect_stock_events(spot, _ind("600000"), AnomalyConfig(), TRADE_DATE)
    types = {e["event_type"] for e in events}
    assert "VOLUME_BREAKOUT" in types
    ev = next(e for e in events if e["event_type"] == "VOLUME_BREAKOUT")
    for col in ANOMALY_DB_COLUMNS:
        assert col in ev
    assert ev["code"] == "600000" and ev["name"] == "测试0"
    assert ev["price"] == 11.5
    assert ev["source"].startswith("test:fixture")
    assert ev["data_status"] == DataStatus.UNKNOWN_TIME
    assert ev["indicator_trade_date"] == "2026-10-28"
    assert json.loads(ev["thresholds_json"])["volume_ratio_min"] == 2.0
    assert json.loads(ev["metrics_json"])["volume_ratio"] == 2.5
    assert ev["dedup_key"] == f"{TRADE_DATE}:VOLUME_BREAKOUT:600000"


def test_no_breakout_without_volume() -> None:
    raw = make_raw_spot(1)
    raw.loc[0, ["最新价", "最高", "最低", "涨跌幅", "量比"]] = [11.5, 11.6, 10.0, 5.0, 1.0]
    events = detect_stock_events(make_spot_result(raw), _ind("600000"), AnomalyConfig(), TRADE_DATE)
    assert "VOLUME_BREAKOUT" not in {e["event_type"] for e in events}


def test_support_break_and_spike_fade() -> None:
    raw = make_raw_spot(1)
    # 昨收10，最高10.8（+8%），现价 8.8：跌破20日低点9.0，且冲高回落
    raw.loc[0, ["最新价", "最高", "最低", "昨收", "涨跌幅", "量比"]] = [8.8, 10.8, 8.7, 10.0, -12.0, 1.0]
    raw.loc[0, "代码"] = "300001"  # 创业板，20%涨跌幅限制，-12% 属于正常范围
    events = detect_stock_events(make_spot_result(raw), _ind("300001"), AnomalyConfig(), TRADE_DATE)
    types = {e["event_type"] for e in events}
    assert {"SUPPORT_BREAK", "SPIKE_FADE"} <= types


def test_rows_with_validation_flags_are_ignored() -> None:
    raw = make_raw_spot(1)
    raw.loc[0, ["最新价", "最高", "最低", "涨跌幅", "量比"]] = [11.5, 5.0, 10.0, 5.0, 3.0]  # 最高<最低
    events = detect_stock_events(make_spot_result(raw), _ind("600000"), AnomalyConfig(), TRADE_DATE)
    assert events == []


def test_breadth_deterioration_from_peak() -> None:
    spot = make_spot_result()
    t0 = datetime(2026, 10, 29, 2, 0, tzinfo=UTC)
    history = [(t0, 0.7), (t0 + timedelta(minutes=30), 0.5)]
    ev = detect_breadth_event(spot, {"advance_ratio": 0.42, "up": 2000, "down": 2700}, history,
                              AnomalyConfig(), TRADE_DATE)
    assert len(ev) == 1 and ev[0]["code"] == "MARKET"
    assert json.loads(ev[0]["metrics_json"])["drop_from_peak"] == round(0.7 - 0.42, 6)
    none = detect_breadth_event(spot, {"advance_ratio": 0.6}, history, AnomalyConfig(), TRADE_DATE)
    assert none == []


def test_sector_resonance_requires_strong_members_when_available() -> None:
    raw = make_raw_spot(4)
    raw["涨跌幅"] = [6.0, 7.0, 5.5, 1.0]
    spot = make_spot_result(raw)
    boards_df = pd.DataFrame({"board_code": ["BK1"], "board_name": ["测试板块"], "price": [1000.0],
                              "pct_change": [3.0], "up_count": [9], "down_count": [1],
                              "leader_name": ["测试0"], "leader_pct_change": [7.0]})
    boards = FetchResult("industry_boards", "test:boards", boards_df, spot.fetched_at,
                         TimeBasis.FETCH_TIME_ONLY, DataStatus.UNKNOWN_TIME)
    members = pd.DataFrame({"code": list(spot.df["code"]), "board_code": "BK1"})
    ev = detect_sector_events(spot, boards, members, AnomalyConfig(), TRADE_DATE)
    assert len(ev) == 1 and ev[0]["code"] == "BK1"
    assert json.loads(ev[0]["metrics_json"])["strong_members"] == 3
    weak = detect_sector_events(spot, boards, members, AnomalyConfig(sector_min_strong_members=4), TRADE_DATE)
    assert weak == []
