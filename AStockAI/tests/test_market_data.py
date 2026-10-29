from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from conftest import make_raw_spot

from config import DataSourceConfig
from market_data import (
    AKShareAdapter,
    CircuitBreaker,
    CircuitOpenError,
    DataSourceError,
    DataStatus,
    DataValidationError,
    TimeBasis,
    classify_bar_freshness,
    exchange_of,
    normalize_daily,
    normalize_minute,
    normalize_spot,
    price_limit_pct,
)
from timeutil import UTC


def test_normalize_spot_maps_fields_without_fabrication() -> None:
    raw = make_raw_spot(3)
    raw.loc[1, "最新价"] = np.nan  # 停牌：价格缺失
    raw["量比"] = raw["量比"].astype(object)
    raw.loc[2, "量比"] = "-"       # 数据源返回的占位符
    df, warnings = normalize_spot(raw)
    assert list(df["code"]) == ["600000", "600001", "600002"]
    assert pd.isna(df.loc[1, "price"])          # 不填补缺失价格
    assert pd.isna(df.loc[2, "volume_ratio"])   # 非数值转为空，不补默认值
    assert "price_missing" in df.loc[1, "validation_flags"]
    assert warnings == []


def test_normalize_spot_missing_required_column() -> None:
    raw = make_raw_spot(3).drop(columns=["成交额"])
    with pytest.raises(DataValidationError):
        normalize_spot(raw)


def test_normalize_spot_flags_inconsistent_rows_and_dedups() -> None:
    raw = make_raw_spot(3)
    raw.loc[0, "最高"] = 5.0  # 最高价低于最低价
    raw = pd.concat([raw, raw.iloc[[2]]], ignore_index=True)
    df, warnings = normalize_spot(raw)
    assert "high_lt_low" in df.loc[0, "validation_flags"]
    assert len(df) == 3
    assert any(w.startswith("dropped_duplicate_code_rows") for w in warnings)


def test_code_normalization_and_exchange() -> None:
    raw = make_raw_spot(2)
    raw["代码"] = ["sh600519", 1]
    df, _ = normalize_spot(raw)
    assert list(df["code"]) == ["600519", "000001"]
    assert exchange_of("600519") == "SH" and exchange_of("000001") == "SZ"
    assert exchange_of("830799") == "BJ" and exchange_of("920001") == "BJ"


def test_price_limit_rules() -> None:
    assert price_limit_pct("600000", "浦发银行") == 10.0
    assert price_limit_pct("300750", "宁德时代") == 20.0
    assert price_limit_pct("688981", "中芯国际") == 20.0
    assert price_limit_pct("830799", "艾融软件") == 30.0
    assert price_limit_pct("600001", "*ST测试") == 5.0


def test_normalize_daily_and_minute() -> None:
    raw = pd.DataFrame({"日期": ["2026-10-28", "2026-10-27"], "开盘": [1, 2], "收盘": [1, 2],
                        "最高": [1, 2], "最低": [1, 2], "成交量": [100, 200]})
    d = normalize_daily(raw, "600000")
    assert list(d["trade_date"]) == ["2026-10-27", "2026-10-28"]
    assert pd.isna(d.loc[0, "turnover_rate"])  # 源数据没有换手率 → 保持为空
    raw_m = pd.DataFrame({"时间": ["2026-10-29 09:31:00"], "开盘": [1], "收盘": [1], "最高": [1],
                          "最低": [1], "成交量": [10]})
    m = normalize_minute(raw_m, "600000", "1")
    assert m.loc[0, "bar_time"] == "2026-10-29T01:31:00+00:00"  # 北京时间 09:31 → UTC 01:31


def test_classify_bar_freshness() -> None:
    now = datetime(2026, 10, 29, 2, 0, tzinfo=UTC)
    assert classify_bar_freshness(now - timedelta(seconds=60), now, True, 150) == DataStatus.REALTIME
    assert classify_bar_freshness(now - timedelta(seconds=600), now, True, 150) == DataStatus.DELAYED
    assert classify_bar_freshness(now - timedelta(seconds=600), now, False, 150) == DataStatus.HISTORICAL
    assert classify_bar_freshness(None, now, True, 150) == DataStatus.UNKNOWN_TIME


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, sec: float) -> None:
        self.t += sec


def _adapter(fake_ak: object, **cfg_overrides: object) -> tuple[AKShareAdapter, _Clock]:
    cfg = DataSourceConfig(min_request_interval_sec=0.0, retry_backoff_sec=0.0, spot_min_rows=3,
                           circuit_breaker_failures=2, circuit_breaker_cooldown_sec=60.0)
    for k, v in cfg_overrides.items():
        setattr(cfg, k, v)
    clock = _Clock()
    return AKShareAdapter(cfg, ak_module=fake_ak, sleeper=clock.sleep, clock=clock), clock


def test_adapter_fetch_spot_success_sets_metadata() -> None:
    fake = SimpleNamespace(stock_zh_a_spot_em=lambda: make_raw_spot(5))
    adapter, _ = _adapter(fake)
    res = adapter.fetch_spot()
    assert res.time_basis == TimeBasis.FETCH_TIME_ONLY
    assert res.data_status == DataStatus.UNKNOWN_TIME
    assert res.quote_time is None
    assert res.source.startswith("akshare:stock_zh_a_spot_em")
    assert res.fetched_at.tzinfo is not None


def test_adapter_marks_incomplete_snapshot_invalid() -> None:
    fake = SimpleNamespace(stock_zh_a_spot_em=lambda: make_raw_spot(2))
    adapter, _ = _adapter(fake)
    assert adapter.fetch_spot().data_status == DataStatus.INVALID


def test_adapter_retries_are_bounded_then_circuit_opens() -> None:
    calls = {"n": 0}

    def boom() -> pd.DataFrame:
        calls["n"] += 1
        raise ConnectionError("network down")

    adapter, clock = _adapter(SimpleNamespace(stock_zh_a_spot_em=boom), max_retries=2)
    with pytest.raises(DataSourceError):
        adapter.fetch_spot()
    assert calls["n"] == 3  # 1 次 + 2 次重试，不会无限重试
    with pytest.raises(DataSourceError):
        adapter.fetch_spot()
    assert calls["n"] == 6
    with pytest.raises(CircuitOpenError):  # 连续两次失败后熔断，不再发请求
        adapter.fetch_spot()
    assert calls["n"] == 6
    clock.t += 61  # 冷却结束后允许试探
    with pytest.raises(DataSourceError):
        adapter.fetch_spot()
    assert calls["n"] == 9


def test_adapter_missing_function_is_reported() -> None:
    adapter, _ = _adapter(SimpleNamespace())
    with pytest.raises(DataSourceError):
        adapter.fetch_spot()


def test_circuit_breaker_success_resets() -> None:
    t = {"now": 0.0}
    cb = CircuitBreaker(2, 10.0, clock=lambda: t["now"])
    cb.record_failure()
    cb.record_success()
    cb.record_failure()
    assert not cb.is_open()


def test_trade_calendar_parsing() -> None:
    fake = SimpleNamespace(tool_trade_date_hist_sina=lambda: pd.DataFrame(
        {"trade_date": [date(2026, 10, 28), date(2026, 10, 29)]}))
    adapter, _ = _adapter(fake)
    assert adapter.fetch_trade_calendar() == [date(2026, 10, 28), date(2026, 10, 29)]
