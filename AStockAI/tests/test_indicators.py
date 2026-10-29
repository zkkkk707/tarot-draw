from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from conftest import make_daily_bars

from config import IndicatorConfig
from indicators import INDICATOR_COLUMNS, compute_batch, compute_indicators, macd, rsi_wilder, sma


def test_sma_requires_full_window() -> None:
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    out = sma(s, 3)
    assert out.isna().tolist()[:2] == [True, True]
    assert out.iloc[2] == pytest.approx(2.0)
    assert out.iloc[4] == pytest.approx(4.0)


def test_rsi_bounds_and_extremes() -> None:
    up = pd.Series(np.arange(1.0, 40.0))
    assert rsi_wilder(up).dropna().eq(100.0).all()
    flat = pd.Series([10.0] * 40)
    assert rsi_wilder(flat).dropna().eq(50.0).all()
    rng = np.random.default_rng(1)
    noisy = pd.Series(10 + rng.normal(0, 1, 200).cumsum())
    r = rsi_wilder(noisy).dropna()
    assert ((r >= 0) & (r <= 100)).all()


def test_macd_zero_for_constant_series() -> None:
    dif, dea, hist = macd(pd.Series([10.0] * 60))
    assert dif.dropna().abs().max() == pytest.approx(0.0)
    assert hist.dropna().abs().max() == pytest.approx(0.0)


def test_output_columns_and_length() -> None:
    bars = make_daily_bars("600000", 120)
    out = compute_indicators(bars, None, IndicatorConfig())
    assert list(out.columns) == INDICATOR_COLUMNS
    assert len(out) == 120
    assert out["bars_available"].iloc[-1] == 120
    assert pd.isna(out["ma60"].iloc[58])  # 第59根K线时样本不足60，必须为空
    assert pd.notna(out["ma60"].iloc[59])
    assert out["ma60"].iloc[59] == pytest.approx(bars["close"].iloc[:60].mean())


def test_no_lookahead_truncation_invariance() -> None:
    """截断未来数据后，历史各行指标值必须完全不变（防未来函数）。"""
    bars = make_daily_bars("600000", 150, seed=7)
    bench = pd.Series(np.linspace(3000, 3300, 150), index=bars["trade_date"])
    cfg = IndicatorConfig()
    full = compute_indicators(bars, bench, cfg)
    for cut in (60, 90, 120):
        part = compute_indicators(bars.iloc[:cut], bench, cfg)
        pd.testing.assert_frame_equal(full.iloc[:cut].reset_index(drop=True), part.reset_index(drop=True))


def test_signals_are_null_when_history_insufficient() -> None:
    bars = make_daily_bars("600000", 10)
    out = compute_indicators(bars, None, IndicatorConfig())
    assert out["sig_breakout_volume"].isna().all()
    assert out["sig_trend_weakening"].isna().all()


def test_breakout_signal_on_constructed_data() -> None:
    bars = make_daily_bars("600000", 40, seed=3)
    # 最后一天：收盘价显著高于前20日最高价，成交量为前5日均量的3倍
    prev_high = bars["high"].iloc[-21:-1].max()
    prev_vol = bars["volume_lots"].iloc[-6:-1].mean()
    bars.loc[bars.index[-1], ["close", "high"]] = [prev_high * 1.05, prev_high * 1.06]
    bars.loc[bars.index[-1], "volume_lots"] = prev_vol * 3
    out = compute_indicators(bars, None, IndicatorConfig())
    assert out["sig_breakout_volume"].iloc[-1] == 1.0
    assert out["prior_high_20"].iloc[-1] == pytest.approx(prev_high)


def test_relative_strength_uses_aligned_dates() -> None:
    bars = make_daily_bars("600000", 30)
    bench = pd.Series(100.0, index=bars["trade_date"])
    out = compute_indicators(bars, bench, IndicatorConfig())
    last = out.iloc[-1]
    assert last["benchmark_ret_20"] == pytest.approx(0.0)
    assert last["rs_20"] == pytest.approx(last["ret_20"])


def test_batch_keeps_last_rows_per_code() -> None:
    bars = pd.concat([make_daily_bars("600000", 70, seed=1), make_daily_bars("000001", 70, seed=2)])
    out = compute_batch(bars, None, IndicatorConfig(), keep_last=3)
    assert len(out) == 6
    assert set(out["code"]) == {"600000", "000001"}


def test_rejects_multiple_codes() -> None:
    bars = pd.concat([make_daily_bars("600000", 5), make_daily_bars("000001", 5)])
    with pytest.raises(ValueError):
        compute_indicators(bars, None, IndicatorConfig())
