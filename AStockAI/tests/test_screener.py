from __future__ import annotations

import numpy as np
import pandas as pd
from conftest import make_raw_spot

from config import ScreenerConfig
from market_data import normalize_spot
from screener import CATALYST_MISSING_STATUS, MarketContext, build_market_context, rank_results, score_universe


def _spot(n: int = 5) -> pd.DataFrame:
    df, _ = normalize_spot(make_raw_spot(n))
    return df


def _indicators(codes: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "code": codes,
        "trade_date": "2026-10-28",
        "bars_available": 120,
        "ma5": 10.5, "ma10": 10.2, "ma20": 9.8,
        "rsi14": 60.0, "macd_hist": 0.1,
        "high_20": 20.0, "low_20": 8.0, "volatility_20": 0.25,
        "sig_breakout_volume": 0.0, "sig_pullback_low_volume": 0.0,
    })


def test_catalyst_always_missing_never_scored() -> None:
    spot = _spot()
    scored = score_universe(spot, _indicators(list(spot["code"])), None, None,
                            MarketContext(0.6, {"000001": True}), ScreenerConfig(), set())
    assert scored["catalyst_score"].isna().all()
    assert (scored["catalyst_max"] == 0).all()
    assert (scored["catalyst_status"] == CATALYST_MISSING_STATUS).all()
    assert scored["missing_components"].str.contains("catalyst:no_reliable_source").all()


def test_missing_sector_reduces_available_max_instead_of_adding_points() -> None:
    spot = _spot()
    scored = score_universe(spot, _indicators(list(spot["code"])), None, None,
                            MarketContext(0.6, {"000001": True}), ScreenerConfig(), set())
    assert scored["sector_score"].isna().all()
    assert (scored["sector_max"] == 0).all()
    # 可得满分 = 量价25 + 市场20 + 风险收益15 = 60
    assert np.allclose(scored["available_max"], 60.0)
    assert (scored["total_score"] <= scored["available_max"]).all()


def test_no_indicators_marks_volume_price_missing() -> None:
    spot = _spot()
    empty = pd.DataFrame(columns=["code", "trade_date"])
    scored = score_universe(spot, empty, None, None, MarketContext(None, {}), ScreenerConfig(), set())
    assert scored["missing_components"].str.contains("volume_price:no_indicator_history").all()
    assert scored["missing_components"].str.contains("market:no_breadth").all()
    # 只有量比一个子项可计算（5分），市场与板块、风险收益均缺失
    assert np.allclose(scored["available_max"], 5.0)


def test_sector_scoring_with_board_data() -> None:
    spot = _spot(4)
    boards = pd.DataFrame({"board_code": ["BK1", "BK2"], "board_name": ["甲", "乙"],
                           "pct_change": [3.0, -1.0], "up_count": [8, 2], "down_count": [2, 8]})
    members = pd.DataFrame({"code": list(spot["code"]), "board_code": ["BK1", "BK1", "BK2", "BK2"]})
    scored = score_universe(spot, _indicators(list(spot["code"])), boards, members,
                            MarketContext(0.6, {}), ScreenerConfig(), set())
    top = scored.loc[scored["board_name"] == "甲"]
    bottom = scored.loc[scored["board_name"] == "乙"]
    assert (top["sector_score"] > bottom["sector_score"]).all()
    assert np.allclose(top["sector_score"], 25 * 0.6 * 1.0 + 25 * 0.4 * 0.8)


def test_ranking_filters_and_keeps_watchlist() -> None:
    raw = make_raw_spot(6)
    raw.loc[5, "名称"] = "*ST测试"
    spot, _ = normalize_spot(raw)
    st_code = spot.loc[5, "code"]
    cfg = ScreenerConfig(top_n=2, min_available_ratio=0.5)
    scored = score_universe(spot, _indicators(list(spot["code"])), None, None,
                            MarketContext(0.6, {"000001": True}), cfg, {st_code})
    ranked = rank_results(scored, cfg)
    ranked_rows = ranked.loc[ranked["rank"] > 0]
    assert list(ranked_rows["rank"]) == [1, 2]
    assert st_code not in set(ranked_rows["code"])
    watch_row = ranked.loc[ranked["code"] == st_code].iloc[0]
    assert watch_row["rank"] == 0
    assert "NOT_RANKED:FILTER_ST" in watch_row["reason_codes"]
    assert (ranked_rows["score_normalized"].diff().dropna() <= 0).all()


def test_build_market_context() -> None:
    ctx = build_market_context({"advance_ratio": 0.55}, {"000001": 3100.0, "399001": 9000.0},
                               {"000001": 3000.0, "399001": 9500.0, "399006": None})
    assert ctx.advance_ratio == 0.55
    assert ctx.index_above_ma20 == {"000001": True, "399001": False, "399006": None}
