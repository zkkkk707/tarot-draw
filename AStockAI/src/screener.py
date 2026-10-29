"""股票筛选与评分（初始研究假设，允许调整与回测）。

评分框架（满分100，权重可在 settings.yaml 的 screener 段调整，合计必须为100）：

| 组成          | 权重 | 子项（占该组成权重的比例）                                         |
|---------------|------|--------------------------------------------------------------------|
| 板块强度      | 25   | 所属行业板块涨幅排名分位 60%；板块上涨家数占比 40%                 |
| 量价结构      | 25   | 价格>MA20 20%；MA5>MA10>MA20 20%；量比区间 20%；突破/回踩信号 20%；  |
|               |      | 动量（MACD柱>0 且 RSI 在区间内）20%                                 |
| 市场环境      | 20   | 市场宽度（上涨家数占比）50%；主要指数位于MA20之上的比例 50%        |
| 催化与基本面  | 15   | V1.0 未接入可靠的新闻/公告/财务数据源 → 一律标记缺失，不补分       |
| 风险收益比    | 15   | 距20日高点空间 ÷ 距20日低点空间 2/3；年化波动率 1/3                 |

缺失处理（核心原则：缺数据就记为缺失，绝不随意补分）
- 每个子项单独判断输入是否齐全；输入缺失的子项既不得分、也不计入“可得满分”。
- total_score = 已得分合计；available_max = 可得满分合计；
  score_normalized = total_score / available_max × 100（用于排名，保证不同缺失程度可比）。
- available_max < min_available_ratio × 100 的股票不参与排名。
- missing_components 字段列出缺失原因；reason_codes 字段列出结构化的满足条件代码。

数据时点
- 盘中筛选：价格/量比来自最新全A快照；均线/RSI/MACD 等来自“前一交易日收盘”的指标
  （indicator_trade_date 字段标明），不会使用当日尚未收盘的日K。
- 收盘后筛选：使用当日收盘快照与当日指标。

本模块只保存结构化评分结果，不生成任何推荐文字或买卖结论。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from config import ScreenerConfig
from market_data import DataStatus, is_bse
from memory_guard import TaskKind
from timeutil import BEIJING_TZ, now_utc, to_db_ts

if TYPE_CHECKING:
    from config import Settings
    from database import Database
    from memory_guard import MemoryGuard
    from scheduler import JobContext, JobResult
    from state import RuntimeState

logger = logging.getLogger(__name__)

CATALYST_MISSING_STATUS = "missing:no_reliable_news_or_fundamental_source"

RESULT_COLUMNS = [
    "rank", "code", "name", "in_watchlist", "price", "pct_change", "amount_yuan",
    "total_score", "available_max", "score_normalized",
    "sector_score", "sector_max", "volume_price_score", "volume_price_max",
    "market_score", "market_max", "catalyst_score", "catalyst_max", "catalyst_status",
    "risk_reward_score", "risk_reward_max", "missing_components", "reason_codes",
    "board_name", "indicator_trade_date",
]

IND_COLUMNS = ["code", "trade_date", "bars_available", "ma5", "ma10", "ma20", "rsi14", "macd_hist",
               "high_20", "low_20", "volatility_20", "sig_breakout_volume", "sig_pullback_low_volume"]


@dataclass
class MarketContext:
    advance_ratio: float | None
    index_above_ma20: dict[str, bool | None] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"advance_ratio": self.advance_ratio, "index_above_ma20": self.index_above_ma20}


class _Acc:
    """累加器：逐子项累计得分、可得满分、理由代码与缺失原因。"""

    def __init__(self, index: pd.Index) -> None:
        self.index = index
        self.reasons = pd.Series("", index=index, dtype=object)
        self.missing = pd.Series("", index=index, dtype=object)

    def part(self, score_frac: pd.Series, available: pd.Series, max_pts: float) -> tuple[pd.Series, pd.Series]:
        avail = available.fillna(False).astype(bool)
        frac = score_frac.astype(float).clip(0.0, 1.0).fillna(0.0)
        score = (frac * max_pts).where(avail, 0.0)
        mx = pd.Series(np.where(avail, max_pts, 0.0), index=self.index)
        return score, mx

    def reason(self, mask: pd.Series, code: str) -> None:
        m = mask.fillna(False).astype(bool)
        self.reasons.loc[m] = self.reasons.loc[m] + code + ";"

    def miss(self, mask: pd.Series, code: str) -> None:
        m = mask.fillna(False).astype(bool)
        self.missing.loc[m] = self.missing.loc[m] + code + ";"


def _finish(parts: list[tuple[pd.Series, pd.Series]]) -> tuple[pd.Series, pd.Series]:
    score = sum((p[0] for p in parts), start=pd.Series(0.0, index=parts[0][0].index))
    mx = sum((p[1] for p in parts), start=pd.Series(0.0, index=parts[0][1].index))
    return score.where(mx > 0), mx


def eligibility(df: pd.DataFrame, cfg: ScreenerConfig) -> pd.Series:
    """返回过滤原因（空字符串表示可参与排名）。"""
    reason = pd.Series("", index=df.index, dtype=object)

    def add(mask: pd.Series, code: str) -> None:
        m = mask.fillna(False).astype(bool)
        reason.loc[m] = reason.loc[m] + code + ";"

    name = df["name"].astype(str)
    if cfg.exclude_st:
        add(name.str.upper().str.contains("ST", regex=False), "FILTER_ST")
        add(name.str.contains("退", regex=False), "FILTER_DELISTING")
    if not cfg.include_bse:
        add(df["code"].map(is_bse), "FILTER_BSE")
    add(df["price"].isna() | (df["price"] <= 0), "FILTER_NO_PRICE")
    add(df["amount_yuan"].isna() | (df["amount_yuan"] < cfg.min_amount_yuan), "FILTER_LOW_AMOUNT")
    add(df["validation_flags"].fillna("").astype(str) != "", "FILTER_DATA_FLAGS")
    return reason


def score_universe(spot: pd.DataFrame, indicators: pd.DataFrame, boards: pd.DataFrame | None,
                   members: pd.DataFrame | None, market: MarketContext, cfg: ScreenerConfig,
                   watchlist: set[str]) -> pd.DataFrame:
    """对全市场打分，返回所有股票的评分明细（未截断、未排序）。"""
    base_cols = ["code", "name", "price", "pct_change", "amount_yuan", "volume_ratio", "validation_flags"]
    df = spot.loc[:, base_cols].copy()
    ind = indicators.loc[:, [c for c in IND_COLUMNS if c in indicators.columns]] if not indicators.empty \
        else pd.DataFrame(columns=IND_COLUMNS)
    for c in IND_COLUMNS:
        if c not in ind.columns:
            ind[c] = np.nan
    ind = ind.rename(columns={"trade_date": "indicator_trade_date"})
    df = df.merge(ind, on="code", how="left")

    # 行业板块
    if boards is not None and not boards.empty and members is not None and not members.empty:
        b = boards.loc[:, ["board_code", "board_name", "pct_change", "up_count", "down_count"]].copy()
        b = b.rename(columns={"pct_change": "board_pct"})
        n = len(b)
        ranks = b["board_pct"].rank(ascending=True, method="average")
        b["board_rank_frac"] = (ranks - 1) / (n - 1) if n > 1 else np.nan
        denom = (b["up_count"] + b["down_count"]).astype(float)
        b["board_up_ratio"] = (b["up_count"] / denom).where(denom > 0)
        mem = members.loc[:, ["code", "board_code"]].drop_duplicates("code")
        df = df.merge(mem, on="code", how="left").merge(
            b.drop(columns=["up_count", "down_count"]), on="board_code", how="left")
    else:
        df["board_code"] = np.nan
        df["board_name"] = np.nan
        df["board_pct"] = np.nan
        df["board_rank_frac"] = np.nan
        df["board_up_ratio"] = np.nan

    df = df.reset_index(drop=True)
    acc = _Acc(df.index)
    price = df["price"].astype(float)

    # ---- 板块强度 ----
    w = cfg.weight_sector
    s1 = acc.part(df["board_rank_frac"], df["board_rank_frac"].notna(), w * 0.6)
    s2 = acc.part(df["board_up_ratio"], df["board_up_ratio"].notna(), w * 0.4)
    sector_score, sector_max = _finish([s1, s2])
    acc.miss(df["board_code"].isna(), "sector:no_board_membership_or_board_data")
    acc.reason(df["board_rank_frac"] >= 0.75, "BOARD_TOP_QUARTILE")
    acc.reason(df["board_up_ratio"] >= 0.6, "BOARD_BREADTH_STRONG")

    # ---- 量价结构 ----
    w = cfg.weight_volume_price / 5.0
    ma5, ma10, ma20 = df["ma5"].astype(float), df["ma10"].astype(float), df["ma20"].astype(float)
    vr = df["volume_ratio"].astype(float)
    above_ma20 = price > ma20
    v1 = acc.part(above_ma20.astype(float), price.notna() & ma20.notna(), w)
    bull = (ma5 > ma10) & (ma10 > ma20)
    v2 = acc.part(bull.astype(float), ma5.notna() & ma10.notna() & ma20.notna(), w)
    in_range = (vr >= cfg.volume_ratio_low) & (vr <= cfg.volume_ratio_high)
    near = ((vr >= 1.0) & (vr < cfg.volume_ratio_low)) | ((vr > cfg.volume_ratio_high) & (vr <= 5.0))
    v3 = acc.part(pd.Series(np.where(in_range, 1.0, np.where(near, 0.5, 0.0)), index=df.index),
                  vr.notna(), w)
    sig_b = df["sig_breakout_volume"].astype(float)
    sig_p = df["sig_pullback_low_volume"].astype(float)
    high20 = df["high_20"].astype(float)
    intraday_break = (price > high20) & (vr >= 1.5)
    any_sig = (sig_b == 1) | (sig_p == 1) | intraday_break
    v4 = acc.part(any_sig.astype(float), sig_b.notna() | sig_p.notna() | (high20.notna() & vr.notna()), w)
    macd_pos = df["macd_hist"].astype(float) > 0
    rsi = df["rsi14"].astype(float)
    rsi_ok = (rsi >= cfg.rsi_low) & (rsi <= cfg.rsi_high)
    mom = np.where(macd_pos & rsi_ok, 1.0, np.where(macd_pos | rsi_ok, 0.5, 0.0))
    v5 = acc.part(pd.Series(mom, index=df.index), df["macd_hist"].notna() & rsi.notna(), w)
    vp_score, vp_max = _finish([v1, v2, v3, v4, v5])
    acc.miss(df["indicator_trade_date"].isna(), "volume_price:no_indicator_history")
    acc.miss(vr.isna(), "volume_price:no_volume_ratio")
    acc.reason(above_ma20, "ABOVE_MA20")
    acc.reason(bull, "MA_BULL_ALIGN")
    acc.reason(in_range, "VOL_RATIO_IN_RANGE")
    acc.reason(sig_b == 1, "BREAKOUT_VOLUME_LAST_CLOSE")
    acc.reason(sig_p == 1, "PULLBACK_LOW_VOLUME_LAST_CLOSE")
    acc.reason(intraday_break, "PRICE_ABOVE_HIGH20_WITH_VOLUME")
    acc.reason(macd_pos, "MACD_HIST_POSITIVE")
    acc.reason(rsi_ok, "RSI_IN_RANGE")

    # ---- 市场环境（全市场相同） ----
    w = cfg.weight_market / 2.0
    all_rows = pd.Series(True, index=df.index)
    no_rows = pd.Series(False, index=df.index)
    zeros = pd.Series(0.0, index=df.index)
    if market.advance_ratio is not None:
        frac = (market.advance_ratio - cfg.breadth_bear) / (cfg.breadth_bull - cfg.breadth_bear)
        m1 = acc.part(zeros + frac, all_rows, w)
        if market.advance_ratio >= 0.5:
            acc.reason(all_rows, "MARKET_BREADTH_POSITIVE")
    else:
        m1 = acc.part(zeros, no_rows, w)
        acc.miss(all_rows, "market:no_breadth")
    known = [v for v in market.index_above_ma20.values() if v is not None]
    if known:
        n_above = sum(1 for v in known if v)
        m2 = acc.part(zeros + n_above / len(known), all_rows, w)
        if n_above * 2 > len(known):
            acc.reason(all_rows, "INDEX_ABOVE_MA20_MAJORITY")
    else:
        m2 = acc.part(zeros, no_rows, w)
        acc.miss(all_rows, "market:no_index_trend")
    market_score, market_max = _finish([m1, m2])

    # ---- 催化与基本面：缺失 ----
    catalyst_score = pd.Series(np.nan, index=df.index)
    catalyst_max = pd.Series(0.0, index=df.index)
    acc.miss(all_rows, "catalyst:no_reliable_source")

    # ---- 风险收益比 ----
    w = cfg.weight_risk_reward
    low20 = df["low_20"].astype(float)
    vol = df["volatility_20"].astype(float)
    below_support = price <= low20
    above_res = price >= high20
    inside = (price > low20) & (price < high20)
    with np.errstate(divide="ignore", invalid="ignore"):
        rr = (high20 - price) / (price - low20)
    rr_frac = (rr / cfg.rr_full_score_ratio).where(inside, 0.0)
    r1 = acc.part(rr_frac, price.notna() & low20.notna() & high20.notna() & ~above_res, w * 2.0 / 3.0)
    vol_frac = (cfg.vol_high - vol) / (cfg.vol_high - cfg.vol_low)
    r2 = acc.part(vol_frac, vol.notna(), w / 3.0)
    rr_score, rr_max = _finish([r1, r2])
    acc.reason(inside & (rr >= cfg.rr_full_score_ratio), "RR_FAVORABLE")
    acc.reason(vol <= cfg.vol_low, "LOW_VOLATILITY")
    acc.reason(below_support, "AT_OR_BELOW_LOW20")
    acc.reason(above_res, "AT_OR_ABOVE_HIGH20")
    acc.miss(above_res, "risk_reward:no_overhead_reference_above_high20")

    total = (sector_score.fillna(0) + vp_score.fillna(0) + market_score.fillna(0) + rr_score.fillna(0))
    available = sector_max + vp_max + market_max + catalyst_max + rr_max
    normalized = (total / available * 100.0).where(available > 0)

    out = pd.DataFrame({
        "code": df["code"],
        "name": df["name"],
        "in_watchlist": df["code"].isin(watchlist).astype(int),
        "price": price,
        "pct_change": df["pct_change"],
        "amount_yuan": df["amount_yuan"],
        "total_score": total.where(available > 0).round(4),
        "available_max": available.round(4),
        "score_normalized": normalized.round(4),
        "sector_score": sector_score.round(4),
        "sector_max": sector_max.round(4),
        "volume_price_score": vp_score.round(4),
        "volume_price_max": vp_max.round(4),
        "market_score": market_score.round(4),
        "market_max": market_max.round(4),
        "catalyst_score": catalyst_score,
        "catalyst_max": catalyst_max,
        "catalyst_status": CATALYST_MISSING_STATUS,
        "risk_reward_score": rr_score.round(4),
        "risk_reward_max": rr_max.round(4),
        "missing_components": acc.missing.str.rstrip(";"),
        "reason_codes": acc.reasons.str.rstrip(";"),
        "board_name": df["board_name"],
        "indicator_trade_date": df["indicator_trade_date"],
    })
    out["filter_reasons"] = eligibility(df, cfg).str.rstrip(";")
    return out


def rank_results(scored: pd.DataFrame, cfg: ScreenerConfig) -> pd.DataFrame:
    """筛选可排名股票并排序；自选股即使未进入前 top_n 或被过滤也会保留（rank=0 表示未排名）。"""
    min_avail = cfg.min_available_ratio * 100.0
    eligible = (scored["filter_reasons"] == "") & (scored["available_max"] >= min_avail) \
        & scored["score_normalized"].notna()
    ranked = scored.loc[eligible].sort_values(
        ["score_normalized", "total_score", "amount_yuan"], ascending=[False, False, False])
    ranked = ranked.assign(rank=np.arange(1, len(ranked) + 1))
    top = ranked.head(cfg.top_n)
    watch_rest = scored.loc[(scored["in_watchlist"] == 1) & ~scored["code"].isin(top["code"])].copy()
    watch_rest = watch_rest.merge(ranked[["code", "rank"]], on="code", how="left")
    watch_rest["rank"] = watch_rest["rank"].fillna(0).astype(int)
    unranked = watch_rest["rank"] == 0
    watch_rest.loc[unranked, "reason_codes"] = (
        watch_rest.loc[unranked, "reason_codes"].fillna("")
        + ";NOT_RANKED:" + watch_rest.loc[unranked, "filter_reasons"].replace("", "INSUFFICIENT_DATA")
    ).str.lstrip(";")
    out = pd.concat([top, watch_rest], ignore_index=True)
    return out.loc[:, RESULT_COLUMNS + ["filter_reasons"]]


def build_market_context(breadth: dict[str, Any] | None, index_prices: dict[str, float],
                         index_ma20: dict[str, float | None]) -> MarketContext:
    adv = None if breadth is None else breadth.get("advance_ratio")
    above: dict[str, bool | None] = {}
    for code, ma in index_ma20.items():
        p = index_prices.get(code)
        above[code] = None if (p is None or ma is None or pd.isna(p) or pd.isna(ma)) else bool(p > ma)
    return MarketContext(advance_ratio=adv, index_above_ma20=above)


# ---------------------------------------------------------------------------
# 筛选服务（数据库读写）
# ---------------------------------------------------------------------------

def load_members_cached(db: "Database", state: "RuntimeState") -> pd.DataFrame:
    """行业成分映射（可释放缓存）。同一股票取最近更新的板块。"""
    row = db.query_one("SELECT MAX(updated_at) AS u FROM industry_members")
    key = None if row is None else row["u"]
    if state.members_cache is None or state.members_cache_key != key:
        state.members_cache = db.query_df(
            "SELECT code, board_code, board_name FROM industry_members ORDER BY updated_at DESC")
        state.members_cache_key = key
    return state.members_cache


def load_indicators_cached(db: "Database", state: "RuntimeState", max_trade_date: str) -> pd.DataFrame:
    """每只股票截至 max_trade_date（含）的最新指标（可释放缓存）。"""
    from indicators import load_latest_indicators

    row = db.query_one("SELECT MAX(computed_at) AS c FROM indicator_values")
    key = f"{max_trade_date}:{None if row is None else row['c']}"
    if state.indicator_cache is None or state.indicator_cache_key != key:
        state.indicator_cache = load_latest_indicators(db, max_trade_date)
        state.indicator_cache_key = key
    return state.indicator_cache


class ScreeningService:
    def __init__(self, settings: "Settings", db: "Database", guard: "MemoryGuard",
                 state: "RuntimeState") -> None:
        self.s = settings
        self.db = db
        self.guard = guard
        self.state = state

    def index_ma20(self, max_trade_date: str) -> dict[str, float | None]:
        out: dict[str, float | None] = {}
        for code in self.s.datasource.index_codes:
            rows = self.db.query(
                "SELECT close FROM index_daily WHERE code = ? AND trade_date <= ?"
                " ORDER BY trade_date DESC LIMIT 20", (code, max_trade_date))
            closes = [r["close"] for r in rows if r["close"] is not None]
            out[code] = float(np.mean(closes)) if len(closes) == 20 else None
        return out

    def run(self, ctx: "JobContext", mode: str) -> "JobResult":
        """mode: intraday（盘中，使用前一交易日指标）/ close（收盘后，使用当日指标）。"""
        from scheduler import JobResult, Scheduler

        spot = self.state.latest_spot
        if spot is None or spot.data_status in (DataStatus.INVALID, DataStatus.STALE):
            logger.info("筛选跳过：无可用全A快照（%s）", None if spot is None else spot.data_status)
            return JobResult.SKIPPED
        if mode == "close":
            target = ctx.target_day
            if target is None or self.db.get_state(Scheduler.done_key("indicators", target.isoformat())) is None:
                return JobResult.SKIPPED
            if spot.fetched_at.astimezone(BEIJING_TZ).date() != target:
                return JobResult.SKIPPED
            max_date = target.isoformat()
        else:
            max_date = (ctx.today - timedelta(days=1)).isoformat()

        self.guard.checkpoint(TaskKind.HEAVY, "screening")
        indicators = load_indicators_cached(self.db, self.state, max_date)
        boards_res = self.state.latest_boards
        boards = None
        if boards_res is not None and boards_res.fetched_at.astimezone(BEIJING_TZ).date() == ctx.today \
                and boards_res.data_status != DataStatus.INVALID:
            boards = boards_res.df
        members = load_members_cached(self.db, self.state)
        idx_prices: dict[str, float] = {}
        if self.state.latest_index is not None:
            idx_df = self.state.latest_index.df
            idx_prices = {str(c): float(p) for c, p in zip(idx_df["code"], idx_df["price"], strict=True)
                          if pd.notna(p)}
        market = build_market_context(self.state.latest_breadth, idx_prices, self.index_ma20(max_date))

        scored = score_universe(spot.df, indicators, boards, members, market, self.s.screener,
                                set(self.s.watchlist.codes))
        result = rank_results(scored, self.s.screener)

        run_at = now_utc()
        run_id = f"{mode}-{to_db_ts(run_at)}"
        ind_date = None if indicators.empty else str(indicators["trade_date"].max())
        out = result.copy()
        out["run_id"] = run_id
        out["run_at"] = to_db_ts(run_at)
        out["spot_fetched_at"] = to_db_ts(spot.fetched_at)
        out["spot_data_status"] = spot.data_status
        out["model_version"] = self.s.screener.model_version
        cols = ["run_id", "run_at"] + RESULT_COLUMNS + ["spot_fetched_at", "spot_data_status", "model_version"]
        self.db.insert_df("screen_results", out, cols, mode="REPLACE")
        weights = {
            "sector": self.s.screener.weight_sector,
            "volume_price": self.s.screener.weight_volume_price,
            "market": self.s.screener.weight_market,
            "catalyst": self.s.screener.weight_catalyst,
            "risk_reward": self.s.screener.weight_risk_reward,
        }
        eligible_count = int((scored["filter_reasons"] == "").sum())
        self.db.insert_rows(
            "screen_runs",
            ["run_id", "run_at", "model_version", "weights_json", "universe_size", "eligible_count",
             "saved_count", "spot_fetched_at", "spot_data_status", "indicator_trade_date",
             "market_context_json", "status", "note"],
            [[run_id, to_db_ts(run_at), self.s.screener.model_version, json.dumps(weights),
              int(len(scored)), eligible_count, int(len(out)), to_db_ts(spot.fetched_at), spot.data_status,
              ind_date, json.dumps(market.as_dict(), ensure_ascii=False), "success", mode]])
        self.state.latest_screen_run = {"run_id": run_id, "run_at": run_at, "mode": mode,
                                        "saved": int(len(out)), "indicator_trade_date": ind_date}
        self.state.dirty.add("candidates")
        logger.info("筛选完成 %s：全市场 %d，可排名 %d，保存 %d", run_id, len(scored), eligible_count, len(out))
        return JobResult.DONE
