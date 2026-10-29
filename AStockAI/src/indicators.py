"""技术指标计算（纯函数，无网络、无数据库依赖）。

记号：t 为当前交易日，C/H/L/V 分别为收盘价/最高价/最低价/成交量（手），
SMA_n(X)_t = (X_t + X_{t-1} + ... + X_{t-n+1}) / n。

一、均线与成交量
- MA5/MA10/MA20/MA60：MA_n_t = SMA_n(C)_t，样本不足 n 时为空
- vol_ma5 / vol_ma20：SMA_5(V)_t、SMA_20(V)_t（包含当日）
- volume_ratio_5d（日线量比）：V_t / SMA_5(V)_{t-1}，即当日量 ÷ 前5日平均量。
  注：交易所/行情软件盘中“量比”是按分钟折算的口径，由数据源直接提供（spot.volume_ratio），
  两者口径不同，字段名已区分。
- turnover_rate：数据源提供的换手率（%），不在本地重新计算。

二、RSI（Wilder，N=14）
- ΔC_t = C_t - C_{t-1}；U_t = max(ΔC_t, 0)；D_t = max(-ΔC_t, 0)
- AU_t = AU_{t-1} + (U_t - AU_{t-1}) / N，AD 同理（Wilder 平滑，即 α=1/N 的指数平均，
  以第一个有效值为初值；与“以前 N 日简单平均为初值”的经典写法在前期略有差异，约 3N 日后收敛）
- RSI_t = 100 - 100 / (1 + AU_t / AD_t)；AD_t=0 且 AU_t>0 时为 100，二者均为 0 时为 50

三、MACD(12, 26, 9)
- EMA_n(X)_t = α·X_t + (1-α)·EMA_n(X)_{t-1}，α = 2/(n+1)，以第一个值为初值
- DIF = EMA_12(C) - EMA_26(C)；DEA = EMA_9(DIF)；MACD柱 = 2 × (DIF - DEA)（国内行情软件惯例）

四、区间高低点、支撑与压力（参考位，不是预测）
- high_20 / low_20：最近20个交易日（含当日）最高价最大值 / 最低价最小值；high_60 / low_60 同理
- prior_high_20 / prior_low_20：不含当日的前20个交易日区间，用于当日突破/跌破判断
- 经典枢轴点（用于下一交易日参考）：P = (H_t + L_t + C_t)/3，S1 = 2P - H_t，R1 = 2P - L_t

五、相对强弱、波动率与趋势
- ret_20 = C_t / C_{t-20} - 1（按该股票自身的第 t 与 t-20 根K线）
- benchmark_ret_20：基准指数在“同样两个日期”的收盘价之比 - 1（日期对齐，避免停牌造成窗口错位）
- rs_20 = ret_20 - benchmark_ret_20
- volatility_20 = std(ln(C_t/C_{t-1}) 的最近20个值, ddof=1) × sqrt(252)（年化）
- ma20_slope_5 = MA20_t / MA20_{t-5} - 1

六、信号（输入不足时为空，而不是 0）
- sig_breakout_volume（放量突破）：C_t > prior_high_20 且 V_t ≥ k1 × SMA_5(V)_{t-1}（k1 默认 1.8）
- sig_pullback_low_volume（缩量回踩）：ma20_slope_5 > 0 且 C_t ≥ MA20_t 且
  L_t ≤ MA10_t × (1 + tol) 且 C_t < C_{t-1} 且 V_t ≤ k2 × SMA_5(V)_{t-1}（k2 默认 0.7，tol 默认 2%）
- sig_trend_weakening（趋势转弱）：满足其一
  (a) 收盘跌破 MA20：C_t < MA20_t 且 C_{t-1} ≥ MA20_{t-1}
  (b) MA5 下穿 MA10 且 C_t < MA20_t
  (c) MACD柱由非负转负 且 C_t < MA20_t

防未来函数：所有计算只使用 rolling / shift(正数) / 递推平均，均只依赖 t 及之前的数据；
不使用 shift(负数)、center=True 或全样本统计量。单元测试中有“截断未来数据后历史值不变”的检查。

注意：技术指标与信号只是对历史价格/成交量的客观描述，不构成买卖结论。
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from config import IndicatorConfig
from memory_guard import TaskKind
from timeutil import now_utc, to_db_ts

if TYPE_CHECKING:
    from config import Settings
    from database import Database
    from memory_guard import MemoryGuard
    from scheduler import JobContext, JobResult
    from state import RuntimeState

logger = logging.getLogger(__name__)

MA_WINDOWS = (5, 10, 20, 60)
RSI_PERIOD = 14
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
RANGE_WINDOW = 20
LONG_RANGE_WINDOW = 60
RS_WINDOW = 20
VOLATILITY_WINDOW = 20
SLOPE_LAG = 5

INDICATOR_COLUMNS = [
    "code", "trade_date", "bars_available", "close",
    "ma5", "ma10", "ma20", "ma60", "vol_ma5", "vol_ma20", "volume_ratio_5d", "turnover_rate",
    "rsi14", "macd_dif", "macd_dea", "macd_hist",
    "high_20", "low_20", "prior_high_20", "prior_low_20", "high_60", "low_60",
    "pivot", "support_s1", "resistance_r1",
    "ret_20", "benchmark_ret_20", "rs_20", "volatility_20", "ma20_slope_5",
    "sig_breakout_volume", "sig_pullback_low_volume", "sig_trend_weakening",
]

REQUIRED_BAR_COLUMNS = ("code", "trade_date", "high", "low", "close", "volume_lots")


def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(window=n, min_periods=n).mean()


def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False, min_periods=n).mean()


def rsi_wilder(close: pd.Series, n: int = RSI_PERIOD) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    avg_loss = loss.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    with np.errstate(divide="ignore", invalid="ignore"):
        rs = avg_gain / avg_loss
        rsi = 100.0 - 100.0 / (1.0 + rs)
    rsi = rsi.where(avg_loss != 0, np.where(avg_gain > 0, 100.0, 50.0))
    return rsi.where(avg_gain.notna() & avg_loss.notna())


def macd(close: pd.Series, fast: int = MACD_FAST, slow: int = MACD_SLOW,
         signal: int = MACD_SIGNAL) -> tuple[pd.Series, pd.Series, pd.Series]:
    dif = ema(close, fast) - ema(close, slow)
    dea = ema(dif, signal)
    hist = 2.0 * (dif - dea)
    return dif, dea, hist


def _flag(cond: pd.Series, *inputs: pd.Series) -> pd.Series:
    """布尔条件转为 1.0/0.0；任一输入为空时结果为空（数据不足不等于“没有信号”）。"""
    valid = pd.Series(True, index=cond.index)
    for s in inputs:
        valid &= s.notna()
    return cond.astype(float).where(valid)


def compute_indicators(bars: pd.DataFrame, benchmark_close: pd.Series | None,
                       cfg: IndicatorConfig) -> pd.DataFrame:
    """计算单只股票全部指标。

    bars：单只股票日K，至少包含 REQUIRED_BAR_COLUMNS，可选 turnover_rate；
    benchmark_close：以 trade_date（YYYY-MM-DD）为索引的基准指数收盘价，可为 None。
    返回与 bars 行数相同、按 trade_date 升序的指标表（列为 INDICATOR_COLUMNS）。
    """
    missing = [c for c in REQUIRED_BAR_COLUMNS if c not in bars.columns]
    if missing:
        raise ValueError(f"日K缺少字段: {missing}")
    if bars["code"].nunique() > 1:
        raise ValueError("compute_indicators 只接受单只股票的数据")

    g = bars.sort_values("trade_date").reset_index(drop=True)
    close = g["close"].astype(float)
    high = g["high"].astype(float)
    low = g["low"].astype(float)
    vol = g["volume_lots"].astype(float)

    out = pd.DataFrame({"code": g["code"], "trade_date": g["trade_date"]})
    out["bars_available"] = np.arange(1, len(g) + 1)
    out["close"] = close
    for n in MA_WINDOWS:
        out[f"ma{n}"] = sma(close, n)
    out["vol_ma5"] = sma(vol, 5)
    out["vol_ma20"] = sma(vol, 20)
    prev_vol_ma5 = sma(vol, 5).shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        out["volume_ratio_5d"] = (vol / prev_vol_ma5).replace([np.inf, -np.inf], np.nan)
    out["turnover_rate"] = (g["turnover_rate"].astype(float) if "turnover_rate" in g.columns
                            else np.nan)

    out["rsi14"] = rsi_wilder(close, RSI_PERIOD)
    dif, dea, hist = macd(close)
    out["macd_dif"], out["macd_dea"], out["macd_hist"] = dif, dea, hist

    out["high_20"] = high.rolling(RANGE_WINDOW, min_periods=RANGE_WINDOW).max()
    out["low_20"] = low.rolling(RANGE_WINDOW, min_periods=RANGE_WINDOW).min()
    out["prior_high_20"] = high.shift(1).rolling(RANGE_WINDOW, min_periods=RANGE_WINDOW).max()
    out["prior_low_20"] = low.shift(1).rolling(RANGE_WINDOW, min_periods=RANGE_WINDOW).min()
    out["high_60"] = high.rolling(LONG_RANGE_WINDOW, min_periods=LONG_RANGE_WINDOW).max()
    out["low_60"] = low.rolling(LONG_RANGE_WINDOW, min_periods=LONG_RANGE_WINDOW).min()

    pivot = (high + low + close) / 3.0
    out["pivot"] = pivot
    out["support_s1"] = 2.0 * pivot - high
    out["resistance_r1"] = 2.0 * pivot - low

    out["ret_20"] = close / close.shift(RS_WINDOW) - 1.0
    if benchmark_close is not None and len(benchmark_close) > 0:
        bench = benchmark_close.astype(float)
        b_now = out["trade_date"].map(bench)
        b_then = out["trade_date"].shift(RS_WINDOW).map(bench)
        out["benchmark_ret_20"] = b_now / b_then - 1.0
    else:
        out["benchmark_ret_20"] = np.nan
    out["rs_20"] = out["ret_20"] - out["benchmark_ret_20"]

    with np.errstate(divide="ignore", invalid="ignore"):
        log_ret = np.log(close / close.shift(1))
    out["volatility_20"] = (log_ret.rolling(VOLATILITY_WINDOW, min_periods=VOLATILITY_WINDOW).std(ddof=1)
                            * np.sqrt(cfg.trading_days_per_year))
    out["ma20_slope_5"] = out["ma20"] / out["ma20"].shift(SLOPE_LAG) - 1.0

    # ---- 信号 ----
    ma5, ma10, ma20 = out["ma5"], out["ma10"], out["ma20"]
    out["sig_breakout_volume"] = _flag(
        (close > out["prior_high_20"]) & (vol >= cfg.breakout_volume_multiple * prev_vol_ma5),
        out["prior_high_20"], prev_vol_ma5)

    prev_close = close.shift(1)
    out["sig_pullback_low_volume"] = _flag(
        (out["ma20_slope_5"] > 0)
        & (close >= ma20)
        & (low <= ma10 * (1.0 + cfg.pullback_ma_tolerance))
        & (close < prev_close)
        & (vol <= cfg.pullback_volume_multiple * prev_vol_ma5),
        out["ma20_slope_5"], ma20, ma10, prev_close, prev_vol_ma5)

    cross_below_ma20 = (close < ma20) & (prev_close >= ma20.shift(1))
    ma5_dead_cross = (ma5 < ma10) & (ma5.shift(1) >= ma10.shift(1)) & (close < ma20)
    hist_turn_neg = (hist < 0) & (hist.shift(1) >= 0) & (close < ma20)
    out["sig_trend_weakening"] = _flag(
        cross_below_ma20 | ma5_dead_cross | hist_turn_neg,
        ma20, ma20.shift(1), ma10.shift(1), hist.shift(1))

    return out.loc[:, INDICATOR_COLUMNS]


def compute_batch(bars_all: pd.DataFrame, benchmark_close: pd.Series | None,
                  cfg: IndicatorConfig, keep_last: int) -> pd.DataFrame:
    """对多只股票逐只计算，只保留每只股票最近 keep_last 行，控制输出体积。"""
    parts: list[pd.DataFrame] = []
    for _code, g in bars_all.groupby("code", sort=False):
        if g.empty:
            continue
        res = compute_indicators(g, benchmark_close, cfg)
        parts.append(res.tail(keep_last))
    if not parts:
        return pd.DataFrame(columns=INDICATOR_COLUMNS)
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# 批量计算服务（数据库读写 + 分批 + 内存检查点）
# ---------------------------------------------------------------------------

INDICATOR_DB_COLUMNS = INDICATOR_COLUMNS + ["computed_at", "adjust", "source"]


def load_latest_indicators(db: "Database", max_trade_date: str) -> pd.DataFrame:
    """读取每只股票在 max_trade_date（含）之前最近一个交易日的指标。

    盘中调用时传入“今天之前”的日期，保证只使用已经收盘、确定可得的数据（无未来函数）。
    """
    sql = (
        "SELECT iv.* FROM indicator_values AS iv"
        " JOIN (SELECT code, MAX(trade_date) AS d FROM indicator_values"
        "       WHERE trade_date <= ? GROUP BY code) AS m"
        " ON iv.code = m.code AND iv.trade_date = m.d"
    )
    return db.query_df(sql, (max_trade_date,))


class IndicatorService:
    """收盘后按批计算指标：每次运行处理 indicator_batch_size 只股票，进度保存在 job_state。"""

    DAILY_JOB_NAME = "daily_bars"

    def __init__(self, settings: "Settings", db: "Database", guard: "MemoryGuard",
                 state: "RuntimeState") -> None:
        self.s = settings
        self.db = db
        self.guard = guard
        self.state = state
        self._bench_key: str | None = None
        self._bench: pd.Series | None = None

    def _benchmark(self, start: str, end: str) -> pd.Series | None:
        key = f"{start}:{end}"
        if self._bench_key != key:
            df = self.db.query_df(
                "SELECT trade_date, close FROM index_daily WHERE code = ? AND trade_date >= ?"
                " AND trade_date <= ? ORDER BY trade_date",
                (self.s.datasource.benchmark_index, start, end))
            self._bench = (df.drop_duplicates("trade_date").set_index("trade_date")["close"]
                           if not df.empty else None)
            self._bench_key = key
        return self._bench

    def run(self, ctx: "JobContext") -> "JobResult":
        from scheduler import JobResult, Scheduler

        target = ctx.target_day
        if target is None:
            return JobResult.SKIPPED
        target_s = target.isoformat()
        if self.db.get_state(Scheduler.done_key(self.DAILY_JOB_NAME, target_s)) is None:
            return JobResult.SKIPPED  # 日K尚未更新完成
        adjust = self.s.datasource.daily_adjust
        recent = (target - timedelta(days=15)).isoformat()
        codes = [r["code"] for r in self.db.query(
            "SELECT DISTINCT code FROM daily_bars WHERE adjust = ? AND trade_date >= ? AND trade_date <= ?"
            " ORDER BY code", (adjust, recent, target_s))]
        offset_key = f"indicators_offset:{target_s}"
        offset = int(self.db.get_state(offset_key) or 0)
        if offset >= len(codes):
            self.state.indicator_cache = None
            self.state.indicator_cache_key = None
            self.state.dirty.add("indicators")
            self._bench = None
            self._bench_key = None
            logger.info("指标计算完成：目标日 %s，共 %d 只股票", target_s, len(codes))
            return JobResult.DONE

        self.guard.checkpoint(TaskKind.HEAVY, "indicators")
        batch = codes[offset: offset + self.s.schedule.indicator_batch_size]
        start_s = (target - timedelta(days=self.s.indicators.history_load_days)).isoformat()
        placeholders = ",".join("?" for _ in batch)
        bars = self.db.query_df(
            "SELECT code, trade_date, open, high, low, close, volume_lots, turnover_rate"
            " FROM daily_bars WHERE adjust = ? AND trade_date >= ? AND trade_date <= ?"
            f" AND code IN ({placeholders}) ORDER BY code, trade_date",
            (adjust, start_s, target_s, *batch))
        result = compute_batch(bars, self._benchmark(start_s, target_s), self.s.indicators,
                               self.s.indicators.store_recent_rows)
        del bars
        if not result.empty:
            result["computed_at"] = to_db_ts(now_utc())
            result["adjust"] = adjust
            result["source"] = f"local:indicators.py(daily_bars adjust={adjust or 'none'})"
            self.db.insert_df("indicator_values", result, INDICATOR_DB_COLUMNS, mode="REPLACE")
        self.db.set_state(offset_key, str(offset + len(batch)))
        return JobResult.MORE
