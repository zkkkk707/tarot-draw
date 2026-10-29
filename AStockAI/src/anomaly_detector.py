"""盘中异动检测：只记录客观事件，不做分析、不生成推荐、不发送通知。

输入
- 最新全A快照（价格、最高、最低、昨收、涨跌幅、量比、成交额）
- 前一交易日收盘后计算的指标（均线、20日区间、MA20斜率），不使用当日未收盘日K
- 当日行业板块快照与行业成分（可选）
- 市场宽度及当日宽度历史

事件定义（阈值均可在 settings.yaml 的 anomaly 段调整，并随事件一起记录）
- VOLUME_BREAKOUT 放量突破：price > high_20 × (1 + breakout_buffer)，且量比 ≥ breakout_volume_ratio，
  且涨跌幅 > 0。high_20 为截至上一交易日的20日最高价。
- LOW_VOLUME_PULLBACK 缩量回踩：ma20_slope_5 > 0（上一交易日MA20上行），当日涨跌幅 < 0，
  量比 ≤ pullback_volume_ratio_max，且价格距 MA10 或 MA20 不超过 pullback_ma_tolerance，
  且 price ≥ MA20 × (1 - pullback_ma_tolerance)。
- SUPPORT_BREAK 跌破关键支撑：price < low_20 × (1 - support_break_buffer)，low_20 为截至上一交易日的20日最低价。
- SPIKE_FADE 冲高回落：(最高价/昨收 - 1)×100 ≥ spike_min_high_pct，且 (最高价 - 现价)/昨收×100 ≥ spike_min_fade_pct。
- SECTOR_RESONANCE 板块共振（板块级事件，code 为板块代码）：板块涨幅 ≥ sector_min_pct，
  上涨家数占比 ≥ sector_min_up_ratio，且（有成分数据时）涨幅 ≥ sector_member_min_pct 的成分股
  不少于 sector_min_strong_members 只。
- BREADTH_DETERIORATION 市场宽度恶化（市场级事件，code=MARKET）：上涨家数占比 ≤ breadth_bad_ratio，
  或较当日盘中最高值回落 ≥ breadth_drop_from_peak。

去重：同一交易日、同一事件类型、同一代码只记录第一次触发（dedup_key 唯一约束）。
数据状态为 stale / invalid 的快照不做检测。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Iterable

import numpy as np
import pandas as pd

from config import AnomalyConfig
from market_data import DataStatus, FetchResult
from timeutil import BEIJING_TZ, date_str, to_db_ts

if TYPE_CHECKING:
    from config import Settings
    from database import Database
    from scheduler import JobContext, JobResult
    from state import RuntimeState

logger = logging.getLogger(__name__)

ANOMALY_DB_COLUMNS = [
    "dedup_key", "trade_date", "code", "name", "event_type", "triggered_at", "data_time", "time_basis",
    "source", "price", "metrics_json", "thresholds_json", "data_status", "indicator_trade_date",
]


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if (np.isnan(f) or np.isinf(f)) else round(f, 6)


def _event(trade_date: str, code: str, name: str, event_type: str, spot: FetchResult, price: Any,
           metrics: dict[str, Any], thresholds: dict[str, Any], indicator_date: Any,
           source_extra: str = "") -> dict[str, Any]:
    return {
        "dedup_key": f"{trade_date}:{event_type}:{code}",
        "trade_date": trade_date,
        "code": code,
        "name": name,
        "event_type": event_type,
        "triggered_at": to_db_ts(spot.fetched_at),
        "data_time": to_db_ts(spot.quote_time) if spot.quote_time else None,
        "time_basis": spot.time_basis,
        "source": spot.source + (f"+{source_extra}" if source_extra else ""),
        "price": _num(price),
        "metrics_json": json.dumps({k: _num(v) if not isinstance(v, str) else v for k, v in metrics.items()},
                                   ensure_ascii=False),
        "thresholds_json": json.dumps(thresholds, ensure_ascii=False),
        "data_status": spot.data_status,
        "indicator_trade_date": None if indicator_date is None or pd.isna(indicator_date) else str(indicator_date),
    }


def detect_stock_events(spot: FetchResult, indicators: pd.DataFrame, cfg: AnomalyConfig,
                        trade_date: str) -> list[dict[str, Any]]:
    df = spot.df
    base = df.loc[df["price"].notna() & (df["validation_flags"].fillna("") == "")
                  & (df["amount_yuan"] >= cfg.min_amount_yuan)]
    if base.empty:
        return []
    ind_cols = ["code", "trade_date", "ma10", "ma20", "high_20", "low_20", "ma20_slope_5"]
    if indicators.empty:
        ind = pd.DataFrame(columns=ind_cols)
    else:
        ind = indicators.loc[:, ind_cols]
    m = base.merge(ind.rename(columns={"trade_date": "indicator_trade_date"}), on="code", how="left")
    m = m.sort_values("amount_yuan", ascending=False)
    price = m["price"].astype(float)
    vr = m["volume_ratio"].astype(float)
    pct = m["pct_change"].astype(float)
    high20 = m["high_20"].astype(float)
    low20 = m["low_20"].astype(float)
    ma10 = m["ma10"].astype(float)
    ma20 = m["ma20"].astype(float)
    slope = m["ma20_slope_5"].astype(float)
    hi = m["high"].astype(float)
    prev = m["prev_close"].astype(float)

    events: list[dict[str, Any]] = []

    mask = (price > high20 * (1 + cfg.breakout_buffer)) & (vr >= cfg.breakout_volume_ratio) & (pct > 0)
    th = {"price_gt_high_20_times": 1 + cfg.breakout_buffer, "volume_ratio_min": cfg.breakout_volume_ratio,
          "pct_change_gt": 0}
    for r in m.loc[mask.fillna(False)].itertuples(index=False):
        events.append(_event(trade_date, r.code, r.name, "VOLUME_BREAKOUT", spot, r.price,
                             {"high_20_prev_close_basis": r.high_20, "volume_ratio": r.volume_ratio,
                              "pct_change": r.pct_change, "amount_yuan": r.amount_yuan},
                             th, r.indicator_trade_date, "indicators"))

    with np.errstate(divide="ignore", invalid="ignore"):
        d10 = (price - ma10).abs() / ma10
        d20 = (price - ma20).abs() / ma20
    tol = cfg.pullback_ma_tolerance
    mask = ((slope > 0) & (pct < 0) & (vr <= cfg.pullback_volume_ratio_max)
            & ((d10 <= tol) | (d20 <= tol)) & (price >= ma20 * (1 - tol)))
    th = {"ma20_slope_5_gt": 0, "pct_change_lt": 0, "volume_ratio_max": cfg.pullback_volume_ratio_max,
          "distance_to_ma10_or_ma20_max": tol}
    for r, a, b in zip(m.loc[mask.fillna(False)].itertuples(index=False), d10[mask.fillna(False)],
                       d20[mask.fillna(False)], strict=True):
        events.append(_event(trade_date, r.code, r.name, "LOW_VOLUME_PULLBACK", spot, r.price,
                             {"ma10": r.ma10, "ma20": r.ma20, "ma20_slope_5": r.ma20_slope_5,
                              "distance_to_ma10": a, "distance_to_ma20": b,
                              "volume_ratio": r.volume_ratio, "pct_change": r.pct_change},
                             th, r.indicator_trade_date, "indicators"))

    mask = price < low20 * (1 - cfg.support_break_buffer)
    th = {"price_lt_low_20_times": 1 - cfg.support_break_buffer}
    for r in m.loc[mask.fillna(False)].itertuples(index=False):
        events.append(_event(trade_date, r.code, r.name, "SUPPORT_BREAK", spot, r.price,
                             {"low_20_prev_close_basis": r.low_20, "pct_change": r.pct_change,
                              "volume_ratio": r.volume_ratio},
                             th, r.indicator_trade_date, "indicators"))

    with np.errstate(divide="ignore", invalid="ignore"):
        high_pct = (hi / prev - 1.0) * 100.0
        fade_pct = (hi - price) / prev * 100.0
    mask = (high_pct >= cfg.spike_min_high_pct) & (fade_pct >= cfg.spike_min_fade_pct)
    th = {"high_pct_min": cfg.spike_min_high_pct, "fade_pct_min": cfg.spike_min_fade_pct}
    for r, hp, fp in zip(m.loc[mask.fillna(False)].itertuples(index=False), high_pct[mask.fillna(False)],
                         fade_pct[mask.fillna(False)], strict=True):
        events.append(_event(trade_date, r.code, r.name, "SPIKE_FADE", spot, r.price,
                             {"high": r.high, "prev_close": r.prev_close, "high_pct": hp,
                              "fade_pct_of_prev_close": fp, "pct_change": r.pct_change},
                             th, None))
    return events


def detect_sector_events(spot: FetchResult, boards: FetchResult | None, members: pd.DataFrame | None,
                         cfg: AnomalyConfig, trade_date: str) -> list[dict[str, Any]]:
    if boards is None or boards.df.empty or boards.data_status == DataStatus.INVALID:
        return []
    b = boards.df
    denom = (b["up_count"] + b["down_count"]).astype(float)
    up_ratio = (b["up_count"] / denom).where(denom > 0)
    cand = b.loc[(b["pct_change"] >= cfg.sector_min_pct) & (up_ratio >= cfg.sector_min_up_ratio)]
    if cand.empty:
        return []
    strong_counts: dict[str, int] = {}
    have_members = members is not None and not members.empty
    if have_members:
        strong = spot.df.loc[spot.df["pct_change"] >= cfg.sector_member_min_pct, ["code"]]
        joined = strong.merge(members.loc[:, ["code", "board_code"]].drop_duplicates("code"), on="code")
        strong_counts = joined.groupby("board_code").size().to_dict()
    th = {"board_pct_min": cfg.sector_min_pct, "up_ratio_min": cfg.sector_min_up_ratio,
          "strong_member_pct_min": cfg.sector_member_min_pct,
          "strong_members_min": cfg.sector_min_strong_members if have_members else "not_applied_no_members"}
    events = []
    for r in cand.itertuples(index=False):
        n_strong = strong_counts.get(r.board_code, 0)
        if have_members and n_strong < cfg.sector_min_strong_members:
            continue
        ur = float(r.up_count) / float(r.up_count + r.down_count)
        # 板块事件以板块快照的获取时间为触发时间
        ev = _event(trade_date, str(r.board_code), str(r.board_name), "SECTOR_RESONANCE", boards, r.price,
                    {"board_pct_change": r.pct_change, "up_count": r.up_count, "down_count": r.down_count,
                     "up_ratio": ur, "strong_members": n_strong if have_members else "unavailable",
                     "leader_name": str(r.leader_name), "leader_pct_change": r.leader_pct_change},
                    th, None, "industry_members" if have_members else "")
        events.append(ev)
    return events


def detect_breadth_event(spot: FetchResult, breadth: dict[str, Any] | None,
                         history: Iterable[tuple[datetime, float]], cfg: AnomalyConfig,
                         trade_date: str) -> list[dict[str, Any]]:
    if breadth is None or breadth.get("advance_ratio") is None:
        return []
    current = float(breadth["advance_ratio"])
    ratios = [r for _, r in history]
    peak = max(ratios) if ratios else current
    drop = peak - current
    if current > cfg.breadth_bad_ratio and drop < cfg.breadth_drop_from_peak:
        return []
    return [_event(trade_date, "MARKET", "全市场", "BREADTH_DETERIORATION", spot, None,
                   {"advance_ratio": current, "intraday_peak_ratio": peak, "drop_from_peak": drop,
                    "up": breadth.get("up"), "down": breadth.get("down"),
                    "limit_down_approx": breadth.get("limit_down_approx")},
                   {"advance_ratio_max": cfg.breadth_bad_ratio, "drop_from_peak_min": cfg.breadth_drop_from_peak},
                   None, "derived_breadth")]


class AnomalyService:
    def __init__(self, settings: "Settings", db: "Database", state: "RuntimeState") -> None:
        self.s = settings
        self.db = db
        self.state = state

    def run(self, ctx: "JobContext") -> "JobResult":
        from scheduler import JobResult
        from screener import load_indicators_cached, load_members_cached

        spot = self.state.latest_spot
        if spot is None or spot.fetched_at == self.state.last_anomaly_spot_at:
            return JobResult.SKIPPED
        if spot.fetched_at.astimezone(BEIJING_TZ).date() != ctx.today:
            return JobResult.SKIPPED
        self.state.last_anomaly_spot_at = spot.fetched_at
        if spot.data_status in (DataStatus.STALE, DataStatus.INVALID):
            logger.warning("快照状态为 %s，跳过本轮异动检测", spot.data_status)
            return JobResult.SKIPPED
        trade_date = date_str(ctx.today)
        # 只使用上一交易日及更早已收盘的指标，避免使用当日未完成K线
        indicators = load_indicators_cached(self.db, self.state, (ctx.today - timedelta(days=1)).isoformat())
        members = load_members_cached(self.db, self.state)
        boards = self.state.latest_boards
        if boards is not None and boards.fetched_at.astimezone(BEIJING_TZ).date() != ctx.today:
            boards = None
        cfg = self.s.anomaly
        events = (detect_breadth_event(spot, self.state.latest_breadth, self.state.breadth_history, cfg, trade_date)
                  + detect_sector_events(spot, boards, members, cfg, trade_date)
                  + detect_stock_events(spot, indicators, cfg, trade_date))
        # 先去掉当日已记录过的事件，再应用条数上限，避免已存在的事件持续占用名额
        existing = {r["dedup_key"] for r in self.db.query(
            "SELECT dedup_key FROM anomalies WHERE trade_date = ?", (trade_date,))}
        events = [e for e in events if e["dedup_key"] not in existing]
        if len(events) > cfg.max_events_per_run:
            logger.warning("本轮异动 %d 条超过上限 %d，按市场/板块/成交额优先保留", len(events), cfg.max_events_per_run)
            events = events[: cfg.max_events_per_run]
        if not events:
            return JobResult.DONE
        before = self.db.conn.total_changes
        self.db.insert_rows("anomalies", ANOMALY_DB_COLUMNS,
                            ([e[c] for c in ANOMALY_DB_COLUMNS] for e in events), mode="IGNORE")
        new = self.db.conn.total_changes - before
        if new > 0:
            self.state.anomalies_since_export += new
            self.state.dirty.add("alerts")
            logger.info("记录新异动 %d 条", new)
        return JobResult.DONE
