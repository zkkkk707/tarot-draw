"""行情采集编排：调用数据源适配器、判定数据状态、写入数据库、更新运行期状态。

数据状态判定规则（写入每条记录的 data_status / time_basis）
- 全A快照、指数快照、行业板块：数据源无行情时间戳 → time_basis=fetch_time_only，
  data_status=unknown_time；若连续交易时段内内容在 stale_after_sec 内完全不变 → stale；
  行数不足等校验失败 → invalid（不入库、不替换内存中的上一份数据，并记录失败）。
- 自选股分钟K线：有K线时间 → 交易时段内按延迟判定 realtime / delayed，非交易时段为 historical。
- 日K / 指数日K：historical。

失败处理：任何采集失败都会写入 collection_runs（status=failed）与运行期错误列表，
不会用旧数据冒充新数据——旧数据仍保留原来的 fetched_at，导出文件会因超过有效期被标记为 stale。
"""

from __future__ import annotations

import json
import logging
import time as _time
from datetime import date, datetime, time, timedelta
from typing import Any, Callable

import numpy as np
import pandas as pd

from config import Settings
from database import Database, WritePausedError
from market_data import (
    CircuitOpenError,
    DataSourceError,
    DataStatus,
    DataValidationError,
    FetchResult,
    MarketDataAdapter,
    classify_bar_freshness,
    is_bse,
    price_limit_pct,
)
from memory_guard import MemoryGuard, TaskKind
from scheduler import JobContext, JobResult
from state import RuntimeState
from timeutil import BEIJING_TZ, date_str, now_utc, to_bj_iso, to_db_ts
from trading_calendar import TRADING_PHASES

logger = logging.getLogger(__name__)

SPOT_DB_COLUMNS = [
    "snapshot_id", "code", "name", "price", "pct_change", "change", "volume_lots", "amount_yuan",
    "amplitude_pct", "high", "low", "open", "prev_close", "volume_ratio", "turnover_rate",
    "pe_dynamic", "pb", "total_mv_yuan", "float_mv_yuan", "quote_time", "fetched_at", "source",
    "time_basis", "data_status", "validation_flags",
]
INDEX_SNAP_DB_COLUMNS = [
    "snapshot_id", "code", "name", "price", "pct_change", "change", "volume", "amount_yuan", "high",
    "low", "open", "prev_close", "quote_time", "fetched_at", "source", "time_basis", "data_status",
]
BOARD_DB_COLUMNS = [
    "snapshot_id", "board_code", "board_name", "price", "pct_change", "turnover_rate",
    "total_mv_yuan", "up_count", "down_count", "leader_name", "leader_pct_change", "fetched_at",
    "source", "time_basis", "data_status",
]
BREADTH_DB_COLUMNS = [
    "snapshot_id", "fetched_at", "total", "traded", "up", "down", "flat", "suspended",
    "limit_up_approx", "limit_down_approx", "advance_ratio", "median_pct_change",
    "amount_total_yuan", "source", "derived_from", "data_status",
]
MINUTE_DB_COLUMNS = [
    "code", "period", "bar_time", "open", "high", "low", "close", "volume_lots", "amount_yuan",
    "fetched_at", "source",
]
DAILY_DB_COLUMNS = [
    "code", "adjust", "trade_date", "open", "high", "low", "close", "volume_lots", "amount_yuan",
    "amplitude_pct", "pct_change", "change", "turnover_rate", "fetched_at", "source",
]
INDEX_DAILY_DB_COLUMNS = [
    "code", "trade_date", "open", "high", "low", "close", "volume", "amount_yuan", "pct_change",
    "fetched_at", "source",
]
MEMBER_DB_COLUMNS = ["board_code", "board_name", "code", "name", "updated_at", "source"]

DAILY_MAX_CODE_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# 纯函数
# ---------------------------------------------------------------------------

def spot_signature(df: pd.DataFrame) -> str:
    """快照内容签名：代码、价格、成交量完全相同则签名相同。"""
    if df.empty:
        return "empty"
    hashed = pd.util.hash_pandas_object(df[["code", "price", "volume_lots"]], index=False)
    total = int(hashed.to_numpy(dtype=np.uint64).sum(dtype=np.uint64))
    return f"{len(df)}:{total:016x}"


def compute_breadth(df: pd.DataFrame) -> dict[str, Any]:
    """由全A快照派生市场宽度。涨停/跌停为近似统计（见 market_data.price_limit_pct）。"""
    total = int(len(df))
    if total == 0:
        return {"total": 0, "traded": 0, "up": 0, "down": 0, "flat": 0, "suspended": 0,
                "limit_up_approx": 0, "limit_down_approx": 0, "advance_ratio": None,
                "median_pct_change": None, "amount_total_yuan": None}
    traded_mask = df["price"].notna() & (df["volume_lots"].fillna(0) > 0)
    t = df.loc[traded_mask]
    pct = t["pct_change"]
    up = int((pct > 0).sum())
    down = int((pct < 0).sum())
    flat = int((pct == 0).sum())
    limits = np.array([price_limit_pct(c, n) for c, n in zip(t["code"], t["name"], strict=True)],
                      dtype=float) / 100.0
    prev_close = t["prev_close"].to_numpy(dtype=float)
    price = t["price"].to_numpy(dtype=float)
    with np.errstate(invalid="ignore"):
        up_price = np.round(prev_close * (1.0 + limits) + 1e-9, 2)
        down_price = np.round(prev_close * (1.0 - limits) + 1e-9, 2)
        limit_up = int(np.nansum((price >= up_price - 1e-6) & ~np.isnan(prev_close)))
        limit_down = int(np.nansum((price <= down_price + 1e-6) & ~np.isnan(prev_close)))
    amount = t["amount_yuan"].sum(min_count=1)
    median = pct.median() if len(pct.dropna()) else np.nan
    return {
        "total": total,
        "traded": int(traded_mask.sum()),
        "up": up,
        "down": down,
        "flat": flat,
        "suspended": int(total - traded_mask.sum()),
        "limit_up_approx": limit_up,
        "limit_down_approx": limit_down,
        "advance_ratio": (up / (up + down)) if (up + down) > 0 else None,
        "median_pct_change": None if pd.isna(median) else float(median),
        "amount_total_yuan": None if pd.isna(amount) else float(amount),
    }


# ---------------------------------------------------------------------------
# 采集器
# ---------------------------------------------------------------------------

class MarketCollector:
    def __init__(self, settings: Settings, db: Database, adapter: MarketDataAdapter,
                 guard: MemoryGuard, state: RuntimeState,
                 clock: Callable[[], float] = _time.monotonic) -> None:
        self.s = settings
        self.db = db
        self.adapter = adapter
        self.guard = guard
        self.state = state
        self._clock = clock
        self._member_attempts: dict[tuple[str, str], int] = {}
        self._last_pause_log = -1e18

    # -- 公共工具 -------------------------------------------------------------
    def _fail(self, dataset: str, started: str, exc: Exception, source: str | None = None) -> None:
        self.state.mark_failure(dataset, f"{type(exc).__name__}: {exc}")
        self.db.record_run(dataset, source or self.adapter.provider, started, "failed",
                           error_type=type(exc).__name__, error_message=str(exc))

    def _safe_store(self, what: str, fn: Callable[[], int]) -> int:
        """写库失败（容量暂停）时降级：数据仍保留在内存并导出，但不入库。"""
        try:
            return fn()
        except WritePausedError as exc:
            now = self._clock()
            if now - self._last_pause_log > 300:
                logger.warning("%s 未写入数据库: %s", what, exc)
                self._last_pause_log = now
            return 0

    @staticmethod
    def _with_meta(df: pd.DataFrame, res: FetchResult, snapshot_prefix: str | None) -> pd.DataFrame:
        out = df.copy()
        ts = to_db_ts(res.fetched_at)
        if snapshot_prefix is not None:
            out["snapshot_id"] = f"{snapshot_prefix}-{ts}"
        out["quote_time"] = to_db_ts(res.quote_time) if res.quote_time else None
        out["fetched_at"] = ts
        out["source"] = res.source
        out["time_basis"] = res.time_basis
        out["data_status"] = res.data_status
        return out

    # -- 全A快照 --------------------------------------------------------------
    def collect_spot(self, ctx: JobContext, purpose: str = "intraday") -> FetchResult:
        started = to_db_ts(now_utc())
        try:
            res = self.adapter.fetch_spot()
        except DataSourceError as exc:
            self._fail("spot", started, exc)
            raise
        if res.data_status == DataStatus.INVALID:
            exc = DataValidationError("spot", f"快照未通过校验: {res.warnings}")
            self._fail("spot", started, exc, res.source)
            raise exc

        self._assess_spot_freshness(res, ctx)
        self._crosscheck_with_minutes(res)

        mono = self._clock()
        full = purpose != "intraday" or (
            mono - self.state.last_full_spot_store_mono >= self.s.schedule.snapshot_store_interval_sec)
        if full:
            stored_df = res.df
        else:
            stored_df = res.df.loc[res.df["code"].isin(self.s.watchlist.codes)]
        n_stored = 0
        if not stored_df.empty:
            out = self._with_meta(stored_df, res, "spot")
            n_stored = self._safe_store("全A快照", lambda: self.db.insert_df(
                "spot_snapshots", out, SPOT_DB_COLUMNS, mode="REPLACE"))
        if full:
            self.state.last_full_spot_store_mono = mono

        breadth = compute_breadth(res.df)
        breadth_row = {
            "snapshot_id": f"breadth-{to_db_ts(res.fetched_at)}",
            "fetched_at": to_db_ts(res.fetched_at),
            **breadth,
            "source": res.source,
            "derived_from": f"spot-{to_db_ts(res.fetched_at)}",
            "data_status": res.data_status,
        }
        self._safe_store("市场宽度", lambda: self.db.insert_rows(
            "breadth_snapshots", BREADTH_DB_COLUMNS, [[breadth_row[c] for c in BREADTH_DB_COLUMNS]]))
        self._update_breadth_history(ctx, res, breadth)

        self.state.latest_spot = res
        self.state.latest_breadth = {**breadth, "fetched_at": res.fetched_at, "source": res.source,
                                     "data_status": res.data_status, "purpose": purpose}
        self.state.mark_success("spot", res.source, res.row_count, res.data_status)
        self.state.dirty.update({"stocks_latest", "market_latest"})

        if self.state.stocks_upserted_day != ctx.today:
            ts = to_db_ts(res.fetched_at)
            rows = list(zip(res.df["code"], res.df["name"], res.df["exchange"],
                            [ts] * len(res.df), [res.source] * len(res.df), strict=True))
            if self._safe_store("股票列表", lambda: self.db.upsert_stocks(rows)) or not self.db.write_paused:
                self.state.stocks_upserted_day = ctx.today

        self.db.record_run("spot", res.source, started, "success", row_count=res.row_count,
                           data_status=res.data_status,
                           detail=json.dumps({"purpose": purpose, "stored_rows": n_stored,
                                              "full_store": full, "warnings": res.warnings},
                                             ensure_ascii=False))
        return res

    def _assess_spot_freshness(self, res: FetchResult, ctx: JobContext) -> None:
        sig = spot_signature(res.df)
        phase = ctx.phase.value
        st = self.state
        if sig != st.spot_signature or st.spot_signature_since is None or st.spot_signature_phase != phase:
            st.spot_signature = sig
            st.spot_signature_since = res.fetched_at
            st.spot_signature_phase = phase
            return
        unchanged = (res.fetched_at - st.spot_signature_since).total_seconds()
        if ctx.phase in TRADING_PHASES and unchanged >= self.s.datasource.stale_after_sec:
            res.data_status = DataStatus.STALE
            res.warnings.append(f"content_unchanged_during_session_sec={int(unchanged)}")
            logger.warning("全A快照内容在交易时段内已 %d 秒未变化，标记为 stale", int(unchanged))

    def _crosscheck_with_minutes(self, res: FetchResult) -> None:
        """用带时间戳的自选股分钟线交叉核对快照价格（仅作参考信息，不修改数据）。"""
        if not self.state.watch_minutes:
            return
        prices = res.df.set_index("code")["price"]
        compared = matched = 0
        for code, info in self.state.watch_minutes.items():
            if info.get("data_status") != DataStatus.REALTIME or info.get("close") is None:
                continue
            p = prices.get(code)
            if p is None or pd.isna(p):
                continue
            compared += 1
            m = float(info["close"])
            if abs(float(p) - m) <= max(0.011, abs(m) * 0.002):
                matched += 1
        self.state.crosscheck = {
            "at": to_bj_iso(res.fetched_at),
            "compared": compared,
            "matched": matched,
            "note": "自选股快照价与最近一根实时分钟K收盘价比较（容差 max(0.01元, 0.2%)），"
                    "两者获取时间可能相差数十秒，仅作一致性参考",
        }

    def _update_breadth_history(self, ctx: JobContext, res: FetchResult, breadth: dict[str, Any]) -> None:
        st = self.state
        if st.breadth_history_day != ctx.today:
            st.breadth_history.clear()
            st.breadth_history_day = ctx.today
        if ctx.phase in TRADING_PHASES and breadth.get("advance_ratio") is not None:
            st.breadth_history.append((res.fetched_at, float(breadth["advance_ratio"])))

    # -- 指数与行业 -----------------------------------------------------------
    def collect_indexes(self, ctx: JobContext) -> FetchResult:
        started = to_db_ts(now_utc())
        try:
            res = self.adapter.fetch_index_spot(self.s.datasource.index_codes)
        except DataSourceError as exc:
            self._fail("index_spot", started, exc)
            raise
        if res.data_status == DataStatus.INVALID:
            exc = DataValidationError("index_spot", f"指数快照为空: {res.warnings}")
            self._fail("index_spot", started, exc, res.source)
            raise exc
        out = self._with_meta(res.df, res, "index")
        self._safe_store("指数快照", lambda: self.db.insert_df("index_snapshots", out, INDEX_SNAP_DB_COLUMNS))
        self.state.latest_index = res
        self.state.mark_success("index_spot", res.source, res.row_count, res.data_status)
        self.state.dirty.add("market_latest")
        self.db.record_run("index_spot", res.source, started, "success", row_count=res.row_count,
                           data_status=res.data_status,
                           detail=json.dumps({"warnings": res.warnings}, ensure_ascii=False))
        return res

    def collect_boards(self, ctx: JobContext) -> FetchResult:
        started = to_db_ts(now_utc())
        try:
            res = self.adapter.fetch_industry_boards()
        except DataSourceError as exc:
            self._fail("industry_boards", started, exc)
            raise
        if res.data_status == DataStatus.INVALID:
            exc = DataValidationError("industry_boards", f"行业板块数据异常: {res.warnings}")
            self._fail("industry_boards", started, exc, res.source)
            raise exc
        out = self._with_meta(res.df, res, "board")
        self._safe_store("行业板块", lambda: self.db.insert_df("industry_snapshots", out, BOARD_DB_COLUMNS))
        self.state.latest_boards = res
        self.state.mark_success("industry_boards", res.source, res.row_count, res.data_status)
        self.state.dirty.add("market_latest")
        self.db.record_run("industry_boards", res.source, started, "success", row_count=res.row_count,
                           data_status=res.data_status)
        return res

    # -- 自选股分钟线 ---------------------------------------------------------
    def collect_watchlist_minutes(self, ctx: JobContext) -> JobResult:
        codes = self.s.watchlist.codes
        if not codes:
            return JobResult.SKIPPED
        started = to_db_ts(now_utc())
        start_dt = datetime.combine(ctx.today, time(9, 15), tzinfo=BEIJING_TZ)
        in_session = ctx.phase in TRADING_PHASES
        ok = failed = rows = 0
        errors: list[str] = []
        source = self.adapter.provider
        for code in codes:
            self.guard.checkpoint(TaskKind.ESSENTIAL, "watchlist_minutes")
            try:
                res = self.adapter.fetch_minute_bars(code, start_dt, ctx.now_bj, period="1")
            except CircuitOpenError as exc:
                self._fail("watchlist_minutes", started, exc)
                raise
            except DataSourceError as exc:
                failed += 1
                errors.append(f"{code}: {exc}")
                continue
            source = res.source
            status = classify_bar_freshness(res.quote_time, res.fetched_at, in_session,
                                            self.s.datasource.minute_realtime_max_lag_sec)
            info: dict[str, Any] = {"last_bar_time": None, "close": None, "data_status": status,
                                    "fetched_at": res.fetched_at, "source": res.source, "bars_today": 0}
            if not res.df.empty:
                out = res.df.copy()
                out["fetched_at"] = to_db_ts(res.fetched_at)
                out["source"] = res.source
                rows += self._safe_store("分钟K线", lambda frame=out: self.db.insert_df(
                    "minute_bars", frame, MINUTE_DB_COLUMNS, mode="REPLACE"))
                last = res.df.iloc[-1]
                info.update({"last_bar_time": res.quote_time, "close": float(last["close"])
                             if pd.notna(last["close"]) else None, "bars_today": int(len(res.df))})
            self.state.watch_minutes[code] = info
            ok += 1
        # 移除已不在自选股列表中的条目
        for code in list(self.state.watch_minutes):
            if code not in codes:
                self.state.watch_minutes.pop(code, None)
        status_txt = "success" if failed == 0 else ("partial" if ok else "failed")
        self.db.record_run("watchlist_minutes", source, started, status_txt, row_count=rows,
                           error_message="; ".join(errors)[:2000] if errors else None)
        self.state.dirty.add("market_status")
        if ok == 0 and failed > 0:
            self.state.mark_failure("watchlist_minutes", errors[0])
            raise DataSourceError("watchlist_minutes", f"全部 {failed} 只自选股分钟线获取失败")
        self.state.mark_success("watchlist_minutes", source, rows,
                                "partial" if failed else "mixed_see_watchlist")
        return JobResult.DONE

    # -- 收盘快照与每日统计 ---------------------------------------------------
    def collect_close(self, ctx: JobContext) -> JobResult:
        res = self.collect_spot(ctx, purpose="close")
        for name, fn in (("指数", self.collect_indexes), ("行业板块", self.collect_boards)):
            try:
                fn(ctx)
            except DataSourceError as exc:
                logger.warning("收盘%s快照失败（不影响收盘全A快照）: %s", name, exc)
        breadth = compute_breadth(res.df)
        index_json: dict[str, Any] = {}
        idx = self.state.latest_index
        if idx is not None and idx.fetched_at.astimezone(BEIJING_TZ).date() == ctx.today:
            for r in idx.df.itertuples(index=False):
                index_json[str(r.code)] = {
                    "name": r.name,
                    "price": None if pd.isna(r.price) else float(r.price),
                    "pct_change": None if pd.isna(r.pct_change) else float(r.pct_change),
                    "fetched_at": to_bj_iso(idx.fetched_at),
                }
        row = [date_str(ctx.today), to_db_ts(now_utc()), breadth["total"], breadth["traded"],
               breadth["up"], breadth["down"], breadth["flat"], breadth["limit_up_approx"],
               breadth["limit_down_approx"], breadth["advance_ratio"], breadth["median_pct_change"],
               breadth["amount_total_yuan"], json.dumps(index_json, ensure_ascii=False),
               res.source, res.data_status]
        self._safe_store("每日统计", lambda: self.db.insert_rows(
            "daily_stats",
            ["trade_date", "computed_at", "total", "traded", "up", "down", "flat", "limit_up_approx",
             "limit_down_approx", "advance_ratio", "median_pct_change", "amount_total_yuan",
             "index_json", "source", "data_status"], [row]))
        return JobResult.DONE

    # -- 行业成分（每日刷新，分批） -------------------------------------------
    def refresh_industry_members(self, ctx: JobContext) -> JobResult:
        boards = self.state.latest_boards
        if boards is None or boards.fetched_at.astimezone(BEIJING_TZ).date() != ctx.today:
            boards = self.collect_boards(ctx)
        today_key = date_str(ctx.today)
        # 清理非当日的尝试计数，保持字典有界
        for k in [k for k in self._member_attempts if k[0] != today_key]:
            self._member_attempts.pop(k, None)
        day_start = to_db_ts(datetime.combine(ctx.today, time(0, 0), tzinfo=BEIJING_TZ))
        done = {r["board_code"] for r in self.db.query(
            "SELECT board_code FROM industry_members GROUP BY board_code HAVING MAX(updated_at) >= ?",
            (day_start,))}
        pending = [(c, n) for c, n in zip(boards.df["board_code"], boards.df["board_name"], strict=True)
                   if c not in done and self._member_attempts.get((today_key, c), 0) < 2]
        if not pending:
            self.state.members_cache = None
            self.state.members_cache_key = None
            logger.info("行业成分刷新完成（%d 个板块已更新）", len(done))
            return JobResult.DONE
        started = to_db_ts(now_utc())
        ok = 0
        errors: list[str] = []
        source = self.adapter.provider
        for board_code, board_name in pending[: self.s.schedule.industry_members_batch_size]:
            self.guard.checkpoint(TaskKind.NONESSENTIAL, "industry_members")
            key = (today_key, board_code)
            self._member_attempts[key] = self._member_attempts.get(key, 0) + 1
            try:
                res = self.adapter.fetch_industry_members(board_code, board_name)
            except CircuitOpenError as exc:
                self._fail("industry_members", started, exc)
                raise
            except DataSourceError as exc:
                errors.append(f"{board_name}: {exc}")
                continue
            source = res.source
            if res.df.empty:
                errors.append(f"{board_name}: empty")
                continue
            ts = to_db_ts(res.fetched_at)
            member_rows = [(board_code, board_name, c, n, ts, res.source)
                           for c, n in zip(res.df["code"], res.df["name"], strict=True)]
            self.db.replace_partition("industry_members", "board_code = ?", (board_code,),
                                      MEMBER_DB_COLUMNS, member_rows)
            ok += 1
        self.db.record_run("industry_members", source, started,
                           "success" if not errors else ("partial" if ok else "failed"), row_count=ok,
                           error_message="; ".join(errors)[:2000] if errors else None)
        return JobResult.MORE

    # -- 指数日K --------------------------------------------------------------
    def update_index_daily(self, ctx: JobContext) -> JobResult:
        target = ctx.target_day
        if target is None:
            return JobResult.SKIPPED
        target_s = date_str(target)
        codes = list(dict.fromkeys(self.s.datasource.index_codes + [self.s.datasource.benchmark_index]))
        started = to_db_ts(now_utc())
        lagging: list[str] = []
        total = 0
        source = self.adapter.provider
        for code in codes:
            self.guard.checkpoint(TaskKind.HEAVY, "index_daily")
            row = self.db.query_one("SELECT MAX(trade_date) AS d FROM index_daily WHERE code = ?", (code,))
            last = row["d"] if row is not None else None
            if last is not None and last >= target_s:
                continue
            start = (target - timedelta(days=self.s.schedule.daily_history_days) if last is None
                     else date.fromisoformat(last) - timedelta(days=10))
            try:
                res = self.adapter.fetch_index_daily(code, start, target)
            except DataSourceError as exc:
                self._fail("index_daily", started, exc)
                raise
            source = res.source
            if not res.df.empty:
                out = res.df.copy()
                out["fetched_at"] = to_db_ts(res.fetched_at)
                out["source"] = res.source
                out = out.reindex(columns=INDEX_DAILY_DB_COLUMNS)
                total += self.db.insert_df("index_daily", out, INDEX_DAILY_DB_COLUMNS, mode="REPLACE")
            latest = None if res.df.empty else str(res.df["trade_date"].max())
            if latest is None or latest < target_s:
                lagging.append(f"{code}:{latest}")
        self.db.record_run("index_daily", source, started, "success" if not lagging else "partial",
                           row_count=total, data_status=DataStatus.HISTORICAL,
                           error_message=f"目标日 {target_s} 数据未就绪: {lagging}" if lagging else None)
        if lagging:
            raise DataSourceError("index_daily", f"目标日 {target_s} 的指数日K尚未全部发布: {lagging}")
        return JobResult.DONE

    # -- 个股日K（分批、可续传） ----------------------------------------------
    def _daily_universe(self, ctx: JobContext) -> list[str]:
        codes: list[str] = []
        if self.s.schedule.daily_update_scope == "all":
            spot = self.state.latest_spot
            if spot is not None and not spot.df.empty:
                codes = spot.df["code"].tolist()
            else:
                cutoff = to_db_ts(now_utc() - timedelta(days=10))
                codes = [r["code"] for r in self.db.query(
                    "SELECT code FROM stocks WHERE last_seen_at >= ?", (cutoff,))]
            if not codes:
                # 首次运行且不在交易时段（如周末）：获取一次真实快照作为股票列表来源
                logger.info("日K更新：本地无股票列表，获取一次全A快照作为股票列表")
                spot = self.collect_spot(ctx, purpose="universe")
                codes = spot.df["code"].tolist()
            if not self.s.screener.include_bse:
                codes = [c for c in codes if not is_bse(c)]
        else:
            row = self.db.query_one("SELECT run_id FROM screen_runs WHERE status = 'success'"
                                    " ORDER BY run_at DESC LIMIT 1")
            if row is not None:
                codes = [r["code"] for r in self.db.query(
                    "SELECT code FROM screen_results WHERE run_id = ?", (row["run_id"],))]
        return sorted(set(codes) | set(self.s.watchlist.codes))

    def _traded_on(self, day: date) -> set[str] | None:
        """若内存中有该交易日收盘后的全A快照，返回当日有成交的股票集合，用于校验日K完整性。"""
        spot = self.state.latest_spot
        if spot is None:
            return None
        fetched_bj = spot.fetched_at.astimezone(BEIJING_TZ)
        if fetched_bj.date() != day or fetched_bj.time() < time(15, 0):
            return None
        df = spot.df
        mask = df["price"].notna() & (df["volume_lots"].fillna(0) > 0)
        return set(df.loc[mask, "code"])

    def update_daily_bars(self, ctx: JobContext) -> JobResult:
        target = ctx.target_day
        if target is None:
            return JobResult.SKIPPED
        target_s = date_str(target)
        init_key = f"daily_progress_init:{target_s}"
        if self.db.get_state(init_key) is None:
            codes = self._daily_universe(ctx)
            if not codes:
                logger.warning("日K更新：暂无股票列表（需要先成功获取一次全A快照），稍后重试")
                return JobResult.SKIPPED
            ts = to_db_ts(now_utc())
            self.db.insert_rows("daily_update_progress",
                                ["trade_date", "code", "status", "attempts", "last_error", "updated_at"],
                                ((target_s, c, "pending", 0, None, ts) for c in codes), mode="IGNORE")
            self.db.set_state(init_key, str(len(codes)))
            logger.info("日K更新开始：目标日 %s，共 %d 只股票", target_s, len(codes))

        batch = self.db.query(
            "SELECT code, attempts FROM daily_update_progress WHERE trade_date = ?"
            " AND status IN ('pending', 'retry') ORDER BY code LIMIT ?",
            (target_s, self.s.schedule.daily_update_batch_size))
        if not batch:
            counts = {r["status"]: r["n"] for r in self.db.query(
                "SELECT status, COUNT(*) AS n FROM daily_update_progress WHERE trade_date = ? GROUP BY status",
                (target_s,))}
            logger.info("日K更新完成：目标日 %s，状态统计 %s", target_s, counts)
            self.db.record_run("daily_bars", self.adapter.provider, to_db_ts(now_utc()), "success",
                               row_count=sum(counts.values()), data_status=DataStatus.HISTORICAL,
                               detail=json.dumps(counts, ensure_ascii=False))
            return JobResult.DONE

        traded = self._traded_on(target)
        for r in batch:
            self.guard.checkpoint(TaskKind.HEAVY, "daily_bars")
            code = r["code"]
            attempts = int(r["attempts"]) + 1
            try:
                status, err = self._update_one_daily(code, target, traded)
            except CircuitOpenError:
                raise
            except DataSourceError as exc:
                status, err = "retry", str(exc)[:500]
            if status == "retry" and attempts >= DAILY_MAX_CODE_ATTEMPTS:
                status = "failed"
                logger.warning("日K更新 %s 连续 %d 次失败，今日放弃: %s", code, attempts, err)
            self.db.execute(
                "UPDATE daily_update_progress SET status = ?, attempts = ?, last_error = ?, updated_at = ?"
                " WHERE trade_date = ? AND code = ?",
                (status, attempts, err, to_db_ts(now_utc()), target_s, code))
        return JobResult.MORE

    def _update_one_daily(self, code: str, target: date, traded: set[str] | None) -> tuple[str, str | None]:
        adjust = self.s.datasource.daily_adjust
        target_s = date_str(target)
        row = self.db.query_one("SELECT MAX(trade_date) AS d FROM daily_bars WHERE code = ? AND adjust = ?",
                                (code, adjust))
        last = row["d"] if row is not None else None
        if last is not None and last >= target_s:
            return "done", None
        history_start = target - timedelta(days=self.s.schedule.daily_history_days)
        full = last is None
        start = history_start if full else date.fromisoformat(last) - timedelta(days=10)
        res = self.adapter.fetch_daily_bars(code, start, target, adjust)
        df = res.df
        replace_all = full
        if not full and adjust and not df.empty and self._adjustment_changed(code, adjust, df, last):
            logger.info("%s 复权价格与库存数据不一致（可能发生除权除息），重新获取完整历史", code)
            res = self.adapter.fetch_daily_bars(code, history_start, target, adjust)
            df = res.df
            replace_all = True
        if df.empty:
            if traded is not None and code in traded:
                return "retry", "daily_bars_empty_but_traded_on_target_day"
            return "done_empty", None
        out = df.copy()
        out["adjust"] = adjust
        out["fetched_at"] = to_db_ts(res.fetched_at)
        out["source"] = res.source
        # 数据源未提供的可选字段（如成交额、换手率）写为 NULL，不做任何填补
        out = out.reindex(columns=DAILY_DB_COLUMNS)
        if replace_all:
            sub = out.astype(object)
            sub = sub.where(pd.notna(sub), None)
            self.db.replace_partition("daily_bars", "code = ? AND adjust = ?", (code, adjust),
                                      DAILY_DB_COLUMNS, sub.itertuples(index=False, name=None))
        else:
            self.db.insert_df("daily_bars", out, DAILY_DB_COLUMNS, mode="REPLACE")
        latest = str(df["trade_date"].max())
        if traded is not None and code in traded and latest < target_s:
            return "retry", f"target_day_bar_missing(latest={latest})"
        return "done", None

    def _adjustment_changed(self, code: str, adjust: str, df: pd.DataFrame, last: str) -> bool:
        overlap = df.loc[df["trade_date"] <= last, ["trade_date", "close"]]
        if overlap.empty:
            return False
        stored = self.db.query_df(
            "SELECT trade_date, close AS close_db FROM daily_bars WHERE code = ? AND adjust = ?"
            " AND trade_date >= ? AND trade_date <= ?",
            (code, adjust, str(overlap["trade_date"].min()), last))
        merged = overlap.merge(stored, on="trade_date", how="inner").dropna()
        if merged.empty:
            return False
        rel = (merged["close"] - merged["close_db"]).abs() / merged["close_db"].abs().clip(lower=1e-9)
        return bool((rel > 1e-4).any())
