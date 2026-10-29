"""work_data 目录导出：供用户日后主动让 Claude 读取的规范化数据文件。

文件（名称固定，UTF-8 编码）
- market_latest.json  指数、市场宽度、行业板块涨跌前后、自选股摘要
- stocks_latest.csv   最新全A快照（含数据来源、获取时间、数据状态）
- candidates.csv      最近一次筛选结果（评分明细与结构化理由代码）
- indicators.csv      每只股票最近一个交易日的技术指标
- alerts.json         最近的异动事件（客观记录）
- market_status.json  系统运行状态（内存、任务、数据源健康度、数据库容量）
- manifest.json       所有文件的版本、生成时间、数据时间、有效期、行数、SHA-256 校验值

写入方式：先写同目录下的临时文件（.文件名.<pid>.tmp）→ flush + fsync → os.replace 原子替换。
读取方在任意时刻看到的都是完整的旧文件或完整的新文件，不会读到写入一半的内容。
manifest.json 最后写入；读取时建议先读 manifest，再按 sha256 校验对应文件。

本模块不生成任何自然语言总结，不向任何外部服务发送数据，也不上传云端。
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import time as _time
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import numpy as np
import pandas as pd

from timeutil import BEIJING_TZ, db_ts_to_bj_iso, now_utc, to_bj_iso
from trading_calendar import TRADING_PHASES, TradingCalendar, phase_of_time

if TYPE_CHECKING:
    from config import Settings
    from database import Database
    from scheduler import JobContext, JobResult
    from state import RuntimeState

logger = logging.getLogger(__name__)

SCHEMA = "astock.work_data.v1"
FILES: dict[str, str] = {
    "market_latest": "market_latest.json",
    "stocks_latest": "stocks_latest.csv",
    "candidates": "candidates.csv",
    "indicators": "indicators.csv",
    "alerts": "alerts.json",
    "market_status": "market_status.json",
}
MANIFEST_FILE = "manifest.json"

STATUS_LEGEND = {
    "realtime": "有行情时间戳，且与获取时间相差在阈值内",
    "delayed": "有行情时间戳，但明显落后于获取时间",
    "historical": "已收盘的历史数据",
    "unknown_time": "数据源未提供行情时间戳，仅有本地获取时间（fetched_at），实时性未经验证",
    "stale": "判定为过期：交易时段内内容长时间未变化，或已超过有效期",
    "invalid": "未通过校验，不应使用",
}


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def atomic_write_bytes(path: Path, data: bytes, retries: int = 5, wait_sec: float = 0.2) -> None:
    """原子写入。Windows 上目标文件被其它程序以独占方式打开时 os.replace 会失败，有限次重试。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        last_exc: Exception | None = None
        for attempt in range(max(1, retries)):
            try:
                os.replace(tmp, path)
                return
            except PermissionError as exc:
                last_exc = exc
                _time.sleep(wait_sec * (attempt + 1))
        raise PermissionError(f"替换 {path.name} 失败（文件可能被占用）: {last_exc}")
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                logger.warning("无法删除临时文件 %s", tmp)


def json_safe(obj: Any) -> Any:
    """递归转换为严格 JSON 可序列化对象：NaN/Inf → null，时间 → 北京时间 ISO 字符串。"""
    if obj is None or isinstance(obj, (bool, str)):
        return obj
    if isinstance(obj, (int, np.integer)) and not isinstance(obj, (bool, np.bool_)):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (float, np.floating)):
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if obj is pd.NaT:
        return None
    if isinstance(obj, pd.Timestamp):
        obj = obj.to_pydatetime()
    if isinstance(obj, datetime):
        return to_bj_iso(obj) if obj.tzinfo is not None else obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [json_safe(v) for v in obj]
    return str(obj)


def json_bytes(obj: Any) -> bytes:
    return json.dumps(json_safe(obj), ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8")


def csv_bytes(df: pd.DataFrame, encoding: str) -> bytes:
    out = df.copy()
    float_cols = out.select_dtypes(include=["float"]).columns
    out[float_cols] = out[float_cols].round(6)
    return out.to_csv(index=False, lineterminator="\n").encode(encoding)


def _ts_col_to_bj(series: pd.Series) -> pd.Series:
    return series.map(lambda v: db_ts_to_bj_iso(v) if isinstance(v, str) and v else None)


# ---------------------------------------------------------------------------
# 导出器
# ---------------------------------------------------------------------------

class WorkDataExporter:
    def __init__(self, settings: "Settings", db: "Database", state: "RuntimeState",
                 calendar: TradingCalendar, status_provider: Callable[[], dict[str, Any]]) -> None:
        self.s = settings
        self.db = db
        self.state = state
        self.calendar = calendar
        self.status_provider = status_provider
        self.dir = settings.work_data_path
        self.entries: dict[str, dict[str, Any]] = {}
        self.manifest_seq = 0
        self._load_previous_manifest()

    # -- manifest 继承 --------------------------------------------------------
    def _load_previous_manifest(self) -> None:
        p = self.dir / MANIFEST_FILE
        if not p.is_file():
            return
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            logger.warning("读取旧 manifest 失败，将重新生成: %s", exc)
            return
        self.manifest_seq = int(data.get("manifest_seq", 0))
        for key, entry in (data.get("files") or {}).items():
            if key in FILES and isinstance(entry, dict):
                entry["from_previous_session"] = True
                self.entries[key] = entry

    def startup(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        # 程序启动时用数据库中已有的历史结果刷新可导出的文件
        self.state.dirty.update({"candidates", "indicators", "alerts"})

    # -- 有效期 ---------------------------------------------------------------
    def _next_session_start(self, after: datetime) -> datetime | None:
        """下一个“行情会发生变化”的时刻：集合竞价开始、连续竞价开始、午后开盘或下一交易日。"""
        bj = after.astimezone(BEIJING_TZ)
        is_td, _ = self.calendar.is_trading_day(bj.date())
        if is_td:
            t = bj.time()
            boundary: time | None = None
            if t < time(9, 15):
                boundary = time(9, 15)
            elif t < time(9, 30):
                boundary = time(9, 30)
            elif time(11, 30) <= t < time(13, 0):
                boundary = time(13, 0)
            if boundary is not None:
                return bj.replace(hour=boundary.hour, minute=boundary.minute, second=0, microsecond=0)
        nd = self.calendar.next_trading_day(bj.date())
        if nd is None:
            return None
        return datetime(nd.year, nd.month, nd.day, 9, 15, tzinfo=BEIJING_TZ)

    def _market_expiry(self, data_time: datetime) -> datetime:
        bj = data_time.astimezone(BEIJING_TZ)
        is_td, _ = self.calendar.is_trading_day(bj.date())
        if is_td and phase_of_time(bj.time()) in TRADING_PHASES:
            return data_time + timedelta(seconds=self.s.export.spot_valid_sec)
        nxt = self._next_session_start(data_time)
        return nxt if nxt is not None else data_time + timedelta(seconds=self.s.export.daily_valid_sec)

    # -- 各文件内容 -----------------------------------------------------------
    def _build_stocks_latest(self) -> tuple[bytes, dict[str, Any]] | None:
        res = self.state.latest_spot
        if res is None:
            return None
        cols = ["code", "name", "exchange", "price", "pct_change", "change", "volume_lots", "amount_yuan",
                "amplitude_pct", "high", "low", "open", "prev_close", "volume_ratio", "turnover_rate",
                "pe_dynamic", "pb", "total_mv_yuan", "float_mv_yuan", "validation_flags"]
        df = res.df.loc[:, cols].copy()
        df["fetched_at"] = to_bj_iso(res.fetched_at)
        df["quote_time"] = to_bj_iso(res.quote_time) if res.quote_time else None
        df["time_basis"] = res.time_basis
        df["data_status"] = res.data_status
        df["source"] = res.source
        meta = {"data_time": res.fetched_at, "time_basis": res.time_basis, "data_status": res.data_status,
                "source": res.source, "rows": len(df), "expires_at": self._market_expiry(res.fetched_at),
                "format": "csv"}
        return csv_bytes(df, self.s.export.csv_encoding), meta

    def _build_market_latest(self, ctx: "JobContext") -> tuple[bytes, dict[str, Any]] | None:
        st = self.state
        if st.latest_spot is None and st.latest_index is None and st.latest_boards is None:
            return None
        doc: dict[str, Any] = {
            "schema": SCHEMA + ".market_latest",
            "generated_at": now_utc(),
            "timezone": "所有时间均为北京时间 (UTC+08:00)",
            "session": {"phase": ctx.phase.value, "calendar_basis": ctx.calendar_basis,
                        "latest_completed_trading_day": ctx.target_day},
            "status_legend": STATUS_LEGEND,
        }
        data_times: list[datetime] = []
        statuses: list[str] = []
        sources: list[str] = []
        if st.latest_index is not None:
            r = st.latest_index
            doc["indices"] = r.df.to_dict(orient="records")
            doc["indices_meta"] = r.meta()
            data_times.append(r.fetched_at)
            statuses.append(r.data_status)
            sources.append(r.source)
        if st.latest_breadth is not None:
            b = dict(st.latest_breadth)
            doc["breadth"] = b
            doc["breadth_meta"] = {"derived_from": "stocks_latest 全A快照",
                                   "limit_counts_note": "涨停/跌停为按涨跌幅限制估算的近似值"}
            peak = max((r for _, r in st.breadth_history), default=None)
            doc["breadth_intraday_peak_advance_ratio"] = peak
        if st.latest_boards is not None:
            r = st.latest_boards
            bd = r.df.sort_values("pct_change", ascending=False)
            doc["industry_boards_top10"] = bd.head(10).to_dict(orient="records")
            doc["industry_boards_bottom10"] = bd.tail(10).iloc[::-1].to_dict(orient="records")
            doc["industry_boards_count"] = int(len(bd))
            doc["industry_meta"] = r.meta()
            data_times.append(r.fetched_at)
            statuses.append(r.data_status)
            sources.append(r.source)
        if st.latest_spot is not None:
            r = st.latest_spot
            doc["spot_meta"] = r.meta()
            data_times.append(r.fetched_at)
            statuses.append(r.data_status)
            sources.append(r.source)
            watch = []
            if self.s.watchlist.codes:
                sub = r.df.loc[r.df["code"].isin(self.s.watchlist.codes),
                               ["code", "name", "price", "pct_change", "high", "low", "prev_close",
                                "volume_ratio", "turnover_rate", "amount_yuan"]]
                for rec in sub.to_dict(orient="records"):
                    m = st.watch_minutes.get(rec["code"], {})
                    rec["minute_last_bar_time"] = m.get("last_bar_time")
                    rec["minute_close"] = m.get("close")
                    rec["minute_data_status"] = m.get("data_status")
                    rec["minute_fetched_at"] = m.get("fetched_at")
                    watch.append(rec)
            doc["watchlist"] = watch
            doc["spot_crosscheck"] = st.crosscheck
        oldest = min(data_times) if data_times else now_utc()
        worst = _worst_status(statuses)
        meta = {"data_time": oldest, "time_basis": "fetch_time_only", "data_status": worst,
                "source": " | ".join(sorted(set(sources))), "rows": None,
                "expires_at": self._market_expiry(oldest), "format": "json"}
        return json_bytes(doc), meta

    def _build_candidates(self) -> tuple[bytes, dict[str, Any]] | None:
        row = self.db.query_one("SELECT run_id, run_at, note, spot_data_status FROM screen_runs"
                                " WHERE status = 'success' ORDER BY run_at DESC LIMIT 1")
        if row is None:
            return None
        df = self.db.query_df(
            "SELECT * FROM screen_results WHERE run_id = ?"
            " ORDER BY CASE WHEN rank = 0 THEN 1 ELSE 0 END, rank LIMIT ?",
            (row["run_id"], self.s.export.candidates_max))
        for c in ("run_at", "spot_fetched_at"):
            df[c] = _ts_col_to_bj(df[c])
        run_at = datetime.fromisoformat(row["run_at"])
        if row["note"] == "intraday":
            expires = run_at + timedelta(seconds=2 * self.s.schedule.screening_interval_sec)
        else:
            expires = self._next_session_start(run_at) or run_at + timedelta(days=1)
        meta = {"data_time": run_at, "time_basis": "screen_run_at", "data_status": row["spot_data_status"],
                "source": "local:screener.py", "rows": len(df), "expires_at": expires, "format": "csv"}
        return csv_bytes(df, self.s.export.csv_encoding), meta

    def _build_indicators(self) -> tuple[bytes, dict[str, Any]] | None:
        from indicators import load_latest_indicators

        df = load_latest_indicators(self.db, "9999-12-31")
        if df.empty:
            return None
        latest = str(df["trade_date"].max())
        floor = (date.fromisoformat(latest) - timedelta(days=10)).isoformat()
        df = df.loc[df["trade_date"] >= floor].copy()
        names = self.db.query_df("SELECT code, name FROM stocks")
        df = df.merge(names, on="code", how="left")
        df["computed_at"] = _ts_col_to_bj(df["computed_at"])
        df["data_status"] = "historical"
        front = ["code", "name", "trade_date"]
        df = df.loc[:, front + [c for c in df.columns if c not in front]].sort_values("code")
        latest_d = date.fromisoformat(latest)
        # 有效期：下一交易日 18:00（届时应已有更新的收盘指标）；日历未知时保守取 3 天
        nd = self.calendar.next_trading_day(latest_d) or (latest_d + timedelta(days=3))
        expires = datetime(nd.year, nd.month, nd.day, 18, 0, tzinfo=BEIJING_TZ)
        meta = {"data_time": datetime(latest_d.year, latest_d.month, latest_d.day, 15, 0, tzinfo=BEIJING_TZ),
                "time_basis": "trade_date", "data_status": "historical",
                "source": "local:indicators.py", "rows": len(df), "expires_at": expires, "format": "csv"}
        return csv_bytes(df, self.s.export.csv_encoding), meta

    def _build_alerts(self) -> tuple[bytes, dict[str, Any]] | None:
        df = self.db.query_df("SELECT * FROM anomalies ORDER BY triggered_at DESC, id DESC LIMIT ?",
                              (self.s.export.alerts_max,))
        events = []
        for rec in df.to_dict(orient="records"):
            rec["triggered_at"] = db_ts_to_bj_iso(rec.get("triggered_at"))
            rec["data_time"] = db_ts_to_bj_iso(rec.get("data_time"))
            rec["metrics"] = json.loads(rec.pop("metrics_json") or "{}")
            rec["thresholds"] = json.loads(rec.pop("thresholds_json") or "{}")
            events.append(rec)
        newest = (datetime.fromisoformat(df["triggered_at"].iloc[0]) if not df.empty else now_utc())
        doc = {
            "schema": SCHEMA + ".alerts",
            "generated_at": now_utc(),
            "timezone": "所有时间均为北京时间 (UTC+08:00)",
            "note": "异动记录只描述客观事件与触发阈值，不包含分析或买卖建议",
            "count": len(events),
            "events": events,
        }
        meta = {"data_time": newest, "time_basis": "triggered_at", "data_status": "event_log",
                "source": "local:anomaly_detector.py", "rows": len(events),
                "expires_at": self._market_expiry(now_utc()), "format": "json"}
        return json_bytes(doc), meta

    def _build_status(self) -> tuple[bytes, dict[str, Any]]:
        now = now_utc()
        doc = {"schema": SCHEMA + ".market_status", "generated_at": now,
               "timezone": "所有时间均为北京时间 (UTC+08:00)", **self.status_provider()}
        meta = {"data_time": now, "time_basis": "generated_at", "data_status": "system_status",
                "source": "local:main.py", "rows": None,
                "expires_at": now + timedelta(seconds=3 * self.s.schedule.export_interval_sec),
                "format": "json"}
        return json_bytes(doc), meta

    # -- 写出 -----------------------------------------------------------------
    def _write(self, key: str, built: tuple[bytes, dict[str, Any]] | None) -> None:
        if built is None:
            return
        data, meta = built
        path = self.dir / FILES[key]
        prev = self.entries.get(key, {})
        version = int(prev.get("version", 0)) + 1
        entry = {
            "file": FILES[key],
            "format": meta["format"],
            "encoding": "utf-8" if meta["format"] == "json" else self.s.export.csv_encoding,
            "version": version,
            "generated_at": to_bj_iso(now_utc()),
            "data_time": to_bj_iso(meta["data_time"]),
            "time_basis": meta["time_basis"],
            "data_status": meta["data_status"],
            "source": meta["source"],
            "rows": meta["rows"],
            "size_bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "expires_at": to_bj_iso(meta["expires_at"]),
            "write_status": "ok",
            "from_previous_session": False,
        }
        try:
            atomic_write_bytes(path, data, self.s.export.replace_retries, self.s.export.replace_retry_wait_sec)
        except OSError as exc:
            logger.error("写入 %s 失败，保留旧文件: %s", FILES[key], exc)
            if prev:
                prev["write_status"] = f"last_write_failed: {exc}"[:300]
                prev["last_failed_at"] = to_bj_iso(now_utc())
            else:
                self.entries[key] = {"file": FILES[key], "write_status": f"write_failed: {exc}"[:300]}
            return
        self.entries[key] = entry

    def export(self, ctx: "JobContext", final: bool = False) -> "JobResult":
        from scheduler import JobResult

        if not self.s.export.enabled:
            return JobResult.SKIPPED
        builders: dict[str, Callable[[], tuple[bytes, dict[str, Any]] | None]] = {
            "stocks_latest": self._build_stocks_latest,
            "market_latest": lambda: self._build_market_latest(ctx),
            "candidates": self._build_candidates,
            "indicators": self._build_indicators,
            "alerts": self._build_alerts,
        }
        dirty = set(self.state.dirty)
        for key, build in builders.items():
            if key not in dirty:
                continue
            try:
                self._write(key, build())
            except Exception:
                # 单个文件构建失败不影响其它文件，但必须留下完整错误记录
                logger.exception("构建 %s 失败", FILES[key])
                if key in self.entries:
                    self.entries[key]["write_status"] = "last_build_failed"
            finally:
                self.state.dirty.discard(key)
        self.state.dirty.discard("market_status")
        self._write("market_status", self._build_status())
        self._write_manifest(running=not final)
        return JobResult.DONE

    def _write_manifest(self, running: bool) -> None:
        now = now_utc()
        self.manifest_seq += 1
        files: dict[str, Any] = {}
        for key, name in FILES.items():
            entry = dict(self.entries.get(key) or {"file": name, "write_status": "not_generated_yet"})
            exp = entry.get("expires_at")
            if exp:
                entry["freshness"] = "expired" if datetime.fromisoformat(exp) <= now else "fresh"
            else:
                entry["freshness"] = "unknown"
            files[key] = entry
        doc = {
            "schema": SCHEMA + ".manifest",
            "manifest_seq": self.manifest_seq,
            "generated_at": now,
            "timezone": "所有时间均为北京时间 (UTC+08:00)",
            "writer": {
                "program": "AStockAI V1.0",
                "pid": os.getpid(),
                "started_at": self.state.started_at,
                "running": running,
                "heartbeat_at": now,
                "heartbeat_expires_at": now + timedelta(seconds=3 * self.s.schedule.export_interval_sec),
            },
            "how_to_read": [
                "先读本文件，再读取各数据文件；用 sha256 校验文件是否与本清单一致。",
                "freshness 是写入本清单那一刻的判断；程序停止后不会再更新，请用 expires_at 与当前北京时间自行比较。",
                "writer.heartbeat_expires_at 早于当前时间，说明采集程序已停止或卡住，所有数据都应视为可能过期。",
                "CSV 中 code 为 6 位字符串（含前导零），用表格软件或 pandas 读取时请按文本读取。",
                "data_status 含义见 status_legend；unknown_time 表示数据源没有提供行情时间戳。",
            ],
            "status_legend": STATUS_LEGEND,
            "files": files,
        }
        try:
            atomic_write_bytes(self.dir / MANIFEST_FILE, json_bytes(doc),
                               self.s.export.replace_retries, self.s.export.replace_retry_wait_sec)
        except OSError as exc:
            logger.error("写入 manifest.json 失败: %s", exc)


def _worst_status(statuses: list[str]) -> str:
    order = ["invalid", "stale", "unknown_time", "delayed", "historical", "realtime"]
    for s in order:
        if s in statuses:
            return s
    return statuses[0] if statuses else "unknown"
