"""SQLite 本地数据库：表结构、连接参数、参数化写入、容量与写入暂停控制。

设计要点
- 单进程、单连接、单线程写入（调度器为单线程），避免并发写锁与额外内存。
- PRAGMA cache_size 限制页缓存（默认 16 MiB），mmap_size=0 禁用内存映射，
  temp_store=FILE 让排序/临时表落盘，三者共同控制 SQLite 的内存占用。
- 新库创建时设置 auto_vacuum=INCREMENTAL，清理后可用 incremental_vacuum 回收空间，
  无需每次全量 VACUUM。
- 所有数据值一律通过参数绑定（?）传入；表名与列名不能参数化，因此使用白名单校验。
- 时间戳列（*_at, *_time）统一存储 UTC ISO-8601 字符串；交易日期列存储 YYYY-MM-DD。
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import numpy as np
import pandas as pd

from timeutil import now_utc, to_db_ts

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1"

# numpy 标量不是 Python 内建类型的子类（如 np.int64），需显式注册适配器
sqlite3.register_adapter(np.int64, int)
sqlite3.register_adapter(np.int32, int)
sqlite3.register_adapter(np.float32, float)
sqlite3.register_adapter(np.bool_, int)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stocks (
    code          TEXT PRIMARY KEY,
    name          TEXT,
    exchange      TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    source        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_calendar (
    trade_date TEXT PRIMARY KEY,
    source     TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset       TEXT NOT NULL,
    source        TEXT NOT NULL,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    status        TEXT NOT NULL,
    row_count     INTEGER,
    data_status   TEXT,
    quote_time    TEXT,
    error_type    TEXT,
    error_message TEXT,
    detail        TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_started ON collection_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_runs_dataset ON collection_runs(dataset, started_at);

CREATE TABLE IF NOT EXISTS spot_snapshots (
    snapshot_id      TEXT NOT NULL,
    code             TEXT NOT NULL,
    name             TEXT,
    price            REAL,
    pct_change       REAL,
    change           REAL,
    volume_lots      REAL,
    amount_yuan      REAL,
    amplitude_pct    REAL,
    high             REAL,
    low              REAL,
    open             REAL,
    prev_close       REAL,
    volume_ratio     REAL,
    turnover_rate    REAL,
    pe_dynamic       REAL,
    pb               REAL,
    total_mv_yuan    REAL,
    float_mv_yuan    REAL,
    quote_time       TEXT,
    fetched_at       TEXT NOT NULL,
    source           TEXT NOT NULL,
    time_basis       TEXT NOT NULL,
    data_status      TEXT NOT NULL,
    validation_flags TEXT,
    PRIMARY KEY (snapshot_id, code)
);
CREATE INDEX IF NOT EXISTS idx_spot_fetched ON spot_snapshots(fetched_at);
CREATE INDEX IF NOT EXISTS idx_spot_code ON spot_snapshots(code, fetched_at);

CREATE TABLE IF NOT EXISTS index_snapshots (
    snapshot_id  TEXT NOT NULL,
    code         TEXT NOT NULL,
    name         TEXT,
    price        REAL,
    pct_change   REAL,
    change       REAL,
    volume       REAL,
    amount_yuan  REAL,
    high         REAL,
    low          REAL,
    open         REAL,
    prev_close   REAL,
    quote_time   TEXT,
    fetched_at   TEXT NOT NULL,
    source       TEXT NOT NULL,
    time_basis   TEXT NOT NULL,
    data_status  TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, code)
);
CREATE INDEX IF NOT EXISTS idx_index_snap_fetched ON index_snapshots(fetched_at);

CREATE TABLE IF NOT EXISTS industry_snapshots (
    snapshot_id       TEXT NOT NULL,
    board_code        TEXT NOT NULL,
    board_name        TEXT,
    price             REAL,
    pct_change        REAL,
    turnover_rate     REAL,
    total_mv_yuan     REAL,
    up_count          INTEGER,
    down_count        INTEGER,
    leader_name       TEXT,
    leader_pct_change REAL,
    fetched_at        TEXT NOT NULL,
    source            TEXT NOT NULL,
    time_basis        TEXT NOT NULL,
    data_status       TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, board_code)
);
CREATE INDEX IF NOT EXISTS idx_industry_fetched ON industry_snapshots(fetched_at);

CREATE TABLE IF NOT EXISTS industry_members (
    board_code TEXT NOT NULL,
    board_name TEXT NOT NULL,
    code       TEXT NOT NULL,
    name       TEXT,
    updated_at TEXT NOT NULL,
    source     TEXT NOT NULL,
    PRIMARY KEY (board_code, code)
);
CREATE INDEX IF NOT EXISTS idx_members_code ON industry_members(code);

CREATE TABLE IF NOT EXISTS breadth_snapshots (
    snapshot_id        TEXT PRIMARY KEY,
    fetched_at         TEXT NOT NULL,
    total              INTEGER,
    traded             INTEGER,
    up                 INTEGER,
    down               INTEGER,
    flat               INTEGER,
    suspended          INTEGER,
    limit_up_approx    INTEGER,
    limit_down_approx  INTEGER,
    advance_ratio      REAL,
    median_pct_change  REAL,
    amount_total_yuan  REAL,
    source             TEXT NOT NULL,
    derived_from       TEXT NOT NULL,
    data_status        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_breadth_fetched ON breadth_snapshots(fetched_at);

CREATE TABLE IF NOT EXISTS minute_bars (
    code        TEXT NOT NULL,
    period      TEXT NOT NULL,
    bar_time    TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume_lots REAL,
    amount_yuan REAL,
    fetched_at  TEXT NOT NULL,
    source      TEXT NOT NULL,
    PRIMARY KEY (code, period, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_minute_time ON minute_bars(bar_time);

CREATE TABLE IF NOT EXISTS daily_bars (
    code          TEXT NOT NULL,
    adjust        TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    open          REAL,
    high          REAL,
    low           REAL,
    close         REAL,
    volume_lots   REAL,
    amount_yuan   REAL,
    amplitude_pct REAL,
    pct_change    REAL,
    change        REAL,
    turnover_rate REAL,
    fetched_at    TEXT NOT NULL,
    source        TEXT NOT NULL,
    PRIMARY KEY (code, adjust, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_bars(trade_date);

CREATE TABLE IF NOT EXISTS index_daily (
    code        TEXT NOT NULL,
    trade_date  TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      REAL,
    amount_yuan REAL,
    pct_change  REAL,
    fetched_at  TEXT NOT NULL,
    source      TEXT NOT NULL,
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_index_daily_date ON index_daily(trade_date);

CREATE TABLE IF NOT EXISTS daily_update_progress (
    trade_date TEXT NOT NULL,
    code       TEXT NOT NULL,
    status     TEXT NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (trade_date, code)
);
CREATE INDEX IF NOT EXISTS idx_progress_updated ON daily_update_progress(updated_at);

CREATE TABLE IF NOT EXISTS indicator_values (
    code                    TEXT NOT NULL,
    trade_date              TEXT NOT NULL,
    computed_at             TEXT NOT NULL,
    adjust                  TEXT NOT NULL,
    bars_available          INTEGER NOT NULL,
    close                   REAL,
    ma5                     REAL,
    ma10                    REAL,
    ma20                    REAL,
    ma60                    REAL,
    vol_ma5                 REAL,
    vol_ma20                REAL,
    volume_ratio_5d         REAL,
    turnover_rate           REAL,
    rsi14                   REAL,
    macd_dif                REAL,
    macd_dea                REAL,
    macd_hist               REAL,
    high_20                 REAL,
    low_20                  REAL,
    prior_high_20           REAL,
    prior_low_20            REAL,
    high_60                 REAL,
    low_60                  REAL,
    pivot                   REAL,
    support_s1              REAL,
    resistance_r1           REAL,
    ret_20                  REAL,
    benchmark_ret_20        REAL,
    rs_20                   REAL,
    volatility_20           REAL,
    ma20_slope_5            REAL,
    sig_breakout_volume     INTEGER,
    sig_pullback_low_volume INTEGER,
    sig_trend_weakening     INTEGER,
    source                  TEXT NOT NULL,
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_indicator_date ON indicator_values(trade_date);

CREATE TABLE IF NOT EXISTS screen_runs (
    run_id               TEXT PRIMARY KEY,
    run_at               TEXT NOT NULL,
    model_version        TEXT NOT NULL,
    weights_json         TEXT NOT NULL,
    universe_size        INTEGER,
    eligible_count       INTEGER,
    saved_count          INTEGER,
    spot_fetched_at      TEXT,
    spot_data_status     TEXT,
    indicator_trade_date TEXT,
    market_context_json  TEXT,
    status               TEXT NOT NULL,
    note                 TEXT
);
CREATE INDEX IF NOT EXISTS idx_screen_runs_at ON screen_runs(run_at);

CREATE TABLE IF NOT EXISTS screen_results (
    run_id               TEXT NOT NULL,
    run_at               TEXT NOT NULL,
    rank                 INTEGER NOT NULL,
    code                 TEXT NOT NULL,
    name                 TEXT,
    in_watchlist         INTEGER NOT NULL,
    price                REAL,
    pct_change           REAL,
    amount_yuan          REAL,
    total_score          REAL,
    available_max        REAL,
    score_normalized     REAL,
    sector_score         REAL,
    sector_max           REAL,
    volume_price_score   REAL,
    volume_price_max     REAL,
    market_score         REAL,
    market_max           REAL,
    catalyst_score       REAL,
    catalyst_max         REAL,
    catalyst_status      TEXT,
    risk_reward_score    REAL,
    risk_reward_max      REAL,
    missing_components   TEXT,
    reason_codes         TEXT,
    board_name           TEXT,
    indicator_trade_date TEXT,
    spot_fetched_at      TEXT,
    spot_data_status     TEXT,
    model_version        TEXT NOT NULL,
    PRIMARY KEY (run_id, code)
);
CREATE INDEX IF NOT EXISTS idx_screen_results_at ON screen_results(run_at);

CREATE TABLE IF NOT EXISTS anomalies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dedup_key       TEXT NOT NULL UNIQUE,
    trade_date      TEXT NOT NULL,
    code            TEXT NOT NULL,
    name            TEXT,
    event_type      TEXT NOT NULL,
    triggered_at    TEXT NOT NULL,
    data_time       TEXT,
    time_basis      TEXT NOT NULL,
    source          TEXT NOT NULL,
    price           REAL,
    metrics_json    TEXT NOT NULL,
    thresholds_json TEXT NOT NULL,
    data_status     TEXT NOT NULL,
    indicator_trade_date TEXT
);
CREATE INDEX IF NOT EXISTS idx_anomalies_triggered ON anomalies(triggered_at);

CREATE TABLE IF NOT EXISTS daily_stats (
    trade_date        TEXT PRIMARY KEY,
    computed_at       TEXT NOT NULL,
    total             INTEGER,
    traded            INTEGER,
    up                INTEGER,
    down              INTEGER,
    flat              INTEGER,
    limit_up_approx   INTEGER,
    limit_down_approx INTEGER,
    advance_ratio     REAL,
    median_pct_change REAL,
    amount_total_yuan REAL,
    index_json        TEXT,
    source            TEXT NOT NULL,
    data_status       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cleanup_runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    status         TEXT NOT NULL,
    db_size_before INTEGER,
    db_size_after  INTEGER,
    deleted_json   TEXT,
    note           TEXT
);
"""

# 写入白名单：表名与列名无法参数化，必须校验
KNOWN_TABLES = frozenset({
    "meta", "job_state", "stocks", "trade_calendar", "collection_runs", "spot_snapshots",
    "index_snapshots", "industry_snapshots", "industry_members", "breadth_snapshots",
    "minute_bars", "daily_bars", "index_daily", "daily_update_progress", "indicator_values",
    "screen_runs", "screen_results", "anomalies", "daily_stats", "cleanup_runs",
})
# 写入暂停时仍允许写入的“控制类”小表（不含行情数据）
CONTROL_TABLES = frozenset({"meta", "job_state", "cleanup_runs"})
_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class WritePausedError(RuntimeError):
    """数据库已达到容量上限，暂停写入行情数据。"""


def _check_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"非法标识符: {name!r}")
    return name


class Database:
    def __init__(self, path: Path, cache_kib: int = 16384, max_bytes: int = 2_000_000_000,
                 soft_bytes: int = 1_800_000_000) -> None:
        self.path = Path(path)
        self.cache_kib = cache_kib
        self.max_bytes = max_bytes
        self.soft_bytes = soft_bytes
        self._conn: sqlite3.Connection | None = None
        self.write_paused = False
        self.pause_reason = ""

    # -- 连接 -----------------------------------------------------------------
    def connect(self) -> None:
        if self._conn is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not self.path.exists() or self.path.stat().st_size == 0
        conn = sqlite3.connect(str(self.path), timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        if is_new:
            # 必须在建表前设置，之后再改需要全量 VACUUM
            conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute(f"PRAGMA cache_size = -{int(self.cache_kib)}")
        conn.execute("PRAGMA mmap_size = 0")
        conn.execute("PRAGMA temp_store = FILE")
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA journal_size_limit = 67108864")
        conn.execute("PRAGMA wal_autocheckpoint = 1000")
        self._conn = conn

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            logger.exception("关闭前 WAL checkpoint 失败")
        self._conn.close()
        self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("数据库未连接")
        return self._conn

    def init_schema(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self.conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)",
                          ("schema_version", SCHEMA_VERSION))
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", ("schema_version",)).fetchone()
        if row is None or row["value"] != SCHEMA_VERSION:
            raise RuntimeError(f"数据库结构版本不匹配: {row['value'] if row else None} != {SCHEMA_VERSION}")

    # -- 基本执行 -------------------------------------------------------------
    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            # SQLITE_FULL / NOMEM / IOERR 等错误时 SQLite 可能已自动回滚，再次 ROLLBACK 会掩盖原始异常
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, tuple(params))

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, tuple(params)).fetchall()

    def query_one(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        return self.conn.execute(sql, tuple(params)).fetchone()

    def query_df(self, sql: str, params: Sequence[Any] = ()) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.conn, params=tuple(params))

    # -- 写入 -----------------------------------------------------------------
    def ensure_writable(self, table: str) -> None:
        if self.write_paused and table not in CONTROL_TABLES:
            raise WritePausedError(f"数据库容量达到上限，暂停写入 {table}: {self.pause_reason}")

    def _insert_sql(self, table: str, columns: Sequence[str], mode: str) -> tuple[str, int]:
        if table not in KNOWN_TABLES:
            raise ValueError(f"未知表: {table}")
        if mode not in ("REPLACE", "IGNORE"):
            raise ValueError(f"非法写入模式: {mode}")
        cols = [_check_ident(c) for c in columns]
        if not cols:
            raise ValueError("列名不能为空")
        placeholders = ",".join("?" for _ in cols)
        return f"INSERT OR {mode} INTO {table} ({','.join(cols)}) VALUES ({placeholders})", len(cols)

    @staticmethod
    def _executemany_chunked(conn: sqlite3.Connection, sql: str, ncols: int, table: str,
                             rows: Iterable[Sequence[Any]], chunk_size: int) -> int:
        total = 0
        chunk: list[Sequence[Any]] = []
        for row in rows:
            if len(row) != ncols:
                raise ValueError(f"写入 {table} 的行长度 {len(row)} 与列数 {ncols} 不一致")
            chunk.append(row)
            if len(chunk) >= chunk_size:
                conn.executemany(sql, chunk)
                total += len(chunk)
                chunk = []
        if chunk:
            conn.executemany(sql, chunk)
            total += len(chunk)
        return total

    def insert_rows(self, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]],
                    mode: str = "REPLACE", chunk_size: int = 2000) -> int:
        """批量参数化写入。mode: REPLACE（主键冲突时覆盖）/ IGNORE（冲突时跳过）。"""
        sql, ncols = self._insert_sql(table, columns, mode)
        self.ensure_writable(table)
        with self.transaction() as conn:
            return self._executemany_chunked(conn, sql, ncols, table, rows, chunk_size)

    def replace_partition(self, table: str, where_sql: str, where_params: Sequence[Any],
                          columns: Sequence[str], rows: Iterable[Sequence[Any]],
                          chunk_size: int = 2000) -> int:
        """在同一事务中删除某个分区（如某只股票的全部日K）并写入新数据，失败则整体回滚。"""
        if "?" not in where_sql:
            raise ValueError("删除条件必须使用参数化占位符")
        sql, ncols = self._insert_sql(table, columns, "REPLACE")
        self.ensure_writable(table)
        with self.transaction() as conn:
            conn.execute(f"DELETE FROM {table} WHERE {where_sql}", tuple(where_params))
            return self._executemany_chunked(conn, sql, ncols, table, rows, chunk_size)

    def upsert_stocks(self, rows: Iterable[tuple[str, str, str, str, str]]) -> int:
        """rows: (code, name, exchange, seen_at, source)。保留 first_seen_at，更新其余字段。"""
        self.ensure_writable("stocks")
        sql = ("INSERT INTO stocks(code, name, exchange, first_seen_at, last_seen_at, source)"
               " VALUES (?, ?, ?, ?, ?, ?)"
               " ON CONFLICT(code) DO UPDATE SET name = excluded.name, exchange = excluded.exchange,"
               " last_seen_at = excluded.last_seen_at, source = excluded.source")
        prepared = ((c, n, e, ts, ts, s) for c, n, e, ts, s in rows)
        with self.transaction() as conn:
            return self._executemany_chunked(conn, sql, 6, "stocks", prepared, 2000)

    def insert_df(self, table: str, df: pd.DataFrame, columns: Sequence[str],
                  mode: str = "REPLACE") -> int:
        """把 DataFrame 指定列写入数据库。NaN/NaT 写为 NULL（不做任何填充）。"""
        if df.empty:
            return 0
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"写入 {table} 缺少列: {missing}")
        sub = df.loc[:, list(columns)].astype(object)
        sub = sub.where(pd.notna(sub), None)
        return self.insert_rows(table, columns, sub.itertuples(index=False, name=None), mode=mode)

    def delete_in_batches(self, table: str, where_sql: str, params: Sequence[Any],
                          batch_rows: int) -> int:
        """分批删除，避免长事务与大内存占用。where_sql 只能由本项目代码提供，值必须参数化。"""
        if table not in KNOWN_TABLES:
            raise ValueError(f"未知表: {table}")
        if "?" not in where_sql:
            raise ValueError("删除条件必须使用参数化占位符")
        sql = (f"DELETE FROM {table} WHERE rowid IN "
               f"(SELECT rowid FROM {table} WHERE {where_sql} LIMIT ?)")
        deleted = 0
        while True:
            with self.transaction() as conn:
                cur = conn.execute(sql, (*params, int(batch_rows)))
                n = cur.rowcount if cur.rowcount is not None else 0
            deleted += max(n, 0)
            if n < batch_rows:
                break
        return deleted

    # -- 状态键值 -------------------------------------------------------------
    def get_state(self, key: str) -> str | None:
        row = self.query_one("SELECT value FROM job_state WHERE key = ?", (key,))
        return None if row is None else str(row["value"])

    def set_state(self, key: str, value: str) -> None:
        self.execute("INSERT OR REPLACE INTO job_state(key, value, updated_at) VALUES (?, ?, ?)",
                     (key, value, to_db_ts(now_utc())))

    def record_run(self, dataset: str, source: str, started_at: str, status: str,
                   row_count: int | None = None, data_status: str | None = None,
                   quote_time: str | None = None, error_type: str | None = None,
                   error_message: str | None = None, detail: str | None = None) -> None:
        """记录一次采集/任务结果（成功与失败都记录）。暂停写入时降级为仅写日志。"""
        try:
            self.ensure_writable("collection_runs")
        except WritePausedError:
            logger.warning("写入暂停，采集记录仅写日志: dataset=%s status=%s err=%s",
                           dataset, status, error_message)
            return
        self.execute(
            "INSERT INTO collection_runs(dataset, source, started_at, finished_at, status, row_count,"
            " data_status, quote_time, error_type, error_message, detail)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (dataset, source, started_at, to_db_ts(now_utc()), status, row_count, data_status,
             quote_time, error_type, (error_message or "")[:2000] or None, detail))

    # -- 容量 -----------------------------------------------------------------
    def size_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(self.path) + suffix)
            try:
                total += os.path.getsize(p)
            except OSError:
                continue
        return total

    def page_stats(self) -> dict[str, int]:
        page_size = int(self.conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(self.conn.execute("PRAGMA page_count").fetchone()[0])
        freelist = int(self.conn.execute("PRAGMA freelist_count").fetchone()[0])
        auto_vacuum = int(self.conn.execute("PRAGMA auto_vacuum").fetchone()[0])
        return {"page_size": page_size, "page_count": page_count,
                "freelist_count": freelist, "auto_vacuum": auto_vacuum}

    def quick_check(self) -> str:
        row = self.conn.execute("PRAGMA quick_check(1)").fetchone()
        return str(row[0]) if row else "unknown"

    def checkpoint(self) -> None:
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def incremental_vacuum(self, pages: int) -> None:
        """pages <= 0 表示回收全部空闲页。

        注意：该 PRAGMA 每回收一页产生一行结果，必须 fetchall() 把语句执行完，
        否则 Python 只执行一步（只回收一页）。"""
        self.conn.execute(f"PRAGMA incremental_vacuum({int(pages)})").fetchall()

    def full_vacuum(self) -> None:
        self.conn.execute("VACUUM")

    def refresh_space_state(self) -> int:
        """根据当前文件大小更新写入暂停状态，返回字节数。"""
        size = self.size_bytes()
        if size >= self.max_bytes:
            if not self.write_paused:
                logger.critical("数据库大小 %.2f GB 已达上限 %.2f GB，暂停写入行情数据",
                                size / 1e9, self.max_bytes / 1e9)
            self.write_paused = True
            self.pause_reason = f"db_size={size} >= max={self.max_bytes}"
        elif self.write_paused and size < self.soft_bytes:
            logger.warning("数据库大小回落至 %.2f GB，恢复写入", size / 1e9)
            self.write_paused = False
            self.pause_reason = ""
        return size
