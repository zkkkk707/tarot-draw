"""pytest 公共夹具。

说明：测试只使用本地构造的小样本 DataFrame 和临时 SQLite 数据库，不访问网络、不调用 AKShare。
测试数据仅用于验证计算与流程逻辑，绝不会写入生产数据库或 work_data 目录。
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import Settings, settings_from_dict  # noqa: E402
from database import Database  # noqa: E402
from market_data import DataStatus, FetchResult, MarketDataAdapter, TimeBasis, normalize_spot  # noqa: E402
from timeutil import BEIJING_TZ, UTC  # noqa: E402


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    s = settings_from_dict({
        "paths": {"root": str(tmp_path)},
        "memory": {"require_hard_limit": False},
        "watchlist": {"codes": ["600000", "000001"]},
    })
    s.ensure_directories()
    return s


@pytest.fixture
def db(settings: Settings) -> Database:
    database = Database(settings.db_path, cache_kib=2048, max_bytes=2_000_000_000, soft_bytes=1_800_000_000)
    database.connect()
    database.init_schema()
    yield database
    database.close()


def make_raw_spot(n: int = 5, **overrides: Any) -> pd.DataFrame:
    """构造与 AKShare stock_zh_a_spot_em 字段一致的小样本（仅测试用）。"""
    codes = [f"{600000 + i:06d}" for i in range(n)]
    df = pd.DataFrame({
        "序号": range(1, n + 1),
        "代码": codes,
        "名称": [f"测试{i}" for i in range(n)],
        "最新价": [10.0 + i for i in range(n)],
        "涨跌幅": [1.0 * (i - 2) for i in range(n)],
        "涨跌额": [0.1 * (i - 2) for i in range(n)],
        "成交量": [1000.0 * (i + 1) for i in range(n)],
        "成交额": [1e8 * (i + 1) for i in range(n)],
        "振幅": [2.0] * n,
        "最高": [10.5 + i for i in range(n)],
        "最低": [9.5 + i for i in range(n)],
        "今开": [10.0 + i for i in range(n)],
        "昨收": [10.0 + i for i in range(n)],
        "量比": [1.5] * n,
        "换手率": [2.0] * n,
        "市盈率-动态": [15.0] * n,
        "市净率": [1.5] * n,
        "总市值": [1e10] * n,
        "流通市值": [8e9] * n,
    })
    for k, v in overrides.items():
        df[k] = v
    return df


def make_spot_result(raw: pd.DataFrame | None = None, fetched_at: datetime | None = None,
                     status: str = DataStatus.UNKNOWN_TIME) -> FetchResult:
    df, warnings = normalize_spot(raw if raw is not None else make_raw_spot())
    return FetchResult("spot", "test:fixture", df, fetched_at or datetime.now(tz=UTC),
                       TimeBasis.FETCH_TIME_ONLY, status, None, warnings)


def make_daily_bars(code: str, n: int, start: date = date(2025, 1, 2), base: float = 10.0,
                    seed: int = 0) -> pd.DataFrame:
    """构造单只股票的日K小样本（仅测试用；随机游走，固定种子保证可重复）。"""
    rng = np.random.default_rng(seed)
    closes = base * np.cumprod(1 + rng.normal(0, 0.01, n))
    dates = []
    d = start
    while len(dates) < n:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += timedelta(days=1)
    return pd.DataFrame({
        "code": code,
        "trade_date": dates,
        "open": closes * 0.995,
        "high": closes * 1.01,
        "low": closes * 0.99,
        "close": closes,
        "volume_lots": rng.integers(10_000, 20_000, n).astype(float),
        "turnover_rate": 1.0,
    })


class FakeAdapter(MarketDataAdapter):
    """测试用适配器：返回预先给定的数据或抛出预先给定的异常。"""

    provider = "fake"

    def __init__(self) -> None:
        self.spot_queue: list[Any] = []
        self.daily: dict[str, pd.DataFrame] = {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_spot(self) -> FetchResult:
        self.calls.append(("spot", ()))
        item = self.spot_queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def fetch_index_spot(self, codes: list[str]) -> FetchResult:
        raise NotImplementedError

    def fetch_industry_boards(self) -> FetchResult:
        raise NotImplementedError

    def fetch_industry_members(self, board_code: str, board_name: str) -> FetchResult:
        raise NotImplementedError

    def fetch_daily_bars(self, code: str, start: date, end: date, adjust: str) -> FetchResult:
        self.calls.append(("daily", (code, start, end, adjust)))
        df = self.daily.get(code, pd.DataFrame(columns=["code", "trade_date"]))
        if not df.empty:
            df = df.loc[(df["trade_date"] >= start.isoformat()) & (df["trade_date"] <= end.isoformat())]
        return FetchResult("daily_bars", "test:fake_daily", df.reset_index(drop=True),
                           datetime.now(tz=UTC), TimeBasis.TRADE_DATE, DataStatus.HISTORICAL)

    def fetch_index_daily(self, code: str, start: date, end: date) -> FetchResult:
        raise NotImplementedError

    def fetch_minute_bars(self, code: str, start: datetime, end: datetime, period: str = "1") -> FetchResult:
        raise NotImplementedError

    def fetch_trade_calendar(self) -> list[date]:
        return []


def bj(y: int, m: int, d: int, hh: int = 10, mm: int = 0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=BEIJING_TZ)
