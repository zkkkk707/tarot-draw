"""行情数据源适配层（默认 AKShare）。

职责
1. 定义与具体数据源无关的接口 MarketDataAdapter，便于将来更换为其他合规行情 API。
2. 实现 AKShareAdapter：限流、超时、有限重试、熔断、字段校验与标准化。
3. 每次获取都返回 FetchResult，携带数据源、获取时间、时间依据（是否有行情自带时间戳）
   与数据状态。

数据真实性原则
- 只做字段重命名、类型转换与校验，绝不生成、插值或填补价格/成交量。
- 接口缺失的字段保持为空（NULL），不得用默认值冒充。
- 接口异常时抛出 DataSourceError，由调用方记录失败，不使用旧数据冒充新数据。

AKShare 注意事项
- AKShare 是开源的数据接口封装库，底层抓取东方财富、新浪等公开网页接口，并非交易所授权的
  官方行情；接口字段可能随上游网页调整而变化，本模块对必需字段做显式校验，变化时会报错。
- 请遵守各数据提供方的使用条款，控制请求频率（见 datasource.min_request_interval_sec）。
- 东方财富实时快照接口不返回逐笔行情时间戳，因此 time_basis=fetch_time_only，
  data_status=unknown_time（实时性未经行情时间戳验证）。分钟K线带有K线时间，可判定实时/延迟。
- 成交量单位：东方财富 A 股接口为“手”（1手=100股），本项目字段名统一写作 volume_lots。
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable

import numpy as np
import pandas as pd

from config import DataSourceConfig
from timeutil import BEIJING_TZ, compact_date, now_utc, to_bj_iso

logger = logging.getLogger(__name__)


class DataStatus:
    REALTIME = "realtime"          # 有行情时间戳，且与获取时间差在阈值内
    DELAYED = "delayed"            # 有行情时间戳，但落后获取时间超过阈值
    HISTORICAL = "historical"      # 已收盘的历史数据（日K、非交易时段分钟K）
    UNKNOWN_TIME = "unknown_time"  # 数据源未提供行情时间，仅有获取时间
    STALE = "stale"                # 判定为过期（内容长时间未更新或超过有效期）
    INVALID = "invalid"            # 未通过校验，不可用于计算


class TimeBasis:
    QUOTE_TIME = "quote_time"            # 行情自带时间戳
    BAR_TIME = "bar_time"                # K线时间
    TRADE_DATE = "trade_date"            # 交易日期（日K）
    FETCH_TIME_ONLY = "fetch_time_only"  # 仅有本地获取时间


class DataSourceError(RuntimeError):
    def __init__(self, dataset: str, message: str) -> None:
        super().__init__(f"[{dataset}] {message}")
        self.dataset = dataset


class CircuitOpenError(DataSourceError):
    """连续失败触发熔断，冷却期内不再请求。"""


class DataValidationError(DataSourceError):
    """返回数据结构或内容不符合要求。"""


@dataclass
class FetchResult:
    dataset: str
    source: str
    df: pd.DataFrame
    fetched_at: datetime
    time_basis: str
    data_status: str
    quote_time: datetime | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        return int(len(self.df))

    def meta(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "source": self.source,
            "fetched_at": to_bj_iso(self.fetched_at),
            "quote_time": to_bj_iso(self.quote_time) if self.quote_time else None,
            "time_basis": self.time_basis,
            "data_status": self.data_status,
            "row_count": self.row_count,
            "warnings": list(self.warnings),
        }


# ---------------------------------------------------------------------------
# 限流与熔断
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, min_interval_sec: float, clock: Callable[[], float] = time.monotonic,
                 sleeper: Callable[[float], Any] = time.sleep) -> None:
        self.min_interval = min_interval_sec
        self._clock = clock
        self._sleep = sleeper
        self._last = -1e18

    def wait(self) -> None:
        now = self._clock()
        delta = now - self._last
        if delta < self.min_interval:
            self._sleep(self.min_interval - delta)
        self._last = self._clock()


class CircuitBreaker:
    def __init__(self, failure_threshold: int, cooldown_sec: float,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown_sec = cooldown_sec
        self._clock = clock
        self.consecutive_failures = 0
        self.opened_at: float | None = None

    def is_open(self) -> bool:
        if self.opened_at is None:
            return False
        if self._clock() - self.opened_at >= self.cooldown_sec:
            # 冷却结束进入半开状态：允许一次试探请求
            self.opened_at = None
            return False
        return True

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.opened_at = None

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.opened_at = self._clock()


# ---------------------------------------------------------------------------
# HTTP 超时注入
# ---------------------------------------------------------------------------

def install_default_http_timeout(connect_sec: float, read_sec: float) -> bool:
    """为 requests 的所有未显式指定 timeout 的请求注入默认超时。

    AKShare 内部多数接口直接调用 requests.get(...) 且未设置 timeout，网络卡住时可能无限等待。
    这里包装 requests.Session.request（只在本进程内生效），对未传 timeout 的调用补上
    (连接超时, 读取超时)。对不使用 requests 的接口无效，调用方仍会记录总耗时。
    """
    try:
        import requests
    except ImportError:
        logger.warning("未安装 requests，无法注入默认超时")
        return False
    original = requests.sessions.Session.request
    if getattr(original, "_astock_timeout_wrapper", False):
        return True

    def request(self: Any, method: Any, url: Any, *args: Any, **kwargs: Any) -> Any:
        # Session.request 的第 7 个位置参数是 timeout；若已按位置传入则不覆盖
        if len(args) < 7 and kwargs.get("timeout") is None:
            kwargs["timeout"] = (connect_sec, read_sec)
        return original(self, method, url, *args, **kwargs)

    request._astock_timeout_wrapper = True  # type: ignore[attr-defined]
    requests.sessions.Session.request = request  # type: ignore[method-assign]
    return True


# ---------------------------------------------------------------------------
# 字段映射与标准化（纯函数，便于单元测试）
# ---------------------------------------------------------------------------

SPOT_COLUMN_MAP = {
    "代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "pct_change", "涨跌额": "change",
    "成交量": "volume_lots", "成交额": "amount_yuan", "振幅": "amplitude_pct", "最高": "high",
    "最低": "low", "今开": "open", "昨收": "prev_close", "量比": "volume_ratio",
    "换手率": "turnover_rate", "市盈率-动态": "pe_dynamic", "市净率": "pb",
    "总市值": "total_mv_yuan", "流通市值": "float_mv_yuan",
}
SPOT_REQUIRED = ("代码", "名称", "最新价", "涨跌幅", "成交量", "成交额")
SPOT_NUMERIC = ("price", "pct_change", "change", "volume_lots", "amount_yuan", "amplitude_pct",
                "high", "low", "open", "prev_close", "volume_ratio", "turnover_rate",
                "pe_dynamic", "pb", "total_mv_yuan", "float_mv_yuan")

INDEX_SPOT_COLUMN_MAP = {
    "代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "pct_change", "涨跌额": "change",
    "成交量": "volume", "成交额": "amount_yuan", "最高": "high", "最低": "low", "今开": "open",
    "昨收": "prev_close",
}
INDEX_SPOT_REQUIRED = ("代码", "名称", "最新价", "涨跌幅")
INDEX_SPOT_NUMERIC = ("price", "pct_change", "change", "volume", "amount_yuan", "high", "low",
                      "open", "prev_close")

DAILY_COLUMN_MAP = {
    "日期": "trade_date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
    "成交量": "volume_lots", "成交额": "amount_yuan", "振幅": "amplitude_pct",
    "涨跌幅": "pct_change", "涨跌额": "change", "换手率": "turnover_rate",
}
DAILY_REQUIRED = ("日期", "开盘", "收盘", "最高", "最低", "成交量")
DAILY_NUMERIC = ("open", "close", "high", "low", "volume_lots", "amount_yuan", "amplitude_pct",
                 "pct_change", "change", "turnover_rate")

INDEX_DAILY_COLUMN_MAP = {
    "日期": "trade_date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
    "成交量": "volume", "成交额": "amount_yuan", "涨跌幅": "pct_change",
}
INDEX_DAILY_REQUIRED = ("日期", "开盘", "收盘", "最高", "最低")
INDEX_DAILY_NUMERIC = ("open", "close", "high", "low", "volume", "amount_yuan", "pct_change")

MINUTE_COLUMN_MAP = {
    "时间": "bar_time", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
    "成交量": "volume_lots", "成交额": "amount_yuan",
}
MINUTE_REQUIRED = ("时间", "开盘", "收盘", "最高", "最低", "成交量")
MINUTE_NUMERIC = ("open", "close", "high", "low", "volume_lots", "amount_yuan")

BOARD_COLUMN_MAP = {
    "板块名称": "board_name", "板块代码": "board_code", "最新价": "price", "涨跌幅": "pct_change",
    "总市值": "total_mv_yuan", "换手率": "turnover_rate", "上涨家数": "up_count",
    "下跌家数": "down_count", "领涨股票": "leader_name", "领涨股票-涨跌幅": "leader_pct_change",
}
BOARD_REQUIRED = ("板块名称", "板块代码", "涨跌幅")
BOARD_NUMERIC = ("price", "pct_change", "total_mv_yuan", "turnover_rate", "up_count",
                 "down_count", "leader_pct_change")


def _require_columns(raw: pd.DataFrame, required: tuple[str, ...], dataset: str) -> None:
    if not isinstance(raw, pd.DataFrame):
        raise DataValidationError(dataset, f"返回类型不是 DataFrame: {type(raw).__name__}")
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise DataValidationError(
            dataset, f"缺少必需字段 {missing}，实际字段 {list(raw.columns)[:30]}；"
                     "可能是 AKShare 版本或上游接口变更")


def _standardize(raw: pd.DataFrame, mapping: dict[str, str], numeric: tuple[str, ...]) -> pd.DataFrame:
    keep = [c for c in mapping if c in raw.columns]
    df = raw.loc[:, keep].rename(columns=mapping).copy()
    for col in mapping.values():
        if col not in df.columns:
            df[col] = np.nan  # 数据源未提供的字段保持为空，不做填充
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def normalize_code(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    # 去掉可能出现的交易所前缀，如 sh600000 / SZ000001 / 600000.SH
    s = s.str.replace(r"^(sh|sz|bj|SH|SZ|BJ)", "", regex=True)
    s = s.str.replace(r"\.(SH|SZ|BJ|sh|sz|bj)$", "", regex=True)
    return s.str.zfill(6)


def exchange_of(code: str) -> str:
    if code.startswith(("60", "68", "90")):
        return "SH"
    if code.startswith(("00", "30", "20")):
        return "SZ"
    if code.startswith(("8", "4", "92")):
        return "BJ"
    return "UNKNOWN"


def is_bse(code: str) -> bool:
    return exchange_of(code) == "BJ"


def price_limit_pct(code: str, name: str) -> float:
    """涨跌幅限制的近似值（用于涨停/跌停家数“近似统计”，不用于交易判断）。

    主板 10%，ST 5%，创业板/科创板 20%，北交所 30%。新股上市初期无涨跌幅限制，
    注册制下 ST 规则也有差异，因此统计结果字段名带 _approx 后缀。
    """
    if code.startswith(("30", "68")):
        return 20.0
    if is_bse(code):
        return 30.0
    if "ST" in str(name).upper():
        return 5.0
    return 10.0


def normalize_spot(raw: pd.DataFrame, dataset: str = "spot") -> tuple[pd.DataFrame, list[str]]:
    _require_columns(raw, SPOT_REQUIRED, dataset)
    df = _standardize(raw, SPOT_COLUMN_MAP, SPOT_NUMERIC)
    warnings: list[str] = []
    df["code"] = normalize_code(df["code"])
    df["name"] = df["name"].astype(str).str.strip()
    bad_code = ~df["code"].str.fullmatch(r"\d{6}")
    if bad_code.any():
        warnings.append(f"dropped_invalid_code_rows={int(bad_code.sum())}")
        df = df.loc[~bad_code]
    dup = df["code"].duplicated(keep="first")
    if dup.any():
        warnings.append(f"dropped_duplicate_code_rows={int(dup.sum())}")
        df = df.loc[~dup]
    df = df.reset_index(drop=True)
    df["exchange"] = df["code"].map(exchange_of)
    df["validation_flags"] = spot_validation_flags(df)
    return df, warnings


def spot_validation_flags(df: pd.DataFrame) -> pd.Series:
    """逐行校验，返回以分号分隔的标记；不修改任何数值。"""
    flags = pd.Series([""] * len(df), index=df.index, dtype=object)

    def add(mask: pd.Series, label: str) -> None:
        mask = mask.fillna(False)
        flags.loc[mask] = flags.loc[mask] + label + ";"

    add(df["price"].isna(), "price_missing")
    add(df["price"] <= 0, "price_non_positive")
    add(df["high"] < df["low"], "high_lt_low")
    add(df["price"] > df["high"] * 1.001, "price_above_high")
    add(df["price"] < df["low"] * 0.999, "price_below_low")
    add(df["pct_change"].abs() > 35, "pct_change_extreme")
    add(df["volume_lots"] < 0, "negative_volume")
    add(df["amount_yuan"] < 0, "negative_amount")
    return flags.str.rstrip(";")


def normalize_index_spot(raw: pd.DataFrame, codes: list[str], dataset: str = "index_spot") -> pd.DataFrame:
    _require_columns(raw, INDEX_SPOT_REQUIRED, dataset)
    df = _standardize(raw, INDEX_SPOT_COLUMN_MAP, INDEX_SPOT_NUMERIC)
    df["code"] = normalize_code(df["code"])
    df = df.loc[df["code"].isin(codes)].drop_duplicates("code").reset_index(drop=True)
    return df


def normalize_daily(raw: pd.DataFrame, code: str, dataset: str = "daily_bars") -> pd.DataFrame:
    if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
        return pd.DataFrame(columns=["code", "trade_date", *DAILY_NUMERIC])
    _require_columns(raw, DAILY_REQUIRED, dataset)
    df = _standardize(raw, DAILY_COLUMN_MAP, DAILY_NUMERIC)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["trade_date"])
    df["code"] = code
    return df.drop_duplicates("trade_date", keep="last").sort_values("trade_date").reset_index(drop=True)


def normalize_index_daily(raw: pd.DataFrame, code: str, dataset: str = "index_daily") -> pd.DataFrame:
    if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
        return pd.DataFrame(columns=["code", "trade_date", *INDEX_DAILY_NUMERIC])
    _require_columns(raw, INDEX_DAILY_REQUIRED, dataset)
    df = _standardize(raw, INDEX_DAILY_COLUMN_MAP, INDEX_DAILY_NUMERIC)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["trade_date"])
    df["code"] = code
    return df.drop_duplicates("trade_date", keep="last").sort_values("trade_date").reset_index(drop=True)


def normalize_minute(raw: pd.DataFrame, code: str, period: str,
                     dataset: str = "minute_bars") -> pd.DataFrame:
    """分钟K线：时间按北京时间解析，转换为 UTC 字符串存储（bar_time）。"""
    if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
        return pd.DataFrame(columns=["code", "period", "bar_time", *MINUTE_NUMERIC])
    _require_columns(raw, MINUTE_REQUIRED, dataset)
    df = _standardize(raw, MINUTE_COLUMN_MAP, MINUTE_NUMERIC)
    ts = pd.to_datetime(df["bar_time"], errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize(BEIJING_TZ)
    ts = ts.dt.tz_convert("UTC")
    df["bar_time"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    df = df.dropna(subset=["bar_time"])
    df["code"] = code
    df["period"] = str(period)
    return df.drop_duplicates("bar_time", keep="last").sort_values("bar_time").reset_index(drop=True)


def normalize_boards(raw: pd.DataFrame, dataset: str = "industry_boards") -> pd.DataFrame:
    _require_columns(raw, BOARD_REQUIRED, dataset)
    df = _standardize(raw, BOARD_COLUMN_MAP, BOARD_NUMERIC)
    df["board_code"] = df["board_code"].astype(str).str.strip()
    df["board_name"] = df["board_name"].astype(str).str.strip()
    return df.drop_duplicates("board_code").reset_index(drop=True)


def normalize_members(raw: pd.DataFrame, board_code: str, board_name: str,
                      dataset: str = "industry_members") -> pd.DataFrame:
    _require_columns(raw, ("代码", "名称"), dataset)
    df = raw.loc[:, ["代码", "名称"]].rename(columns={"代码": "code", "名称": "name"}).copy()
    df["code"] = normalize_code(df["code"])
    df = df.loc[df["code"].str.fullmatch(r"\d{6}")].drop_duplicates("code")
    df["board_code"] = board_code
    df["board_name"] = board_name
    return df.reset_index(drop=True)


def classify_bar_freshness(last_bar_time: datetime | None, fetched_at: datetime,
                           in_session: bool, max_lag_sec: float) -> str:
    """根据K线时间判断 realtime / delayed / historical。"""
    if last_bar_time is None:
        return DataStatus.UNKNOWN_TIME
    if not in_session:
        return DataStatus.HISTORICAL
    lag = (fetched_at - last_bar_time).total_seconds()
    return DataStatus.REALTIME if lag <= max_lag_sec else DataStatus.DELAYED


# ---------------------------------------------------------------------------
# 适配器接口
# ---------------------------------------------------------------------------

class MarketDataAdapter(ABC):
    """行情数据源抽象接口。新增数据源时实现以下方法并返回相同结构的 DataFrame/FetchResult。"""

    provider: str = "abstract"

    @abstractmethod
    def fetch_spot(self) -> FetchResult: ...

    @abstractmethod
    def fetch_index_spot(self, codes: list[str]) -> FetchResult: ...

    @abstractmethod
    def fetch_industry_boards(self) -> FetchResult: ...

    @abstractmethod
    def fetch_industry_members(self, board_code: str, board_name: str) -> FetchResult: ...

    @abstractmethod
    def fetch_daily_bars(self, code: str, start: date, end: date, adjust: str) -> FetchResult: ...

    @abstractmethod
    def fetch_index_daily(self, code: str, start: date, end: date) -> FetchResult: ...

    @abstractmethod
    def fetch_minute_bars(self, code: str, start: datetime, end: datetime,
                          period: str = "1") -> FetchResult: ...

    @abstractmethod
    def fetch_trade_calendar(self) -> list[date]: ...

    def status(self) -> dict[str, Any]:
        return {"provider": self.provider}


class AKShareAdapter(MarketDataAdapter):
    provider = "akshare"

    def __init__(self, cfg: DataSourceConfig, ak_module: Any | None = None,
                 sleeper: Callable[[float], Any] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.cfg = cfg
        self._ak = ak_module
        self._sleep = sleeper
        self._clock = clock
        self._limiter = RateLimiter(cfg.min_request_interval_sec, clock=clock, sleeper=sleeper)
        self._breakers: dict[str, CircuitBreaker] = {}
        self._last_durations: dict[str, float] = {}
        self._timeout_installed = False

    # -- 底层调用 -------------------------------------------------------------
    def _module(self) -> Any:
        if self._ak is None:
            if not self._timeout_installed:
                self._timeout_installed = install_default_http_timeout(
                    self.cfg.http_connect_timeout_sec, self.cfg.http_read_timeout_sec)
            import akshare as ak  # 延迟导入：akshare 及其依赖体积较大

            self._ak = ak
        return self._ak

    def _breaker(self, dataset: str) -> CircuitBreaker:
        if dataset not in self._breakers:
            self._breakers[dataset] = CircuitBreaker(
                self.cfg.circuit_breaker_failures, self.cfg.circuit_breaker_cooldown_sec, clock=self._clock)
        return self._breakers[dataset]

    def _call(self, dataset: str, func_name: str, **kwargs: Any) -> pd.DataFrame:
        breaker = self._breaker(dataset)
        if breaker.is_open():
            raise CircuitOpenError(dataset, f"连续失败 {breaker.consecutive_failures} 次，熔断冷却中")
        ak = self._module()
        fn = getattr(ak, func_name, None)
        if fn is None:
            breaker.record_failure()
            raise DataSourceError(dataset, f"当前 AKShare 版本没有函数 {func_name}，请检查版本")
        attempts = 1 + max(0, self.cfg.max_retries)
        last_exc: Exception | None = None
        for attempt in range(attempts):
            self._limiter.wait()
            started = self._clock()
            try:
                result = fn(**kwargs)
            except MemoryError:
                raise
            except Exception as exc:  # 网络/解析异常：有限次重试
                last_exc = exc
                logger.warning("[%s] %s 第 %d/%d 次失败: %s: %s", dataset, func_name, attempt + 1,
                               attempts, type(exc).__name__, exc)
                if attempt + 1 < attempts:
                    self._sleep(self.cfg.retry_backoff_sec * (2 ** attempt))
                continue
            finally:
                self._last_durations[dataset] = round(self._clock() - started, 3)
            if not isinstance(result, pd.DataFrame):
                breaker.record_failure()
                raise DataValidationError(dataset, f"{func_name} 返回类型 {type(result).__name__}")
            breaker.record_success()
            return result
        breaker.record_failure()
        raise DataSourceError(dataset, f"{func_name} 在 {attempts} 次尝试后仍失败: "
                                       f"{type(last_exc).__name__}: {last_exc}") from last_exc

    def _source(self, func_name: str, upstream: str) -> str:
        return f"akshare:{func_name}({upstream})"

    # -- 具体接口 -------------------------------------------------------------
    def fetch_spot(self) -> FetchResult:
        dataset = "spot"
        raw = self._call(dataset, "stock_zh_a_spot_em")
        fetched_at = now_utc()
        df, warnings = normalize_spot(raw, dataset)
        if len(df) < self.cfg.spot_min_rows:
            warnings.append(f"row_count_below_min={len(df)}<{self.cfg.spot_min_rows}")
            status = DataStatus.INVALID
        else:
            status = DataStatus.UNKNOWN_TIME
        return FetchResult(dataset, self._source("stock_zh_a_spot_em", "eastmoney"), df, fetched_at,
                           TimeBasis.FETCH_TIME_ONLY, status, None, warnings)

    def fetch_index_spot(self, codes: list[str]) -> FetchResult:
        dataset = "index_spot"
        try:
            raw = self._call(dataset, "stock_zh_index_spot_em", symbol="沪深重要指数")
            source = self._source("stock_zh_index_spot_em", "eastmoney")
        except CircuitOpenError:
            raise
        except DataSourceError as primary_exc:
            logger.warning("东方财富指数快照失败，尝试新浪指数快照: %s", primary_exc)
            raw = self._call("index_spot_sina", "stock_zh_index_spot_sina")
            source = self._source("stock_zh_index_spot_sina", "sina")
        fetched_at = now_utc()
        df = normalize_index_spot(raw, codes, dataset)
        warnings: list[str] = []
        missing = sorted(set(codes) - set(df["code"]))
        if missing:
            warnings.append(f"missing_index_codes={','.join(missing)}")
        status = DataStatus.UNKNOWN_TIME if not df.empty else DataStatus.INVALID
        return FetchResult(dataset, source, df, fetched_at, TimeBasis.FETCH_TIME_ONLY, status, None, warnings)

    def fetch_industry_boards(self) -> FetchResult:
        dataset = "industry_boards"
        raw = self._call(dataset, "stock_board_industry_name_em")
        fetched_at = now_utc()
        df = normalize_boards(raw, dataset)
        status = DataStatus.UNKNOWN_TIME if len(df) >= 20 else DataStatus.INVALID
        warnings = [] if len(df) >= 20 else [f"board_count_low={len(df)}"]
        return FetchResult(dataset, self._source("stock_board_industry_name_em", "eastmoney"), df,
                           fetched_at, TimeBasis.FETCH_TIME_ONLY, status, None, warnings)

    def fetch_industry_members(self, board_code: str, board_name: str) -> FetchResult:
        dataset = "industry_members"
        raw = self._call(dataset, "stock_board_industry_cons_em", symbol=board_name)
        fetched_at = now_utc()
        df = normalize_members(raw, board_code, board_name, dataset)
        return FetchResult(dataset, self._source("stock_board_industry_cons_em", "eastmoney"), df,
                           fetched_at, TimeBasis.FETCH_TIME_ONLY, DataStatus.HISTORICAL)

    def fetch_daily_bars(self, code: str, start: date, end: date, adjust: str) -> FetchResult:
        dataset = "daily_bars"
        raw = self._call(dataset, "stock_zh_a_hist", symbol=code, period="daily",
                         start_date=compact_date(start), end_date=compact_date(end), adjust=adjust)
        fetched_at = now_utc()
        df = normalize_daily(raw, code, dataset)
        return FetchResult(dataset, self._source("stock_zh_a_hist", "eastmoney"), df, fetched_at,
                           TimeBasis.TRADE_DATE, DataStatus.HISTORICAL)

    def fetch_index_daily(self, code: str, start: date, end: date) -> FetchResult:
        dataset = "index_daily"
        raw = self._call(dataset, "index_zh_a_hist", symbol=code, period="daily",
                         start_date=compact_date(start), end_date=compact_date(end))
        fetched_at = now_utc()
        df = normalize_index_daily(raw, code, dataset)
        return FetchResult(dataset, self._source("index_zh_a_hist", "eastmoney"), df, fetched_at,
                           TimeBasis.TRADE_DATE, DataStatus.HISTORICAL)

    def fetch_minute_bars(self, code: str, start: datetime, end: datetime,
                          period: str = "1") -> FetchResult:
        dataset = "minute_bars"
        fmt = "%Y-%m-%d %H:%M:%S"
        raw = self._call(dataset, "stock_zh_a_hist_min_em", symbol=code,
                         start_date=start.astimezone(BEIJING_TZ).strftime(fmt),
                         end_date=end.astimezone(BEIJING_TZ).strftime(fmt),
                         period=str(period), adjust="")
        fetched_at = now_utc()
        df = normalize_minute(raw, code, period, dataset)
        last_bar: datetime | None = None
        if not df.empty:
            last_bar = datetime.fromisoformat(str(df["bar_time"].iloc[-1]))
        # 实时/延迟的最终判定需要结合交易时段，由采集器调用 classify_bar_freshness 完成
        return FetchResult(dataset, self._source("stock_zh_a_hist_min_em", "eastmoney"), df, fetched_at,
                           TimeBasis.BAR_TIME, DataStatus.UNKNOWN_TIME if last_bar is None else DataStatus.HISTORICAL,
                           last_bar)

    def fetch_trade_calendar(self) -> list[date]:
        raw = self._call("trade_calendar", "tool_trade_date_hist_sina")
        _require_columns(raw, ("trade_date",), "trade_calendar")
        dates = pd.to_datetime(raw["trade_date"], errors="coerce").dropna().dt.date
        return list(dates)

    def status(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "breakers": {k: {"consecutive_failures": b.consecutive_failures, "open": b.opened_at is not None}
                         for k, b in self._breakers.items()},
            "last_call_duration_sec": dict(self._last_durations),
        }


def build_adapter(cfg: DataSourceConfig, sleeper: Callable[[float], Any] = time.sleep) -> MarketDataAdapter:
    if cfg.provider == "akshare":
        return AKShareAdapter(cfg, sleeper=sleeper)
    raise ValueError(f"不支持的数据源: {cfg.provider}")
