"""集中配置管理。

所有可调参数都集中在 config/settings.yaml，本模块负责：
1. 提供带默认值的强类型配置对象（dataclass）。
2. 读取 YAML 并与默认值合并，未知键直接报错（避免拼写错误被静默忽略）。
3. 做基本的取值校验（例如内存阈值必须单调递增且小于 1024 MiB）。

单位约定（本项目所有配置项严格区分）：
- *_mb   : 十进制兆字节，1 MB  = 1,000,000 字节
- *_mib  : 二进制兆字节，1 MiB = 1,048,576 字节
- *_gb   : 十进制吉字节，1 GB  = 1,000,000,000 字节
- *_sec  : 秒
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

MB = 1_000_000
MIB = 1_048_576
GB = 1_000_000_000

# 项目根目录：src 的上一级。用户也可在 settings.yaml 的 paths.root 中显式指定，
# 例如 "E:/AStockAI"。
DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class ConfigError(ValueError):
    """配置文件内容不合法。"""


@dataclass
class PathsConfig:
    root: str = ""  # 为空表示使用 src 的上一级目录
    data_dir: str = "data"
    db_file: str = "data/market.db"
    work_data_dir: str = "work_data"
    logs_dir: str = "logs"
    run_dir: str = "run"  # 运行期锁文件、停止标记文件


@dataclass
class MemoryConfig:
    # 软阈值（十进制 MB），针对“受控进程合计内存”的保守口径：
    # max(私有提交内存合计, 工作集合计)。详见 memory_guard.py。
    warn_mb: int = 750
    protect_mb: int = 850
    critical_mb: int = 950
    # Windows Job Object 硬限制（二进制 MiB），限制的是作业内所有进程的“已提交私有内存”。
    job_memory_limit_mib: int = 960
    process_memory_limit_mib: int = 960
    max_active_processes: int = 4
    # 主进程“硬性工作集上限”（MiB），通过 SetProcessWorkingSetSizeEx 设置；0 表示不设置。
    working_set_hard_max_mib: int = 1000
    # 无法建立 Job Object 硬限制时是否拒绝启动（默认拒绝，保证 <1GiB 约束优先）。
    require_hard_limit: bool = True
    check_interval_sec: float = 5.0
    # 私有工作集（USS）采样代价较高，单独设置较长间隔；0 表示不采样。
    private_ws_sample_interval_sec: float = 300.0
    # 连续多少次处于 CRITICAL 后执行安全退出
    critical_exit_consecutive: int = 6
    # 计算线程数上限（OpenBLAS/MKL/OMP/NumExpr），只影响本进程环境变量
    numeric_threads: int = 1


@dataclass
class DataSourceConfig:
    provider: str = "akshare"
    http_connect_timeout_sec: float = 5.0
    http_read_timeout_sec: float = 20.0
    max_retries: int = 2  # 不含首次请求；总尝试次数 = 1 + max_retries
    retry_backoff_sec: float = 3.0
    min_request_interval_sec: float = 0.6
    circuit_breaker_failures: int = 5
    circuit_breaker_cooldown_sec: float = 300.0
    spot_min_rows: int = 3000  # 全A快照行数低于此值视为不完整
    # 连续交易时段内快照内容完全不变超过该秒数，判定为疑似过期（stale）
    stale_after_sec: float = 180.0
    # 分钟K线最新一根距获取时间不超过该秒数视为 realtime，否则 delayed
    minute_realtime_max_lag_sec: float = 150.0
    daily_adjust: str = "qfq"  # qfq 前复权 / hfq 后复权 / "" 不复权
    index_codes: list[str] = field(default_factory=lambda: ["000001", "399001", "399006"])
    benchmark_index: str = "000001"
    # 交易日历不可用时，是否临时按“周一至周五”判断（状态文件中会标记 fallback_weekday）
    calendar_weekday_fallback: bool = True


@dataclass
class ScheduleConfig:
    tick_sec: float = 1.0
    spot_interval_sec: float = 60.0
    watchlist_interval_sec: float = 30.0
    index_interval_sec: float = 60.0
    industry_interval_sec: float = 120.0
    # 全A快照写入数据库的最小间隔（内存中仍按 spot_interval_sec 更新）
    snapshot_store_interval_sec: float = 300.0
    screening_interval_sec: float = 900.0
    export_interval_sec: float = 60.0
    call_auction_snapshot: bool = True
    # 日K/指标等收盘后任务的最早开始时间。科创板、创业板盘后固定价格交易持续到 15:30，
    # 15:35 之后更新可避免取到未包含盘后成交的日K。
    post_close_start: str = "15:35"
    cleanup_start: str = "15:30"
    cleanup_end: str = "23:30"
    daily_update_batch_size: int = 40
    indicator_batch_size: int = 200
    industry_members_batch_size: int = 5
    daily_history_days: int = 400  # 首次回填的自然日跨度（覆盖约 1 年以上交易日）
    max_backoff_sec: float = 900.0
    daily_job_max_attempts: int = 3
    # 单只股票日K获取失败后，至少间隔多少秒再重试（最多 3 次）
    daily_retry_delay_sec: float = 300.0
    # 全A日K更新范围：all = 全部A股；focus = 自选股 + 最近一次筛选候选
    daily_update_scope: str = "all"


@dataclass
class WatchlistConfig:
    codes: list[str] = field(default_factory=list)
    max_size: int = 20


@dataclass
class IndicatorConfig:
    # 指标窗口（MA5/10/20/60、RSI14、MACD(12,26,9)、20/60日区间等）在 indicators.py 中
    # 固定为常量，保证输出字段名与计算公式一一对应；这里只放可调的信号阈值与批处理参数。
    trading_days_per_year: int = 252
    breakout_volume_multiple: float = 1.8
    pullback_volume_multiple: float = 0.7
    pullback_ma_tolerance: float = 0.02
    history_load_days: int = 220  # 计算指标时每只股票读取的自然日跨度
    store_recent_rows: int = 5  # 每只股票写入数据库的最近指标行数
    min_bars_full: int = 60


@dataclass
class ScreenerConfig:
    model_version: str = "v1.0-initial-hypothesis"
    weight_sector: float = 25.0
    weight_volume_price: float = 25.0
    weight_market: float = 20.0
    weight_catalyst: float = 15.0
    weight_risk_reward: float = 15.0
    top_n: int = 100
    exclude_st: bool = True
    include_bse: bool = False
    min_amount_yuan: float = 50_000_000.0
    # 可计算分值占满分比例低于该值的股票不参与排名
    min_available_ratio: float = 0.6
    volume_ratio_low: float = 1.2
    volume_ratio_high: float = 3.0
    rsi_low: float = 50.0
    rsi_high: float = 75.0
    breadth_bear: float = 0.3
    breadth_bull: float = 0.7
    rr_full_score_ratio: float = 3.0
    vol_low: float = 0.30
    vol_high: float = 0.80


@dataclass
class AnomalyConfig:
    breakout_volume_ratio: float = 2.0
    breakout_buffer: float = 0.0
    pullback_volume_ratio_max: float = 0.8
    pullback_ma_tolerance: float = 0.015
    support_break_buffer: float = 0.005
    spike_min_high_pct: float = 5.0
    spike_min_fade_pct: float = 3.0
    sector_min_pct: float = 2.0
    sector_min_up_ratio: float = 0.75
    sector_min_strong_members: int = 3
    sector_member_min_pct: float = 5.0
    breadth_bad_ratio: float = 0.30
    breadth_drop_from_peak: float = 0.25
    max_events_per_run: int = 300
    min_amount_yuan: float = 20_000_000.0


@dataclass
class StorageConfig:
    # 整个项目文件夹（含 .venv、数据库、日志、导出文件）的总容量上限（十进制GB）。
    # 超过“上限 - 安全余量”时，从最旧的数据开始自动删除，直到回到预算内。
    folder_budget_gb: float = 2.0
    folder_safety_margin_mb: int = 150
    # 扫描非数据库文件（.venv、日志等）总大小的间隔；数据库大小每次检查都实时读取
    folder_scan_interval_sec: float = 1800.0
    # 数据库自身的上限；实际生效上限 = min(db_max_gb, 文件夹预算 - 余量 - 其它文件大小)
    db_max_gb: float = 2.0
    db_soft_limit_gb: float = 1.8
    sqlite_cache_kib: int = 16384
    cleanup_batch_rows: int = 20000
    incremental_vacuum_pages: int = 50000
    full_vacuum_freelist_ratio: float = 0.3
    retention_spot_days: int = 3
    retention_minute_days: int = 7
    retention_daily_days: int = 365
    retention_anomaly_days: int = 30
    retention_screen_days: int = 90
    retention_log_days: int = 7
    retention_runs_days: int = 30
    # 空间紧张时的“紧急保留天数”（仅作用于可删除的高频数据）
    emergency_spot_days: int = 1
    emergency_minute_days: int = 3


@dataclass
class ExportConfig:
    enabled: bool = True
    csv_encoding: str = "utf-8"  # 如需 Excel 直接打开可改为 utf-8-sig
    alerts_max: int = 500
    candidates_max: int = 200
    # 文件在生成后多少秒内被视为新鲜，写入 manifest 的 expires_at
    spot_valid_sec: float = 180.0
    daily_valid_sec: float = 86400.0
    replace_retries: int = 5
    replace_retry_wait_sec: float = 0.2


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file_name: str = "astock.log"
    retention_days: int = 7


@dataclass
class Settings:
    paths: PathsConfig = field(default_factory=PathsConfig)
    memory: MemoryConfig = field(default_factory=MemoryConfig)
    datasource: DataSourceConfig = field(default_factory=DataSourceConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    watchlist: WatchlistConfig = field(default_factory=WatchlistConfig)
    indicators: IndicatorConfig = field(default_factory=IndicatorConfig)
    screener: ScreenerConfig = field(default_factory=ScreenerConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    # ---- 派生路径 -------------------------------------------------------
    @property
    def root_path(self) -> Path:
        return Path(self.paths.root).expanduser() if self.paths.root else DEFAULT_PROJECT_ROOT

    def resolve(self, rel: str) -> Path:
        p = Path(rel).expanduser()
        return p if p.is_absolute() else self.root_path / p

    @property
    def db_path(self) -> Path:
        return self.resolve(self.paths.db_file)

    @property
    def work_data_path(self) -> Path:
        return self.resolve(self.paths.work_data_dir)

    @property
    def logs_path(self) -> Path:
        return self.resolve(self.paths.logs_dir)

    @property
    def run_path(self) -> Path:
        return self.resolve(self.paths.run_dir)

    @property
    def data_path(self) -> Path:
        return self.resolve(self.paths.data_dir)

    def ensure_directories(self) -> None:
        """创建运行所需目录（仅在用户未来手动启动程序时调用）。"""
        for p in (self.data_path, self.db_path.parent, self.work_data_path,
                  self.logs_path, self.run_path):
            p.mkdir(parents=True, exist_ok=True)


T = TypeVar("T")


def _coerce(value: Any, target_type: Any, where: str) -> Any:
    """把 YAML 读取的值转换为 dataclass 字段声明的类型（仅处理本项目用到的简单类型）。"""
    type_str = target_type if isinstance(target_type, str) else getattr(target_type, "__name__", str(target_type))
    if value is None:
        raise ConfigError(f"{where} 不能为 null")
    if type_str == "bool":
        if not isinstance(value, bool):
            raise ConfigError(f"{where} 必须是 true/false，当前为 {value!r}")
        return value
    if type_str == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(f"{where} 必须是整数，当前为 {value!r}")
        return value
    if type_str == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"{where} 必须是数字，当前为 {value!r}")
        return float(value)
    if type_str == "str":
        if not isinstance(value, (str, int)):
            raise ConfigError(f"{where} 必须是字符串，当前为 {value!r}")
        return str(value)
    if type_str.startswith("list[int]"):
        if not isinstance(value, list) or not all(isinstance(v, int) and not isinstance(v, bool) for v in value):
            raise ConfigError(f"{where} 必须是整数列表")
        return list(value)
    if type_str.startswith("list[str]"):
        if not isinstance(value, list):
            raise ConfigError(f"{where} 必须是列表")
        # 股票代码在 YAML 中可能被写成数字（如 600519），统一转为 6 位字符串
        out: list[str] = []
        for v in value:
            if isinstance(v, bool) or not isinstance(v, (str, int)):
                raise ConfigError(f"{where} 中存在非法元素 {v!r}")
            out.append(str(v).strip())
        return out
    return value


def _build_section(cls: type[T], data: dict[str, Any] | None, section: str) -> T:
    if data is None:
        return cls()
    if not isinstance(data, dict):
        raise ConfigError(f"配置段 {section} 必须是映射")
    known = {f.name: f for f in dataclasses.fields(cls)}  # type: ignore[arg-type]
    unknown = set(data) - set(known)
    if unknown:
        raise ConfigError(f"配置段 {section} 存在未知键: {sorted(unknown)}")
    kwargs = {k: _coerce(v, known[k].type, f"{section}.{k}") for k, v in data.items()}
    return cls(**kwargs)


def validate(settings: Settings) -> None:
    m = settings.memory
    if not (0 < m.warn_mb < m.protect_mb < m.critical_mb):
        raise ConfigError("内存阈值必须满足 0 < warn_mb < protect_mb < critical_mb")
    if m.critical_mb * MB >= 1024 * MIB:
        raise ConfigError("critical_mb 必须小于 1024 MiB（约 1073.7 MB）")
    if not (0 < m.job_memory_limit_mib < 1024):
        raise ConfigError("job_memory_limit_mib 必须在 (0, 1024) 区间内")
    if m.process_memory_limit_mib > m.job_memory_limit_mib:
        raise ConfigError("process_memory_limit_mib 不能大于 job_memory_limit_mib")
    if m.critical_mb * MB > m.job_memory_limit_mib * MIB:
        raise ConfigError("critical_mb 应不大于 Job 硬限制，否则软保护在硬限制之后才触发")
    if not (0 <= m.working_set_hard_max_mib < 1024):
        raise ConfigError("working_set_hard_max_mib 必须在 [0, 1024) 区间内（0 表示不设置）")
    if 0 < m.working_set_hard_max_mib < 256:
        raise ConfigError("working_set_hard_max_mib 过小会导致频繁换页，建议不低于 256")
    if m.max_active_processes < 1:
        raise ConfigError("max_active_processes 至少为 1")
    if m.numeric_threads < 1:
        raise ConfigError("numeric_threads 至少为 1")

    s = settings.storage
    if not (0 < s.db_soft_limit_gb < s.db_max_gb):
        raise ConfigError("db_soft_limit_gb 必须小于 db_max_gb")
    if s.db_max_gb > s.folder_budget_gb:
        raise ConfigError("db_max_gb 不能大于 folder_budget_gb")
    if s.folder_safety_margin_mb < 50:
        raise ConfigError("folder_safety_margin_mb 至少 50（WAL 日志等需要余量）")
    if s.folder_safety_margin_mb * MB >= s.folder_budget_gb * GB:
        raise ConfigError("folder_safety_margin_mb 必须小于 folder_budget_gb")
    if s.folder_scan_interval_sec < 60:
        raise ConfigError("folder_scan_interval_sec 至少 60 秒")
    for name in ("retention_spot_days", "retention_minute_days", "retention_daily_days",
                 "retention_anomaly_days", "retention_screen_days", "retention_log_days",
                 "retention_runs_days", "emergency_spot_days", "emergency_minute_days"):
        if getattr(s, name) < 1:
            raise ConfigError(f"storage.{name} 至少为 1 天")
    if s.retention_daily_days < 120:
        raise ConfigError("日K保留天数过短，MA60/MACD 需要至少约 120 个自然日")

    ds = settings.datasource
    if ds.provider != "akshare":
        raise ConfigError("V1.0 仅实现 akshare 适配器；如需更换，请在 market_data.py 中新增适配器")
    if ds.daily_adjust not in ("qfq", "hfq", ""):
        raise ConfigError("daily_adjust 只能是 qfq / hfq / 空字符串")
    if ds.max_retries < 0 or ds.max_retries > 5:
        raise ConfigError("max_retries 应在 0~5 之间，避免无限重试")
    if ds.http_connect_timeout_sec <= 0 or ds.http_read_timeout_sec <= 0:
        raise ConfigError("HTTP 超时必须为正数")
    for code in ds.index_codes + [ds.benchmark_index]:
        if not (len(code) == 6 and code.isdigit()):
            raise ConfigError(f"指数代码格式错误: {code}")

    sc = settings.schedule
    if sc.spot_interval_sec < 30 or sc.watchlist_interval_sec < 15:
        raise ConfigError("采集间隔过短：普通行情不得低于30秒，重点观察不得低于15秒")
    if sc.daily_update_scope not in ("all", "focus"):
        raise ConfigError("daily_update_scope 只能是 all 或 focus")
    for name in ("post_close_start", "cleanup_start", "cleanup_end"):
        _validate_hhmm(getattr(sc, name), f"schedule.{name}")
    if sc.cleanup_start < "15:30":
        raise ConfigError("cleanup_start 不得早于北京时间 15:30")

    wl = settings.watchlist
    for code in wl.codes:
        if not (len(code) == 6 and code.isdigit()):
            raise ConfigError(f"自选股代码必须为6位数字字符串: {code!r}（YAML 中请加引号）")
    if len(wl.codes) > wl.max_size:
        raise ConfigError(f"自选股数量 {len(wl.codes)} 超过 max_size={wl.max_size}，请减少以遵守接口限流")

    ind = settings.indicators
    if ind.history_load_days < 150:
        raise ConfigError("indicators.history_load_days 至少 150 个自然日（MA60 与 MACD 需要足够的预热样本）")
    if sc.daily_history_days < ind.history_load_days:
        raise ConfigError("schedule.daily_history_days 不能小于 indicators.history_load_days")
    if ind.store_recent_rows < 1:
        raise ConfigError("indicators.store_recent_rows 至少为 1")
    for name in ("daily_update_batch_size", "indicator_batch_size", "industry_members_batch_size"):
        if getattr(sc, name) < 1:
            raise ConfigError(f"schedule.{name} 至少为 1")
    if sc.daily_retry_delay_sec < 0:
        raise ConfigError("schedule.daily_retry_delay_sec 不能为负数")
    if sc.indicator_batch_size > 500:
        raise ConfigError("schedule.indicator_batch_size 不宜超过 500（控制单批内存与 SQL 参数数量）")

    ex = settings.export
    if ex.csv_encoding not in ("utf-8", "utf-8-sig"):
        raise ConfigError("export.csv_encoding 只能是 utf-8 或 utf-8-sig")
    if ex.spot_valid_sec <= 0 or ex.daily_valid_sec <= 0:
        raise ConfigError("export 有效期必须为正数")

    sr = settings.screener
    total = (sr.weight_sector + sr.weight_volume_price + sr.weight_market
             + sr.weight_catalyst + sr.weight_risk_reward)
    if abs(total - 100.0) > 1e-6:
        raise ConfigError(f"筛选权重之和必须为 100，当前为 {total}")
    if not (0 < sr.min_available_ratio <= 1):
        raise ConfigError("min_available_ratio 必须在 (0,1] 区间")

    if settings.logging.level.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        raise ConfigError("logging.level 只能是 DEBUG/INFO/WARNING/ERROR")


def _validate_hhmm(value: str, where: str) -> None:
    parts = value.split(":")
    if len(parts) != 2 or not all(p.isdigit() and len(p) == 2 for p in parts):
        raise ConfigError(f"{where} 必须是 HH:MM 格式，当前为 {value!r}")
    hh, mm = int(parts[0]), int(parts[1])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ConfigError(f"{where} 时间非法: {value!r}")


_SECTIONS: dict[str, type] = {
    "paths": PathsConfig,
    "memory": MemoryConfig,
    "datasource": DataSourceConfig,
    "schedule": ScheduleConfig,
    "watchlist": WatchlistConfig,
    "indicators": IndicatorConfig,
    "screener": ScreenerConfig,
    "anomaly": AnomalyConfig,
    "storage": StorageConfig,
    "export": ExportConfig,
    "logging": LoggingConfig,
}


def settings_from_dict(raw: dict[str, Any] | None) -> Settings:
    raw = raw or {}
    if not isinstance(raw, dict):
        raise ConfigError("配置文件顶层必须是映射")
    unknown = set(raw) - set(_SECTIONS)
    if unknown:
        raise ConfigError(f"配置文件存在未知配置段: {sorted(unknown)}")
    kwargs = {name: _build_section(cls, raw.get(name), name) for name, cls in _SECTIONS.items()}
    settings = Settings(**kwargs)
    settings.watchlist.codes = [c.zfill(6) if c.isdigit() else c for c in settings.watchlist.codes]
    validate(settings)
    return settings


def load_settings(path: str | Path) -> Settings:
    """读取 YAML 配置文件。文件不存在时报错，而不是静默使用默认值。"""
    import yaml  # 延迟导入，便于在内存保护建立后再加载第三方库

    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"找不到配置文件: {p}")
    with p.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return settings_from_dict(raw)
