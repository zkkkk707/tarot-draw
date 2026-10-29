"""运行期共享状态（单线程使用）。

所有容器都有上限，避免无限缓存：
- 最新快照只保留“最近一份”（覆盖式）
- 错误记录、市场宽度历史使用定长 deque
- 可释放缓存（指标、行业成分映射）在内存守护进入 PROTECT 时被清空，需要时再从数据库读取
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import pandas as pd

from market_data import FetchResult
from timeutil import now_utc, to_bj_iso


@dataclass
class SourceHealth:
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    last_data_status: str | None = None
    last_row_count: int | None = None
    source: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "last_attempt_at": to_bj_iso(self.last_attempt_at) if self.last_attempt_at else None,
            "last_success_at": to_bj_iso(self.last_success_at) if self.last_success_at else None,
            "last_error_at": to_bj_iso(self.last_error_at) if self.last_error_at else None,
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
            "last_data_status": self.last_data_status,
            "last_row_count": self.last_row_count,
        }


@dataclass
class RuntimeState:
    started_at: datetime = field(default_factory=now_utc)
    latest_spot: FetchResult | None = None
    latest_index: FetchResult | None = None
    latest_boards: FetchResult | None = None
    latest_breadth: dict[str, Any] | None = None
    # 当日市场宽度历史（用于“较日内高点恶化”判断），最多保留一个交易日的分钟级数据
    breadth_history: deque = field(default_factory=lambda: deque(maxlen=400))
    breadth_history_day: date | None = None
    # 自选股分钟线摘要：code -> {last_bar_time, close, data_status, fetched_at, source}
    watch_minutes: dict[str, dict[str, Any]] = field(default_factory=dict)
    health: dict[str, SourceHealth] = field(default_factory=dict)
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=50))
    # 快照内容签名，用于检测“数据长时间不变”的疑似过期
    spot_signature: str | None = None
    spot_signature_since: datetime | None = None
    spot_signature_phase: str | None = None
    last_full_spot_store_mono: float = -1e18
    stocks_upserted_day: date | None = None
    last_anomaly_spot_at: datetime | None = None
    anomalies_since_export: int = 0
    latest_screen_run: dict[str, Any] | None = None
    crosscheck: dict[str, Any] | None = None
    # 需要重新导出的 work_data 文件键
    dirty: set[str] = field(default_factory=set)
    # ---- 可释放缓存 ----
    indicator_cache: pd.DataFrame | None = None
    indicator_cache_key: str | None = None
    members_cache: pd.DataFrame | None = None
    members_cache_key: str | None = None

    def health_of(self, dataset: str) -> SourceHealth:
        if dataset not in self.health:
            self.health[dataset] = SourceHealth()
        return self.health[dataset]

    def mark_success(self, dataset: str, source: str, row_count: int, data_status: str) -> None:
        h = self.health_of(dataset)
        now = now_utc()
        h.last_attempt_at = now
        h.last_success_at = now
        h.consecutive_failures = 0
        h.last_data_status = data_status
        h.last_row_count = row_count
        h.source = source

    def mark_failure(self, dataset: str, error: str) -> None:
        h = self.health_of(dataset)
        now = now_utc()
        h.last_attempt_at = now
        h.last_error_at = now
        h.last_error = error[:500]
        h.consecutive_failures += 1
        self.recent_errors.append({"at": to_bj_iso(now), "dataset": dataset, "error": error[:500]})

    def release_optional_caches(self) -> None:
        self.indicator_cache = None
        self.indicator_cache_key = None
        self.members_cache = None
        self.members_cache_key = None
