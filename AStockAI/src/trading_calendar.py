"""中国A股交易日历与交易时段判断。

交易日来源：AKShare tool_trade_date_hist_sina()（新浪财经发布的历史及当年交易日列表，
包含已公布的法定节假日休市安排）。获取成功后缓存到数据库 trade_calendar 表。

状态：
- verified          : 当日在已获取的交易日历覆盖范围内，结论可信
- fallback_weekday  : 交易日历不可用或未覆盖当日，按“周一至周五”临时判断；此时采集到的数据
                      若内容与上一交易日完全相同，会被新鲜度检测标记为 stale。
- unavailable       : 不允许回退时，无法判断，调度器不执行盘中采集

交易时段（北京时间，沪深交易所现行规则）：
- 09:15–09:25 开盘集合竞价（09:20 之后不可撤单）
- 09:30–11:30 上午连续竞价
- 13:00–14:57 下午连续竞价
- 14:57–15:00 收盘集合竞价（沪深主板、创业板、科创板均已实行）
本系统把 13:00–15:00 视为下午交易时段整体。
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Callable

from timeutil import BEIJING_TZ, date_str, now_utc, to_db_ts

logger = logging.getLogger(__name__)


class SessionPhase(Enum):
    NON_TRADING_DAY = "non_trading_day"
    UNKNOWN_DAY = "unknown_day"
    PRE_OPEN = "pre_open"            # 00:00–09:15
    CALL_AUCTION = "call_auction"    # 09:15–09:25
    PRE_CONTINUOUS = "pre_continuous"  # 09:25–09:30（集合竞价结果已产生，连续竞价未开始）
    MORNING = "morning"              # 09:30–11:30
    LUNCH_BREAK = "lunch_break"      # 11:30–13:00
    AFTERNOON = "afternoon"          # 13:00–15:00
    POST_CLOSE = "post_close"        # 15:00–24:00


TRADING_PHASES = frozenset({SessionPhase.MORNING, SessionPhase.AFTERNOON})

_T0915 = time(9, 15)
_T0925 = time(9, 25)
_T0930 = time(9, 30)
_T1130 = time(11, 30)
_T1300 = time(13, 0)
_T1500 = time(15, 0)


def phase_of_time(t: time) -> SessionPhase:
    """仅按时刻判断所处时段（调用前需已确认当天为交易日）。"""
    if t < _T0915:
        return SessionPhase.PRE_OPEN
    if t < _T0925:
        return SessionPhase.CALL_AUCTION
    if t < _T0930:
        return SessionPhase.PRE_CONTINUOUS
    if t < _T1130:
        return SessionPhase.MORNING
    if t < _T1300:
        return SessionPhase.LUNCH_BREAK
    if t < _T1500:
        return SessionPhase.AFTERNOON
    return SessionPhase.POST_CLOSE


class TradingCalendar:
    """交易日历。fetcher 返回交易日列表；db 用于缓存（可为 None，仅在内存中保存）。"""

    def __init__(self, fetcher: Callable[[], list[date]] | None, db: object | None = None,
                 allow_weekday_fallback: bool = True, source_name: str = "akshare:tool_trade_date_hist_sina") -> None:
        self._fetcher = fetcher
        self._db = db
        self._allow_fallback = allow_weekday_fallback
        self._source = source_name
        self._dates: set[date] = set()
        self._min: date | None = None
        self._max: date | None = None
        self.last_refresh_day: date | None = None
        self.last_error: str | None = None

    # -- 加载与刷新 -----------------------------------------------------------
    def load_cached(self) -> int:
        if self._db is None:
            return 0
        rows = self._db.query("SELECT trade_date FROM trade_calendar")  # type: ignore[attr-defined]
        dates = {date.fromisoformat(r["trade_date"]) for r in rows}
        self._set_dates(dates)
        return len(dates)

    def _set_dates(self, dates: set[date]) -> None:
        self._dates = dates
        self._min = min(dates) if dates else None
        self._max = max(dates) if dates else None

    def refresh(self, today: date) -> bool:
        """每天最多成功刷新一次；失败时保留缓存并记录错误，不伪造日历。"""
        if self.last_refresh_day == today and self._dates:
            return True
        if self._fetcher is None:
            return bool(self._dates)
        try:
            fetched = self._fetcher()
        except Exception as exc:  # 网络或接口异常：记录并沿用缓存
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.warning("交易日历获取失败，沿用缓存（%d 天）: %s", len(self._dates), self.last_error)
            return False
        dates = {d for d in fetched if isinstance(d, date)}
        if len(dates) < 200:
            self.last_error = f"交易日历数据异常：仅 {len(dates)} 条"
            logger.warning(self.last_error)
            return False
        self._set_dates(dates)
        self.last_refresh_day = today
        self.last_error = None
        if self._db is not None:
            ts = to_db_ts(now_utc())
            self._db.insert_rows(  # type: ignore[attr-defined]
                "trade_calendar", ["trade_date", "source", "fetched_at"],
                ((date_str(d), self._source, ts) for d in sorted(dates)), mode="REPLACE")
        logger.info("交易日历已更新: %d 个交易日，范围 %s ~ %s", len(dates), self._min, self._max)
        return True

    # -- 查询 -----------------------------------------------------------------
    def covers(self, d: date) -> bool:
        return self._min is not None and self._max is not None and self._min <= d <= self._max

    def is_trading_day(self, d: date) -> tuple[bool | None, str]:
        """返回 (是否交易日, 判断依据)。无法判断时返回 (None, 'unavailable')。"""
        if self.covers(d):
            return d in self._dates, "verified"
        if self._allow_fallback:
            return d.weekday() < 5, "fallback_weekday"
        return None, "unavailable"

    def phase(self, now: datetime) -> tuple[SessionPhase, str]:
        bj = now.astimezone(BEIJING_TZ)
        is_td, basis = self.is_trading_day(bj.date())
        if is_td is None:
            return SessionPhase.UNKNOWN_DAY, basis
        if not is_td:
            return SessionPhase.NON_TRADING_DAY, basis
        return phase_of_time(bj.time()), basis

    def previous_trading_day(self, d: date, max_lookback: int = 30) -> date | None:
        """严格早于 d 的最近一个交易日。"""
        cur = d
        for _ in range(max_lookback):
            cur = cur - timedelta(days=1)
            is_td, _basis = self.is_trading_day(cur)
            if is_td:
                return cur
        return None

    def next_trading_day(self, d: date, max_lookahead: int = 30) -> date | None:
        """严格晚于 d 的最近一个交易日；无法判断时返回 None。"""
        cur = d
        for _ in range(max_lookahead):
            cur = cur + timedelta(days=1)
            is_td, _basis = self.is_trading_day(cur)
            if is_td is None:
                return None
            if is_td:
                return cur
        return None

    def latest_completed_trading_day(self, now: datetime) -> date | None:
        """已经收盘的最近交易日：当日 15:00 之后为当日，否则为前一交易日。"""
        bj = now.astimezone(BEIJING_TZ)
        is_td, _ = self.is_trading_day(bj.date())
        if is_td and bj.time() >= _T1500:
            return bj.date()
        return self.previous_trading_day(bj.date())

    def status(self) -> dict[str, object]:
        return {
            "source": self._source,
            "cached_days": len(self._dates),
            "range_start": date_str(self._min) if self._min else None,
            "range_end": date_str(self._max) if self._max else None,
            "last_refresh_day": date_str(self.last_refresh_day) if self.last_refresh_day else None,
            "last_error": self.last_error,
            "weekday_fallback_allowed": self._allow_fallback,
        }
