"""时间与时区工具。

约定：
- 数据库中所有“时间戳”统一以 UTC、带偏移量的 ISO-8601 字符串存储，例如
  "2026-10-29T01:31:05+00:00"。统一偏移量保证字符串比较与时间先后一致。
- 对外展示（work_data 文件）统一使用北京时间，例如 "2026-10-29T09:31:05+08:00"。
- “交易日期”是日历日期（不是时间戳），以 "YYYY-MM-DD" 存储，按北京时间划分。

北京时间使用固定 UTC+8 偏移。中国自 1991 年起不实行夏令时，固定偏移对当前及未来数据
是准确的；这样也避免了 Windows 上 zoneinfo 需要额外安装 tzdata 的问题。
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

UTC = timezone.utc
BEIJING_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def now_bj() -> datetime:
    return datetime.now(tz=BEIJING_TZ)


def _require_aware(dt: datetime) -> None:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f"需要带时区的时间，收到无时区时间: {dt!r}")


def to_db_ts(dt: datetime) -> str:
    """转为数据库存储格式（UTC，秒级精度）。"""
    _require_aware(dt)
    return dt.astimezone(UTC).isoformat(timespec="seconds")


def to_bj_iso(dt: datetime) -> str:
    """转为对外展示格式（北京时间，秒级精度）。"""
    _require_aware(dt)
    return dt.astimezone(BEIJING_TZ).isoformat(timespec="seconds")


def from_db_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    _require_aware(dt)
    return dt


def db_ts_to_bj_iso(value: str | None) -> str | None:
    if not value:
        return None
    return to_bj_iso(from_db_ts(value))


def bj_naive_to_aware(dt: datetime) -> datetime:
    """数据源返回的无时区时间（约定为北京时间）转为带时区时间。"""
    if dt.tzinfo is not None:
        return dt.astimezone(BEIJING_TZ)
    return dt.replace(tzinfo=BEIJING_TZ)


def bj_date(dt: datetime) -> date:
    _require_aware(dt)
    return dt.astimezone(BEIJING_TZ).date()


def date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def compact_date(d: date) -> str:
    """AKShare 常用的 YYYYMMDD 格式。"""
    return d.strftime("%Y%m%d")


def parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def utc_cutoff_days(days: float, now: datetime | None = None) -> str:
    """返回 “now - days” 的数据库时间戳字符串，用于参数化删除/查询。"""
    base = now if now is not None else now_utc()
    return to_db_ts(base - timedelta(days=days))


def date_cutoff_days(days: int, today: date | None = None) -> str:
    base = today if today is not None else now_bj().date()
    return date_str(base - timedelta(days=days))
