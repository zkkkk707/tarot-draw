from __future__ import annotations

from datetime import datetime

import pytest

from timeutil import BEIJING_TZ, UTC, db_ts_to_bj_iso, from_db_ts, to_bj_iso, to_db_ts


def test_db_ts_is_utc_and_round_trips() -> None:
    dt = datetime(2026, 10, 29, 9, 31, 5, tzinfo=BEIJING_TZ)
    s = to_db_ts(dt)
    assert s == "2026-10-29T01:31:05+00:00"
    assert from_db_ts(s) == dt


def test_display_is_beijing_time() -> None:
    dt = datetime(2026, 10, 29, 1, 31, 5, tzinfo=UTC)
    assert to_bj_iso(dt) == "2026-10-29T09:31:05+08:00"
    assert db_ts_to_bj_iso("2026-10-29T01:31:05+00:00") == "2026-10-29T09:31:05+08:00"


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValueError):
        to_db_ts(datetime(2026, 1, 1, 10, 0))


def test_db_ts_string_order_matches_time_order() -> None:
    a = to_db_ts(datetime(2026, 1, 1, 23, 0, tzinfo=BEIJING_TZ))
    b = to_db_ts(datetime(2026, 1, 2, 1, 0, tzinfo=BEIJING_TZ))
    assert a < b
