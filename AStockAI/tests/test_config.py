from __future__ import annotations

import pytest

from config import MB, MIB, ConfigError, settings_from_dict


def test_defaults_are_valid_and_under_one_gib() -> None:
    s = settings_from_dict({})
    m = s.memory
    assert m.warn_mb == 750 and m.protect_mb == 850
    assert m.critical_mb * MB < 1024 * MIB
    assert m.job_memory_limit_mib < 1024
    assert m.working_set_hard_max_mib < 1024
    assert s.screener.weight_sector + s.screener.weight_volume_price + s.screener.weight_market \
        + s.screener.weight_catalyst + s.screener.weight_risk_reward == 100


def test_unknown_key_is_rejected() -> None:
    with pytest.raises(ConfigError):
        settings_from_dict({"memory": {"warn_mbb": 700}})
    with pytest.raises(ConfigError):
        settings_from_dict({"unknown_section": {}})


def test_threshold_order_enforced() -> None:
    with pytest.raises(ConfigError):
        settings_from_dict({"memory": {"warn_mb": 900, "protect_mb": 850}})


def test_job_limit_must_be_below_1024_mib() -> None:
    with pytest.raises(ConfigError):
        settings_from_dict({"memory": {"job_memory_limit_mib": 1024}})


def test_weights_must_sum_to_100() -> None:
    with pytest.raises(ConfigError):
        settings_from_dict({"screener": {"weight_sector": 30.0}})


def test_watchlist_numeric_codes_are_padded() -> None:
    s = settings_from_dict({"watchlist": {"codes": [1, "600519"]}})
    assert s.watchlist.codes == ["000001", "600519"]


def test_collection_interval_lower_bound() -> None:
    with pytest.raises(ConfigError):
        settings_from_dict({"schedule": {"spot_interval_sec": 5.0}})


def test_cleanup_cannot_start_before_1530() -> None:
    with pytest.raises(ConfigError):
        settings_from_dict({"schedule": {"cleanup_start": "15:00"}})


def test_bool_type_checked() -> None:
    with pytest.raises(ConfigError):
        settings_from_dict({"memory": {"require_hard_limit": "yes"}})
