from __future__ import annotations

import sys

import pytest

from config import MB, MemoryConfig
from memory_guard import (
    HardLimitUnavailable,
    MemoryBudgetExceeded,
    MemoryGuard,
    MemoryLevel,
    MemorySample,
    MemoryThresholds,
    TaskKind,
    WindowsJobLimiter,
)


class SeqSampler:
    """按顺序返回预设的内存值（十进制 MB），用于模拟内存变化。"""

    def __init__(self, values_mb: list[float]) -> None:
        self.values = list(values_mb)
        self.last = self.values[-1]

    def __call__(self) -> MemorySample:
        v = self.values.pop(0) if self.values else self.last
        b = int(v * MB)
        return MemorySample(timestamp=0.0, main_working_set=b, main_private_commit=b,
                            children_count=0, children_working_set=0, children_private_commit=0)


def _guard(values: list[float], **cfg: object) -> MemoryGuard:
    c = MemoryConfig(require_hard_limit=False, check_interval_sec=0.0, critical_exit_consecutive=3)
    for k, v in cfg.items():
        setattr(c, k, v)
    return MemoryGuard(c, sampler=SeqSampler(values))


def test_levels_and_hysteresis() -> None:
    t = MemoryThresholds.from_config(MemoryConfig())
    assert t.level_for(int(700 * MB), MemoryLevel.NORMAL) is MemoryLevel.NORMAL
    assert t.level_for(int(760 * MB), MemoryLevel.NORMAL) is MemoryLevel.WARNING
    assert t.level_for(int(860 * MB), MemoryLevel.NORMAL) is MemoryLevel.PROTECT
    assert t.level_for(int(960 * MB), MemoryLevel.NORMAL) is MemoryLevel.CRITICAL
    # 回落到阈值附近（未低于阈值 - 回滞量）时保持原级别
    assert t.level_for(int(740 * MB), MemoryLevel.WARNING) is MemoryLevel.WARNING
    assert t.level_for(int(720 * MB), MemoryLevel.WARNING) is MemoryLevel.NORMAL
    assert t.level_for(int(600 * MB), MemoryLevel.CRITICAL) is MemoryLevel.NORMAL


def test_governing_metric_uses_larger_of_commit_and_working_set() -> None:
    s = MemorySample(0.0, main_working_set=int(500 * MB), main_private_commit=int(300 * MB),
                     children_count=1, children_working_set=int(100 * MB),
                     children_private_commit=int(400 * MB))
    assert s.governing_bytes == int(700 * MB)


def test_allow_rules_by_task_kind() -> None:
    g = _guard([760])  # WARNING
    assert g.allow(TaskKind.ESSENTIAL)
    assert g.allow(TaskKind.HEAVY)
    assert not g.allow(TaskKind.NONESSENTIAL)
    g = _guard([860])  # PROTECT
    assert g.allow(TaskKind.ESSENTIAL)
    assert not g.allow(TaskKind.HEAVY)
    g = _guard([960])  # CRITICAL
    assert not g.allow(TaskKind.ESSENTIAL)


def test_protect_releases_registered_caches() -> None:
    g = _guard([500, 860])
    released: list[str] = []
    g.register_cache("x", lambda: released.append("x"))
    g.check(force=True)
    assert released == []
    g.check(force=True)
    assert released == ["x"]


def test_checkpoint_raises_when_budget_exceeded() -> None:
    g = _guard([860])
    with pytest.raises(MemoryBudgetExceeded):
        g.checkpoint(TaskKind.HEAVY, "unit-test")


def test_persistent_critical_requests_shutdown() -> None:
    g = _guard([960, 960, 960])
    for _ in range(3):
        g.check(force=True)
    assert g.shutdown_requested


def test_repeated_memory_errors_request_shutdown() -> None:
    g = _guard([500])
    for _ in range(3):
        g.on_memory_error("job")
    assert g.shutdown_requested


def test_sampler_failure_is_treated_as_critical() -> None:
    def broken() -> MemorySample:
        raise RuntimeError("psutil failed")

    g = MemoryGuard(MemoryConfig(require_hard_limit=False, check_interval_sec=0.0), sampler=broken)
    assert g.check(force=True) is MemoryLevel.CRITICAL


@pytest.mark.skipif(sys.platform == "win32", reason="非 Windows 平台上验证：无法建立硬限制时的拒绝启动逻辑")
def test_hard_limit_required_but_unavailable_refuses_start() -> None:
    g = MemoryGuard(MemoryConfig(require_hard_limit=True), sampler=SeqSampler([100]))
    with pytest.raises(HardLimitUnavailable):
        g.install_hard_limit()


@pytest.mark.skipif(sys.platform == "win32", reason="非 Windows 平台行为")
def test_limiter_reports_unsupported_platform() -> None:
    status = WindowsJobLimiter(960, 960, 4, 1000).apply()
    assert status.active is False
    assert status.reason.startswith("unsupported_platform")


@pytest.mark.skipif(sys.platform != "win32", reason="仅在 Windows 上验证 Job Object（需由用户在本机自行运行）")
def test_windows_job_object_applies_in_subprocess() -> None:
    """在子进程中建立 Job 限制并读取状态，避免影响运行 pytest 的进程本身。"""
    import subprocess
    import textwrap
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "src"
    code = textwrap.dedent(f"""
        import sys; sys.path.insert(0, r"{src}")
        from memory_guard import WindowsJobLimiter
        st = WindowsJobLimiter(512 * 1048576, 512 * 1048576, 2, 600 * 1048576).apply()
        print("ACTIVE" if st.active else "INACTIVE:" + st.reason)
    """)
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.stdout.strip() == "ACTIVE", out.stdout + out.stderr
