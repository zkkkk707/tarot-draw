"""进程级内存保护：Windows Job Object 硬限制 + psutil 软阈值分级保护。

================================================================================
一、内存口径（必须区分）
--------------------------------------------------------------------------------
Windows 任务管理器 / psutil 中常见的几个指标含义不同：

- 工作集 Working Set（psutil: memory_info().rss / .wset）
  进程当前驻留在物理内存中的全部页面，包含可与其它进程共享的页面（如 DLL 映像）。
- 私有工作集 Private Working Set（任务管理器“进程”页默认“内存”列；psutil: USS 近似）
  工作集中只属于本进程的部分。采样代价高，本模块按较长间隔采样。
- 私有字节 / 提交大小 Commit Size（psutil: memory_info().private，Windows 下与 .vms 相同）
  本进程向系统“提交”的私有内存总量，包括已被换出到页面文件、当前不在物理内存中的部分。
- 虚拟大小 Virtual Size：仅为地址空间保留，不代表实际占用（如 V8 引擎会保留较大地址空间），
  本项目不以此为预算口径。

二、硬限制（Windows Job Object）
--------------------------------------------------------------------------------
程序启动后第一时间（在导入 pandas/numpy/akshare 之前）创建一个 Job Object，并把当前进程
加入其中。之后由本进程创建的所有子进程默认自动进入同一个 Job（未设置 BREAKAWAY 权限）。

- JOB_OBJECT_LIMIT_JOB_MEMORY     : 限制作业内全部进程“已提交私有内存”之和（默认 960 MiB）
- JOB_OBJECT_LIMIT_PROCESS_MEMORY : 限制单个进程“已提交私有内存”（默认 960 MiB）
- JOB_OBJECT_LIMIT_ACTIVE_PROCESS : 限制作业内同时存在的进程数
- JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE : 主进程退出（作业句柄关闭）时终止所有残留子进程
- JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION : 崩溃时不弹出 WER 对话框，直接结束

达到 Job 内存上限时，新的内存提交请求会被操作系统拒绝：Python 层通常表现为 MemoryError，
部分原生扩展可能直接异常终止。这是设计上“允许任务失败、不继续扩大占用”的预期行为。

附加：对主进程额外调用 SetProcessWorkingSetSizeEx 设置“硬性工作集上限”（默认 1000 MiB），
使任务管理器中“工作集”列也被系统强制约束（超出部分会被换出，而不是继续驻留）。该项设置失败
不会阻止启动，但会记录警告并写入 market_status.json。

三、技术边界（不做虚假承诺）
--------------------------------------------------------------------------------
1. Job 硬限制约束的是“提交的私有内存”。私有工作集 ≤ 私有提交，因此也被同一上限约束。
2. 工作集（含共享 DLL 页面）不在 Job 内存限制口径内；仅主进程有额外的硬性工作集上限，
   子进程的工作集只受软监测约束。
3. 内核为进程分配的分页/非分页池、页表、显存等不计入上述口径，任务管理器个别列可能
   因此略有出入。
4. 若使用 venv 或 py.exe 启动器，Windows 会先启动一个很小的“启动器进程”再启动真正的
   python.exe。启动器进程（通常仅数 MB）在 Job 之外，任务管理器中会显示为两个进程。
5. Job 限制随进程存在；无法约束本程序之外的其它程序。
6. 如果 Job 创建或加入失败，并且 memory.require_hard_limit=true（默认），程序拒绝启动；
   如果设为 false，程序以“仅软监测”模式运行，并在状态文件中明确标记 hard_limit_active=false。

四、软阈值分级（十进制 MB，口径为 max(私有提交合计, 工作集合计)，取二者较大值以保守判断）
--------------------------------------------------------------------------------
- NORMAL   (<750MB) : 正常运行
- WARNING  (>=750MB): 停止非必要数据加载（如分钟线、行业成分刷新、整批日K更新）
- PROTECT  (>=850MB): 释放已注册缓存、触发 GC、暂停大型计算（指标批量计算、全市场筛选）
- CRITICAL (>=950MB): 仅保留状态写出与安全退出；持续处于该状态则安全退出
"""

from __future__ import annotations

import gc
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Callable

from config import MB, MIB, MemoryConfig

logger = logging.getLogger(__name__)


class MemoryLevel(IntEnum):
    NORMAL = 0
    WARNING = 1
    PROTECT = 2
    CRITICAL = 3


class TaskKind(Enum):
    ESSENTIAL = "essential"          # 实时行情快照、状态文件写出
    HEAVY = "heavy"                  # 批量指标计算、全市场筛选
    NONESSENTIAL = "nonessential"    # 分钟线、行业成分刷新、历史回填


class MemoryBudgetExceeded(RuntimeError):
    """当前内存级别不允许执行该类任务。"""


class HardLimitUnavailable(RuntimeError):
    """无法建立 Windows Job Object 硬限制。"""


@dataclass(frozen=True)
class MemoryThresholds:
    warn_bytes: int
    protect_bytes: int
    critical_bytes: int
    hysteresis_bytes: int = 20 * MB

    @classmethod
    def from_config(cls, cfg: MemoryConfig) -> "MemoryThresholds":
        return cls(cfg.warn_mb * MB, cfg.protect_mb * MB, cfg.critical_mb * MB)

    def level_for(self, metric: int, previous: MemoryLevel) -> MemoryLevel:
        """带回滞的级别判断：上升立即生效，下降需低于阈值一个回滞量，避免反复抖动。"""
        raw = MemoryLevel.NORMAL
        if metric >= self.critical_bytes:
            raw = MemoryLevel.CRITICAL
        elif metric >= self.protect_bytes:
            raw = MemoryLevel.PROTECT
        elif metric >= self.warn_bytes:
            raw = MemoryLevel.WARNING
        if raw >= previous:
            return raw
        bounds = {
            MemoryLevel.WARNING: self.warn_bytes,
            MemoryLevel.PROTECT: self.protect_bytes,
            MemoryLevel.CRITICAL: self.critical_bytes,
        }
        level = previous
        while level > raw and metric < bounds[level] - self.hysteresis_bytes:
            level = MemoryLevel(level - 1)
        return level


@dataclass
class MemorySample:
    timestamp: float
    main_working_set: int
    main_private_commit: int
    children_count: int
    children_working_set: int
    children_private_commit: int
    main_private_working_set: int | None = None  # USS，按较长间隔采样
    platform_note: str = ""

    @property
    def total_working_set(self) -> int:
        return self.main_working_set + self.children_working_set

    @property
    def total_private_commit(self) -> int:
        return self.main_private_commit + self.children_private_commit

    @property
    def governing_bytes(self) -> int:
        """分级判断使用的保守口径。"""
        return max(self.total_working_set, self.total_private_commit)

    def as_dict(self) -> dict[str, Any]:
        def mb(v: int | None) -> float | None:
            return None if v is None else round(v / MB, 1)

        def mib(v: int | None) -> float | None:
            return None if v is None else round(v / MIB, 1)

        return {
            "main_working_set_mb": mb(self.main_working_set),
            "main_private_commit_mb": mb(self.main_private_commit),
            "main_private_working_set_mb": mb(self.main_private_working_set),
            "children_count": self.children_count,
            "children_working_set_mb": mb(self.children_working_set),
            "children_private_commit_mb": mb(self.children_private_commit),
            "total_working_set_mb": mb(self.total_working_set),
            "total_private_commit_mb": mb(self.total_private_commit),
            "total_private_commit_mib": mib(self.total_private_commit),
            "governing_mb": mb(self.governing_bytes),
            "platform_note": self.platform_note,
        }


@dataclass
class JobStatus:
    active: bool = False
    reason: str = "not_attempted"
    job_memory_limit_bytes: int = 0
    process_memory_limit_bytes: int = 0
    max_active_processes: int = 0
    was_in_job_before: bool | None = None
    working_set_hard_max_bytes: int = 0
    working_set_hard_max_active: bool = False
    working_set_reason: str = "not_attempted"
    peak_job_memory_used: int | None = None
    peak_process_memory_used: int | None = None
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "hard_limit_active": self.active,
            "reason": self.reason,
            "job_memory_limit_mib": round(self.job_memory_limit_bytes / MIB, 1),
            "process_memory_limit_mib": round(self.process_memory_limit_bytes / MIB, 1),
            "max_active_processes": self.max_active_processes,
            "was_in_job_before": self.was_in_job_before,
            "working_set_hard_max_mib": round(self.working_set_hard_max_bytes / MIB, 1),
            "working_set_hard_max_active": self.working_set_hard_max_active,
            "working_set_reason": self.working_set_reason,
            "peak_job_memory_used_mib": (None if self.peak_job_memory_used is None
                                         else round(self.peak_job_memory_used / MIB, 1)),
            "peak_process_memory_used_mib": (None if self.peak_process_memory_used is None
                                             else round(self.peak_process_memory_used / MIB, 1)),
            "warnings": list(self.warnings),
        }


# ---------------------------------------------------------------------------
# Windows Job Object（仅在 win32 上可用；其它平台调用会返回 active=False）
# ---------------------------------------------------------------------------

JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 0x00000008
JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100
JOB_OBJECT_LIMIT_JOB_MEMORY = 0x00000200
JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION = 0x00000400
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
QUOTA_LIMITS_HARDWS_MIN_DISABLE = 0x00000002
QUOTA_LIMITS_HARDWS_MAX_ENABLE = 0x00000004


class WindowsJobLimiter:
    """封装 Job Object 的创建、加入、查询与校验。句柄在进程生命周期内保持打开。"""

    def __init__(self, job_limit_bytes: int, process_limit_bytes: int,
                 max_active_processes: int, working_set_hard_max_bytes: int) -> None:
        self.job_limit_bytes = job_limit_bytes
        self.process_limit_bytes = process_limit_bytes
        self.max_active_processes = max_active_processes
        self.working_set_hard_max_bytes = working_set_hard_max_bytes
        self._handle: Any = None
        self._k32: Any = None
        self._structs: dict[str, Any] = {}
        self.status = JobStatus(
            job_memory_limit_bytes=job_limit_bytes,
            process_memory_limit_bytes=process_limit_bytes,
            max_active_processes=max_active_processes,
            working_set_hard_max_bytes=working_set_hard_max_bytes,
        )

    # -- ctypes 初始化 -------------------------------------------------------
    def _load(self) -> None:
        import ctypes
        from ctypes import wintypes

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        k32.CreateJobObjectW.restype = wintypes.HANDLE
        k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int,
                                                ctypes.c_void_p, wintypes.DWORD]
        k32.SetInformationJobObject.restype = wintypes.BOOL
        k32.QueryInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                                  wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
        k32.QueryInformationJobObject.restype = wintypes.BOOL
        k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        k32.AssignProcessToJobObject.restype = wintypes.BOOL
        k32.GetCurrentProcess.argtypes = []
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        k32.IsProcessInJob.argtypes = [wintypes.HANDLE, wintypes.HANDLE,
                                       ctypes.POINTER(wintypes.BOOL)]
        k32.IsProcessInJob.restype = wintypes.BOOL
        k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        k32.OpenProcess.restype = wintypes.HANDLE
        k32.CloseHandle.argtypes = [wintypes.HANDLE]
        k32.CloseHandle.restype = wintypes.BOOL
        k32.SetProcessWorkingSetSizeEx.argtypes = [wintypes.HANDLE, ctypes.c_size_t,
                                                   ctypes.c_size_t, wintypes.DWORD]
        k32.SetProcessWorkingSetSizeEx.restype = wintypes.BOOL

        self._k32 = k32
        self._structs = {
            "ctypes": ctypes,
            "wintypes": wintypes,
            "EXT": JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        }

    def _last_error(self) -> int:
        return int(self._structs["ctypes"].get_last_error())

    # -- 建立硬限制 -----------------------------------------------------------
    def apply(self) -> JobStatus:
        if sys.platform != "win32":
            self.status.active = False
            self.status.reason = f"unsupported_platform:{sys.platform}"
            return self.status
        try:
            self._load()
        except (OSError, AttributeError) as exc:
            self.status.reason = f"ctypes_load_failed:{exc}"
            return self.status

        ctypes = self._structs["ctypes"]
        wintypes = self._structs["wintypes"]
        k32 = self._k32
        current = k32.GetCurrentProcess()

        in_job = wintypes.BOOL(False)
        if k32.IsProcessInJob(current, None, ctypes.byref(in_job)):
            self.status.was_in_job_before = bool(in_job.value)
            if in_job.value:
                # Windows 8+ 支持嵌套 Job；外层 Job 若有更严格限制同样生效。
                self.status.warnings.append("process_already_in_job_nested_job_will_be_used")

        handle = k32.CreateJobObjectW(None, None)
        if not handle:
            self.status.reason = f"CreateJobObject_failed:winerr={self._last_error()}"
            return self.status

        info = self._structs["EXT"]()
        flags = (JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_PROCESS_MEMORY
                 | JOB_OBJECT_LIMIT_ACTIVE_PROCESS | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                 | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION)
        info.BasicLimitInformation.LimitFlags = flags
        info.BasicLimitInformation.ActiveProcessLimit = self.max_active_processes
        info.JobMemoryLimit = self.job_limit_bytes
        info.ProcessMemoryLimit = self.process_limit_bytes
        ok = k32.SetInformationJobObject(handle, JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
                                         ctypes.byref(info), ctypes.sizeof(info))
        if not ok:
            err = self._last_error()
            k32.CloseHandle(handle)
            self.status.reason = f"SetInformationJobObject_failed:winerr={err}"
            return self.status

        if not k32.AssignProcessToJobObject(handle, current):
            err = self._last_error()
            k32.CloseHandle(handle)
            self.status.reason = f"AssignProcessToJobObject_failed:winerr={err}"
            return self.status

        self._handle = handle  # 保持句柄打开直至进程结束
        if not self.verify_current():
            self.status.reason = "assigned_but_verification_failed"
            return self.status
        self.status.active = True
        self.status.reason = "ok"
        self._apply_working_set_cap()
        return self.status

    def _apply_working_set_cap(self) -> None:
        if self.working_set_hard_max_bytes <= 0:
            self.status.working_set_reason = "disabled_by_config"
            return
        k32 = self._k32
        minimum = 16 * MIB
        ok = k32.SetProcessWorkingSetSizeEx(
            k32.GetCurrentProcess(), minimum, self.working_set_hard_max_bytes,
            QUOTA_LIMITS_HARDWS_MIN_DISABLE | QUOTA_LIMITS_HARDWS_MAX_ENABLE)
        if ok:
            self.status.working_set_hard_max_active = True
            self.status.working_set_reason = "ok"
        else:
            self.status.working_set_reason = f"SetProcessWorkingSetSizeEx_failed:winerr={self._last_error()}"
            self.status.warnings.append("working_set_hard_cap_not_applied")

    # -- 校验与查询 -----------------------------------------------------------
    def verify_current(self) -> bool:
        if self._handle is None or self._k32 is None:
            return False
        wintypes = self._structs["wintypes"]
        ctypes = self._structs["ctypes"]
        result = wintypes.BOOL(False)
        ok = self._k32.IsProcessInJob(self._k32.GetCurrentProcess(), self._handle,
                                      ctypes.byref(result))
        return bool(ok and result.value)

    def is_pid_in_job(self, pid: int) -> bool | None:
        """检查某个子进程是否位于本 Job 内；无法判断时返回 None。"""
        if self._handle is None or self._k32 is None:
            return None
        wintypes = self._structs["wintypes"]
        ctypes = self._structs["ctypes"]
        h = self._k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h:
            return None
        try:
            result = wintypes.BOOL(False)
            if not self._k32.IsProcessInJob(h, self._handle, ctypes.byref(result)):
                return None
            return bool(result.value)
        finally:
            self._k32.CloseHandle(h)

    def query_peaks(self) -> None:
        if self._handle is None or self._k32 is None:
            return
        ctypes = self._structs["ctypes"]
        wintypes = self._structs["wintypes"]
        info = self._structs["EXT"]()
        ret_len = wintypes.DWORD(0)
        ok = self._k32.QueryInformationJobObject(
            self._handle, JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
            ctypes.byref(info), ctypes.sizeof(info), ctypes.byref(ret_len))
        if ok:
            self.status.peak_job_memory_used = int(info.PeakJobMemoryUsed)
            self.status.peak_process_memory_used = int(info.PeakProcessMemoryUsed)
        else:
            logger.warning("查询 Job 峰值内存失败 winerr=%s", self._last_error())


# ---------------------------------------------------------------------------
# 采样器
# ---------------------------------------------------------------------------

class PsutilSampler:
    """采样主进程与其全部子进程的内存。"""

    def __init__(self, private_ws_interval_sec: float) -> None:
        import psutil

        self._psutil = psutil
        self._proc = psutil.Process()
        self._private_ws_interval = private_ws_interval_sec
        self._last_private_ws_at = 0.0
        self._last_private_ws: int | None = None

    @staticmethod
    def _private_of(mi: Any) -> tuple[int, str]:
        private = getattr(mi, "private", None)  # Windows: PrivateUsage（提交的私有内存）
        if private is not None:
            return int(private), ""
        # 非 Windows 平台没有等价字段，退化为 RSS，仅用于开发环境
        return int(mi.rss), "non_windows_private_commit_approximated_by_rss"

    def children(self) -> list[Any]:
        try:
            return self._proc.children(recursive=True)
        except (self._psutil.NoSuchProcess, self._psutil.AccessDenied):
            return []

    def __call__(self) -> MemorySample:
        mi = self._proc.memory_info()
        main_private, note = self._private_of(mi)
        ws_c = priv_c = count = 0
        for child in self.children():
            try:
                cmi = child.memory_info()
            except (self._psutil.NoSuchProcess, self._psutil.AccessDenied):
                continue
            count += 1
            ws_c += int(cmi.rss)
            priv_c += self._private_of(cmi)[0]

        now = time.monotonic()
        if self._private_ws_interval > 0 and now - self._last_private_ws_at >= self._private_ws_interval:
            try:
                self._last_private_ws = int(self._proc.memory_full_info().uss)
            except (self._psutil.AccessDenied, AttributeError, OSError) as exc:
                logger.debug("私有工作集采样失败: %s", exc)
                self._last_private_ws = None
            self._last_private_ws_at = now

        return MemorySample(
            timestamp=time.time(),
            main_working_set=int(mi.rss),
            main_private_commit=main_private,
            children_count=count,
            children_working_set=ws_c,
            children_private_commit=priv_c,
            main_private_working_set=self._last_private_ws,
            platform_note=note,
        )


# ---------------------------------------------------------------------------
# 统一守护对象
# ---------------------------------------------------------------------------

class MemoryGuard:
    """内存守护：建立硬限制、周期采样、按级别放行任务、释放缓存、请求安全退出。"""

    def __init__(self, cfg: MemoryConfig,
                 sampler: Callable[[], MemorySample] | None = None,
                 limiter: WindowsJobLimiter | None = None) -> None:
        self.cfg = cfg
        self.thresholds = MemoryThresholds.from_config(cfg)
        self._sampler = sampler
        self.limiter = limiter or WindowsJobLimiter(
            cfg.job_memory_limit_mib * MIB,
            cfg.process_memory_limit_mib * MIB,
            cfg.max_active_processes,
            cfg.working_set_hard_max_mib * MIB,
        )
        self.level = MemoryLevel.NORMAL
        self.last_sample: MemorySample | None = None
        self._last_check_mono = 0.0
        self._caches: dict[str, Callable[[], None]] = {}
        self._lock = threading.Lock()
        self._consecutive_critical = 0
        self.shutdown_requested = False
        self.shutdown_reason = ""
        self.memory_errors = 0
        self.hard_limit_lost = False

    # -- 硬限制 ---------------------------------------------------------------
    def install_hard_limit(self) -> JobStatus:
        status = self.limiter.apply()
        if status.active:
            logger.info("Windows Job Object 硬限制已生效: 作业提交内存上限 %.0f MiB, 单进程 %.0f MiB, 最大进程数 %d",
                        status.job_memory_limit_bytes / MIB, status.process_memory_limit_bytes / MIB,
                        status.max_active_processes)
            if not status.working_set_hard_max_active:
                logger.warning("主进程硬性工作集上限未生效: %s", status.working_set_reason)
        elif self.cfg.require_hard_limit:
            raise HardLimitUnavailable(
                f"无法建立 Windows Job Object 内存硬限制（{status.reason}）。"
                "为保证内存预算，程序拒绝启动。若确需仅以软监测模式运行，"
                "请在 settings.yaml 中设置 memory.require_hard_limit: false。")
        else:
            logger.warning("未建立内存硬限制（%s），当前仅为 psutil 软监测，不能保证内存上限。", status.reason)
        return status

    # -- 缓存注册 -------------------------------------------------------------
    def register_cache(self, name: str, release: Callable[[], None]) -> None:
        with self._lock:
            self._caches[name] = release

    def release_caches(self, reason: str) -> None:
        with self._lock:
            items = list(self._caches.items())
        for name, release in items:
            try:
                release()
            except Exception:  # 释放缓存失败不应中断保护流程，但必须记录
                logger.exception("释放缓存 %s 失败", name)
        collected = gc.collect()
        logger.warning("已释放 %d 个缓存并执行 GC（回收对象 %d），原因: %s", len(items), collected, reason)

    # -- 采样与分级 -----------------------------------------------------------
    def _get_sampler(self) -> Callable[[], MemorySample]:
        if self._sampler is None:
            self._sampler = PsutilSampler(self.cfg.private_ws_sample_interval_sec)
        return self._sampler

    def check(self, force: bool = False) -> MemoryLevel:
        now = time.monotonic()
        if not force and self.last_sample is not None and now - self._last_check_mono < self.cfg.check_interval_sec:
            return self.level
        self._last_check_mono = now
        try:
            sample = self._get_sampler()()
        except Exception:
            # 监测本身失效属于严重问题：保守地视为 CRITICAL，由调度器决定退出
            logger.exception("内存采样失败，按 CRITICAL 处理")
            self._enter(MemoryLevel.CRITICAL, "sampler_failure")
            return self.level
        self.last_sample = sample
        new_level = self.thresholds.level_for(sample.governing_bytes, self.level)
        self._enter(new_level, f"governing={sample.governing_bytes / MB:.0f}MB")
        self._verify_hard_limit()
        return self.level

    def _enter(self, new_level: MemoryLevel, detail: str) -> None:
        old = self.level
        self.level = new_level
        if new_level != old:
            log = logger.warning if new_level > old else logger.info
            log("内存级别 %s -> %s (%s)", old.name, new_level.name, detail)
        if new_level >= MemoryLevel.PROTECT and new_level > old:
            self.release_caches(f"enter_{new_level.name}")
        if new_level == MemoryLevel.CRITICAL:
            self._consecutive_critical += 1
            if self._consecutive_critical >= self.cfg.critical_exit_consecutive:
                self.request_shutdown(f"内存持续处于 CRITICAL（{self._consecutive_critical} 次检查）")
        else:
            self._consecutive_critical = 0

    def _verify_hard_limit(self) -> None:
        """周期性确认硬限制仍然有效，并检查子进程是否都在 Job 内。"""
        if not self.limiter.status.active:
            return
        if not self.limiter.verify_current():
            self.hard_limit_lost = True
            logger.critical("检测到主进程已不在 Job Object 中，内存硬限制失效")
            if self.cfg.require_hard_limit:
                self.request_shutdown("内存硬限制失效")
            return
        sampler = self._sampler
        if isinstance(sampler, PsutilSampler):
            for child in sampler.children():
                in_job = self.limiter.is_pid_in_job(child.pid)
                if in_job is False:
                    logger.critical("子进程 pid=%s 不在 Job 内，终止该进程以维持内存预算", child.pid)
                    try:
                        child.terminate()
                    except Exception:
                        logger.exception("终止越界子进程失败 pid=%s", child.pid)

    # -- 任务放行 -------------------------------------------------------------
    def allow(self, kind: TaskKind) -> bool:
        level = self.check()
        if self.shutdown_requested:
            return False
        if kind is TaskKind.ESSENTIAL:
            return level < MemoryLevel.CRITICAL
        if kind is TaskKind.HEAVY:
            return level < MemoryLevel.PROTECT
        return level < MemoryLevel.WARNING

    def checkpoint(self, kind: TaskKind, what: str) -> None:
        """批处理循环中的检查点：不允许继续时抛出异常，由调用方中止本批次。"""
        self.check(force=True)
        if not self.allow(kind):
            raise MemoryBudgetExceeded(f"内存级别 {self.level.name} 不允许继续执行 {what}（{kind.value}）")

    def on_memory_error(self, where: str) -> None:
        """任务中捕获到 MemoryError 后调用：释放缓存，计数，必要时请求退出。"""
        self.memory_errors += 1
        logger.error("任务 %s 发生 MemoryError（累计 %d 次），已中止该任务", where, self.memory_errors)
        self.release_caches(f"memory_error:{where}")
        self.check(force=True)
        if self.memory_errors >= 3:
            self.request_shutdown("多次发生 MemoryError")

    def request_shutdown(self, reason: str) -> None:
        if not self.shutdown_requested:
            logger.critical("内存守护请求安全退出: %s", reason)
        self.shutdown_requested = True
        self.shutdown_reason = reason

    def status_dict(self) -> dict[str, Any]:
        self.limiter.query_peaks()
        return {
            "level": self.level.name,
            "thresholds_mb": {
                "warn": self.cfg.warn_mb,
                "protect": self.cfg.protect_mb,
                "critical": self.cfg.critical_mb,
            },
            "governing_metric": "max(total_private_commit, total_working_set)",
            "sample": self.last_sample.as_dict() if self.last_sample else None,
            "job": self.limiter.status.as_dict(),
            "hard_limit_lost": self.hard_limit_lost,
            "memory_errors": self.memory_errors,
            "shutdown_requested": self.shutdown_requested,
            "shutdown_reason": self.shutdown_reason,
        }


def limit_numeric_threads(threads: int) -> None:
    """在导入 numpy/pandas 之前限制数值计算库线程数（只修改本进程环境变量）。

    多线程 BLAS 会为每个线程分配缓冲区，限制线程数可显著降低峰值内存。
    这些变量仅存在于当前进程及其子进程中，不修改系统或用户级环境变量。
    """
    import os

    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                 "NUMEXPR_MAX_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ[name] = str(threads)
