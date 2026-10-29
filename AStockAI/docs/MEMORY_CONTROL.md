# 内存控制机制说明

目标：正常运行 400–600 MB；预警 750 MB；保护 850 MB；最大预算 < 1024 MiB。
本文严格区分 MB（10⁶ 字节）与 MiB（2²⁰ 字节）：1024 MiB ≈ 1073.7 MB，960 MiB ≈ 1006.6 MB。

## 1. 内存口径

| 口径 | 含义 | 在哪里看 | 本项目如何约束 |
|---|---|---|---|
| 提交大小 / 私有字节（Commit / Private Bytes） | 进程向系统提交的私有内存，含已换出到页面文件的部分 | 任务管理器“详细信息”→ 添加“提交大小”列；psutil `memory_info().private` | **Job Object 硬限制**：作业合计 ≤ 960 MiB，单进程 ≤ 960 MiB |
| 专用工作集（Private Working Set） | 驻留物理内存、且只属于本进程的页面 | 任务管理器“进程”页默认的“内存”列、“详细信息”页“内存(专用工作集)” | 它是提交内存的子集，因此同样 ≤ 960 MiB（硬性） |
| 工作集（Working Set） | 驻留物理内存的全部页面，含可共享的 DLL 映像页 | 任务管理器“工作集(内存)”列；psutil `rss` | 主进程：`SetProcessWorkingSetSizeEx` 硬性上限 1000 MiB；子进程：仅软监测 |
| 虚拟大小（Virtual Size） | 地址空间保留量，不代表实际占用 | Process Explorer 等 | 不作为预算口径 |

软阈值判断使用的“治理口径” = `max(受控进程私有提交合计, 受控进程工作集合计)`，两者取大，偏保守。

## 2. 硬限制：Windows Job Object

实现位置：`src/memory_guard.py` → `WindowsJobLimiter`。

1. `main.py` 读取配置后、导入 pandas/numpy/akshare **之前**，调用 `CreateJobObjectW` 创建作业对象。
2. `SetInformationJobObject(JobObjectExtendedLimitInformation)` 设置：
   - `JOB_OBJECT_LIMIT_JOB_MEMORY`：作业内所有进程提交内存合计 ≤ `job_memory_limit_mib`（默认 960）
   - `JOB_OBJECT_LIMIT_PROCESS_MEMORY`：单进程 ≤ `process_memory_limit_mib`（默认 960）
   - `JOB_OBJECT_LIMIT_ACTIVE_PROCESS`：作业内最多 `max_active_processes` 个进程（默认 4）
   - `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`：主进程退出时结束所有残留子进程
   - `JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION`：崩溃时不弹出错误报告对话框
3. `AssignProcessToJobObject` 把当前进程加入作业；随后用 `IsProcessInJob` 验证确实已在作业内。
4. 之后由本进程创建的任何子进程**自动进入同一作业**（未授予 breakaway 权限），受同一合计上限约束。
5. 额外调用 `SetProcessWorkingSetSizeEx(..., QUOTA_LIMITS_HARDWS_MAX_ENABLE)` 为主进程设置硬性工作集上限（默认 1000 MiB）；设置失败只记录警告，不影响启动。
6. 运行期间每次内存检查都会重新确认主进程仍在作业中，并逐个检查子进程是否在作业内；发现越界子进程会终止它。若主进程不在作业中（硬限制失效）且 `require_hard_limit=true`，程序请求安全退出。
7. 作业句柄在进程生命周期内保持打开；`market_status.json` 中可以看到 `hard_limit_active`、各项上限，以及 `QueryInformationJobObject` 读出的 `PeakJobMemoryUsed` / `PeakProcessMemoryUsed`。

**达到上限时会发生什么**：系统拒绝新的内存提交。Python 层通常抛出 `MemoryError`，调度器捕获后中止当前任务、释放缓存、执行 GC；同一会话累计 3 次 `MemoryError` 则安全退出。部分原生扩展（numpy、V8 等）在分配失败时也可能直接终止进程——这符合“允许任务失败、不继续扩大内存占用”的设计目标，数据库（WAL）与导出文件（原子替换）不会因此损坏。

**建立失败时**：`require_hard_limit: true`（默认）→ 程序以退出码 3 拒绝启动并说明原因；只有在你明确改为 `false` 时才以“仅软监测”模式运行，此时 `market_status.json` 中 `hard_limit_active=false`，**不能**视为有硬限制。

## 3. 软阈值分级保护

| 级别 | 触发（治理口径） | 行为 |
|---|---|---|
| NORMAL | < 750 MB | 正常运行 |
| WARNING | ≥ 750 MB | 停止非必要数据加载（行业成分刷新等 `NONESSENTIAL` 任务） |
| PROTECT | ≥ 850 MB | 释放已注册缓存（指标缓存、行业成分映射）并 GC；暂停大型计算（日K批量更新、指标计算、全市场筛选、清理等 `HEAVY` 任务） |
| CRITICAL | ≥ 950 MB | 只允许状态文件写出等最小任务；连续 6 次检查（约 30 秒）仍为 CRITICAL → 安全退出（退出码 5） |

- 级别下降需要低于阈值 20 MB（回滞），避免来回抖动。
- 批处理循环中每处理一只股票/一个板块都会调用 `checkpoint()`，内存超限时立即中止本批次（已完成的部分已提交，下次续传）。
- 内存采样本身失败时按 CRITICAL 处理（监测失效不能被当作正常）。

## 4. 降低内存占用的结构性措施

- 单线程调度器：同一时刻只运行一个任务，不存在多任务内存叠加。
- 不一次性加载全A历史：日K更新每批 40 只股票、每只只请求增量区间；指标计算每批 200 只、每只只读取最近 220 个自然日。
- 全市场数据只在内存中保留“最新一份快照”（约 5500 行，几 MB）。
- 所有缓存与历史队列都有上限（错误记录 50 条、市场宽度历史 400 点等），可释放缓存在 PROTECT 时清空。
- SQLite：页缓存上限 16 MiB（`cache_size`），`mmap_size=0`，`temp_store=FILE`。
- numpy/BLAS/OpenMP 线程数限制为 1（只设置本进程环境变量）。
- 单实例锁，防止重复启动导致内存翻倍。
- 不运行任何本地 AI 模型。

## 5. 技术边界（不做虚假承诺）

1. Job Object 约束的是**提交的私有内存**。任务管理器默认“内存（专用工作集）”列与“提交大小”列由硬限制保证 < 1 GiB；“工作集”列包含共享 DLL 页面，只有主进程有硬性上限（1000 MiB），子进程仅受软监测。
2. 内核为进程分配的分页/非分页池、页表、句柄表、显存等不在上述任何口径内，任务管理器的部分列可能因此与本文数字略有出入。
3. 使用 venv 时，`.venv\Scripts\python.exe` 是一个很小的启动器，会再启动真正的 python.exe。启动器进程（通常几 MB）在作业之外，任务管理器里会看到两个 python 进程；作业约束的是真正运行代码的那个进程。
4. 作业只能约束本程序及其子进程，无法约束同时运行的其它程序（包括你用来与 Claude 对话的浏览器/客户端）。
5. 硬限制是“上限”而不是“目标”：它保证不会超过，但不保证任务一定能在该上限内完成——超出时任务会失败。
6. 以上机制已按 Windows SDK 的结构体定义编写，但**尚未在 Windows 上实际运行验证**（见 README 第 13 节）。`tests/test_memory_guard.py` 中包含一个仅在 Windows 上运行的子进程测试，可在你本机验证 Job Object 是否生效。
