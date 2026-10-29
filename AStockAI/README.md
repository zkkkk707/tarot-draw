# AStockAI V1.0 — A股真实行情采集与量化数据系统

运行于 Windows 11 的轻量化本地数据系统。**只负责**：真实行情采集、数据校验、技术指标计算、股票筛选、异动检测、本地数据管理，并把结果整理成规范化文件，供你日后**主动**让 Claude 读取、讨论。

**不包含**：AI 分析、Claude API 调用、本地大模型、自动推荐/总结/日报、通知弹窗、真实交易、云同步、开机自启、计划任务、Windows 服务。

---

## 0. 交付状态（请先阅读）

| 项目 | 状态 |
|---|---|
| 源代码、配置、依赖清单、脚本、文档、单元测试代码 | 已编写完成 |
| 静态检查 | 已用 ruff 做过语法/未定义名称/常见错误模式检查，并人工逐模块复查逻辑 |
| 安装依赖、运行程序、连接 AKShare、执行单元测试 | **均未进行**（按任务要求禁止执行） |
| 是否声称“测试通过” | **否**。单元测试代码已写好，但从未运行过 |
| 文件位置 | GitHub 仓库 `zkkkk707/tarot-draw` 分支 `claude/astockquant-v1-dev-l7uk9o` 下的 `AStockAI/` 目录。**没有写入你电脑的 E 盘** |

第 13 节列出了所有尚未经过运行验证的部分。

---

## 1. 目录结构

```
E:\AStockAI                     ← 建议放置位置（也可放在任意目录，见 settings.yaml 的 paths.root）
├── src
│   ├── main.py                 入口：启动顺序、单实例锁、信号处理、任务注册、安全退出
│   ├── config.py               集中配置：dataclass + YAML 合并 + 严格校验（含单位约定）
│   ├── timeutil.py             时区工具：库内统一存 UTC，对外统一显示北京时间
│   ├── memory_guard.py         Windows Job Object 内存硬限制 + 软阈值分级保护
│   ├── market_data.py          数据源适配层（AKShare）：限流、超时、有限重试、熔断、字段校验
│   ├── trading_calendar.py     A股交易日历与交易时段判断
│   ├── collector.py            采集编排：快照/指数/板块/分钟线/日K/行业成分，数据状态判定
│   ├── indicators.py           技术指标（明确公式、无未来函数）+ 分批计算服务
│   ├── screener.py             评分筛选（缺失即标记缺失，不补分）
│   ├── anomaly_detector.py     异动检测（只记录客观事件）
│   ├── scheduler.py            单线程调度器（分批让出、退避重试、按交易时段放行）
│   ├── cleanup.py              自动清理与容量控制
│   ├── database.py             SQLite 表结构、参数化写入、容量与写入暂停
│   ├── work_data.py            work_data 原子导出 + manifest 校验清单
│   └── state.py                运行期共享状态（全部有界）
├── data\market.db              SQLite 数据库（首次运行时创建）
├── work_data\                  供 Claude 读取的规范化文件（运行后生成）
│   ├── market_latest.json  stocks_latest.csv  candidates.csv  indicators.csv
│   ├── alerts.json  market_status.json  manifest.json
├── logs\                       程序日志（按天滚动，保留 7 天）
├── run\                        单实例锁文件；放入 STOP 文件可让程序正常退出
├── config\settings.yaml        全部可调参数
├── tests\                      单元测试代码（未执行）
├── docs\DATA_FIELDS.md         数据字段说明
├── docs\MEMORY_CONTROL.md      内存控制机制说明
├── docs\KNOWN_LIMITATIONS.md   已知技术限制
├── requirements.txt            运行依赖
├── requirements-dev.txt        测试依赖（pytest）
├── pytest.ini
├── start.bat                   手动启动脚本
└── stop.bat                    正常退出脚本（创建 run\STOP 标记）
```

> 源码里额外增加了 `config.py / timeutil.py / trading_calendar.py / collector.py / state.py / work_data.py` 六个模块，是对建议结构的拆分细化，便于审查和测试。

---

## 2. 安装步骤（未来由你手动执行，本次未执行）

所有依赖都需要你自己安装，程序和脚本都不会自动安装任何东西。

1. 安装 **Python 3.11 或更新版本**（python.org 官方安装包，安装时勾选 “py launcher”）。
2. 把 `AStockAI` 文件夹复制到 `E:\AStockAI`（或其它位置）。
3. 打开“命令提示符”，执行：
   ```bat
   cd /d E:\AStockAI
   py -3.11 -m venv .venv
   .venv\Scripts\python.exe -m pip install -r requirements.txt
   ```
4. （建议）固定实际安装的版本：`.venv\Scripts\python.exe -m pip freeze > requirements.lock.txt`
5. 编辑 `config\settings.yaml`，例如在 `watchlist.codes` 中填写重点观察股票（6位代码必须加引号，如 `"600519"`）。
6. 只校验配置、不启动采集：
   ```bat
   .venv\Scripts\python.exe src\main.py --check-config
   ```
7. （可选）运行单元测试（不联网，只用本地构造的小样本）：
   ```bat
   .venv\Scripts\python.exe -m pip install -r requirements-dev.txt
   .venv\Scripts\python.exe -m pytest
   ```

---

## 3. 启动与退出

- **启动**：双击 `start.bat`（或在命令行运行它）。脚本只在当前窗口内设置 `PYTHONUTF8` 等变量，不修改系统环境变量。
- **正常退出**（任选其一）：
  - 在程序窗口按 `Ctrl+C`；
  - 双击 `stop.bat`（创建 `run\STOP` 标记，程序在当前批次结束后退出）；
- 直接关闭窗口属于非正常退出：数据库使用 WAL 日志、导出文件使用原子替换，不会损坏，但正在进行的批次会中断，下次启动会自动续传。
- 不会开机自启，不创建计划任务，不注册服务，不在后台常驻；窗口关闭即程序结束。
- 重复启动会被单实例锁拒绝（避免内存翻倍）。

退出码：`0` 正常；`1` 未预期错误（见日志）；`2` 配置错误；`3` 无法建立内存硬限制；`4` 已有实例运行；`5` 内存保护触发的安全退出。

---

## 4. 运行时做什么（北京时间）

| 时段 | 任务 | 间隔 |
|---|---|---|
| 全天 | 交易日历刷新、work_data 状态文件与 manifest 刷新、数据库容量检查 | 1 小时 / 60 秒 / 10 分钟 |
| 09:25–09:30 | 集合竞价结果快照（一次） | 每日一次 |
| 09:30–11:30、13:00–15:00 | 全A快照、指数快照、行业板块快照 | 60 / 60 / 120 秒 |
| 同上 | 重点观察股票 1 分钟K线（带行情时间戳） | 30 秒 |
| 同上 | 异动检测（每份新快照一次）、盘中筛选 | 随快照 / 15 分钟 |
| 15:01 之后 | 收盘快照 + 当日市场统计（长期保存） | 每日一次 |
| 15:30–23:30 | 自动清理（过期数据、日志、空间回收） | 每日一次 |
| 15:35 之后、午间休市、开盘前、非交易日 | 指数日K、全A日K增量更新（分批、可续传）、技术指标计算、收盘筛选、行业成分刷新 | 每日一次（分批进行） |

- 交易日来自 AKShare 新浪交易日历（含法定节假日休市安排），缓存在数据库；获取失败时临时按工作日判断，并在状态文件中标记 `fallback_weekday`。
- 日K更新被拆成小批次，批与批之间调度器会处理其它任务；若在午间休市未完成，13:00 开盘后自动暂停，收盘后继续。
- 首次运行需要回填约 1 年的全A日K（约 5000 次接口请求，按 0.6 秒间隔限流，预计 1 小时以上），只在非交易时段进行。

---

## 5. 数据真实性设计

- 所有行情数据都来自 AKShare 调用的真实行情源；**不生成、不插值、不填补**任何价格或成交量。数据源缺失的字段写为空（CSV 空单元格 / JSON `null` / SQLite `NULL`）。
- 每条记录都带 `source`（数据源与具体接口）、`fetched_at`（获取时间）、`time_basis`（时间依据）、`data_status`（数据状态）。
- `data_status` 取值：`realtime` / `delayed` / `historical` / `unknown_time` / `stale` / `invalid`，含义见 `docs/DATA_FIELDS.md`。
  - 东方财富全A快照**不提供逐笔行情时间戳**，因此标记为 `unknown_time`，绝不冒充 `realtime`。
  - 自选股分钟K线带K线时间，可以判断 `realtime` 或 `delayed`，并用于交叉核对快照价格（`spot_crosscheck`）。
  - 交易时段内快照内容 180 秒完全不变 → `stale`；行数不足等校验失败 → `invalid`（不入库、不替换旧数据）。
- 网络异常、接口变化、限流：有限次重试（默认最多 3 次尝试）→ 失败记录到 `collection_runs` 表与 `market_status.json`；连续失败触发熔断冷却。**旧数据保留原获取时间，不会被当作新数据**；导出文件超过有效期后在 manifest 中显示 `expired`。
- 时间统一存储为带时区的时间戳（库内 UTC），对外文件统一显示北京时间（`+08:00`）。

---

## 6. 让 Claude 读取 work_data（你主动发起）

本系统**不会**向 Claude 或任何外部服务发送数据，也不上传云端。Claude 默认**没有**读取你本地 E 盘的权限。可行方式：

1. 在 Claude 网页/桌面应用的对话或 Projects 中，**手动上传** `work_data` 里需要的文件（建议同时上传 `manifest.json`）。
2. 若你自己使用了具备本地文件访问能力、并由你明确授权的客户端或工具，可以让它读取 `E:\AStockAI\work_data`。本项目没有、也不会擅自添加这类连接。

建议的读取顺序与提示：

> 请先读取 manifest.json，确认 writer.heartbeat_expires_at 和各文件的 expires_at 是否早于当前北京时间、sha256 是否一致；再读取 market_status.json 了解数据源是否异常；然后结合 market_latest.json、candidates.csv、indicators.csv、alerts.json 与我讨论。注意 data_status 为 unknown_time / stale 的含义。

---

## 7. 内存控制（摘要）

详见 `docs/MEMORY_CONTROL.md`。

- 启动后第一时间建立 **Windows Job Object 硬限制**：作业内所有进程的“已提交私有内存”合计 ≤ 960 MiB（单进程同样 ≤ 960 MiB），并对主进程设置 1000 MiB 的**硬性工作集上限**；之后才导入 pandas/akshare。
- 建立失败且 `require_hard_limit: true`（默认）→ **拒绝启动**，不会只靠 psutil 监测就声称有硬限制。
- 软阈值（十进制 MB，取私有提交与工作集的较大值）：750 停止非必要加载；850 释放缓存并暂停大型计算；950 只保留必要任务，持续则安全退出。
- 达到硬限制时，新的内存申请被系统拒绝，任务以 `MemoryError` 失败而不是继续扩大；多次发生则安全退出。
- 单线程调度、分批处理（日K 40 只/批、指标 200 只/批）、SQLite 页缓存 16 MiB、禁用 mmap、数值库单线程、所有缓存有界。
- **技术边界**：硬限制约束的是“提交的私有内存”（任务管理器“提交大小”，以及默认“内存(专用工作集)”列都不会超过它）；包含共享 DLL 页面的“工作集”只有主进程有硬上限；内核池、页表等不在约束口径内；venv 启动器进程（几 MB）在作业之外。不承诺任务管理器每一列数字的绝对上限。

---

## 8. 数据库与自动清理

| 数据 | 保留 |
|---|---|
| 实时行情快照（全A/指数/板块/市场宽度） | 3 天 |
| 分钟K线 | 7 天 |
| 日K线、指数日K、技术指标 | 1 年 |
| 异动记录 | 30 天 |
| 筛选结果 | 90 天 |
| 程序日志 | 7 天 |
| 每日市场统计、清理记录、交易日历、股票列表 | 长期保存 |

- 清理只在北京时间 15:30 之后执行；交易时段拒绝常规清理。
- 清理前 `PRAGMA quick_check`，异常则中止且不删除任何数据；全部参数化 SQL、分批删除；结果写入 `cleanup_runs`。
- 空间回收：WAL checkpoint → incremental_vacuum →（空闲页过多且磁盘空间足够时）全量 VACUUM。
- 容量目标 2 GB：超过 1.8 GB 先紧急清理高频数据；仍 ≥ 2 GB 则**暂停行情写入**（不会删除日K、统计等重要数据），内存中的最新数据和 work_data 导出仍继续。
- 全A快照在内存中每 60 秒更新，但每 5 分钟才完整入库一次（自选股每次都入库），以控制数据库体积。

---

## 9. 筛选与异动

- 评分框架（板块 25 / 量价 25 / 市场 20 / 催化与基本面 15 / 风险收益 15）是**初始研究假设**，权重和阈值都在 `settings.yaml` 中可调。
- V1.0 没有可靠的新闻/公告/财务数据源，“催化与基本面”一律标记为缺失（`catalyst_status = missing:no_reliable_news_or_fundamental_source`），**不补分**；其它子项缺数据同样不计分、也不计入可得满分，排名使用 `score_normalized = 得分 / 可得满分 × 100`。
- 结果只保存结构化字段（分项得分、可得满分、`reason_codes`、`missing_components`），不生成推荐文字。
- 异动：放量突破、缩量回踩、板块共振、跌破关键支撑、冲高回落、市场宽度恶化。每条记录包含代码、名称、事件类型、触发时间、数据来源、价格、指标数值、触发阈值、数据有效性。默认不发送通知、不弹窗。
- 公式与阈值见 `src/indicators.py`、`src/screener.py`、`src/anomaly_detector.py` 文件头注释及 `docs/DATA_FIELDS.md`。

---

## 10. 更换数据源

`market_data.py` 定义了 `MarketDataAdapter` 抽象接口。新增合规行情 API 时，实现相同的方法并返回相同字段的 `FetchResult`，再在 `build_adapter()` 与配置校验中登记即可，采集、指标、筛选、导出代码无需改动。请遵守数据提供方的使用条款与频率限制。

---

## 11. 单元测试

`tests/` 下共 13 个测试文件，覆盖：配置校验、时区、指标公式与“截断未来数据不改变历史值”的防未来函数检查、数据标准化与不填补缺失值、重试上限与熔断、数据库参数化写入/写入暂停/事务回滚、内存分级与缓存释放、交易日历与节假日、调度退避与每日任务、清理保留规则与保护措施、原子写入与 manifest 校验、采集的过期判定与日K续传/复权变化重取。

测试只使用本地构造的小样本数据（仅用于验证代码逻辑），不访问网络，不写入生产数据。**这些测试从未被执行过。**

---

## 12. 已知技术限制（摘要）

详见 `docs/KNOWN_LIMITATIONS.md`。要点：AKShare 是公开网页接口的开源封装，非交易所授权行情，字段可能变化；全A快照没有行情时间戳；前复权历史价格会随除权除息整体变化，回测时需注意；涨停/跌停家数为近似统计；量比口径区分（数据源盘中量比 vs 日线量比）；内存硬限制的口径边界见第 7 节。

---

## 13. 尚未经过运行验证的部分

由于本次任务禁止安装和执行，以下内容**全部未经运行验证**：

1. 所有 Python 模块的实际运行（只做了静态检查与人工逻辑审查）。
2. 全部单元测试（已编写，未执行）。
3. AKShare 各接口在你安装的版本中的实际函数名、参数与返回字段（代码对必需字段做了显式校验，不匹配时会报错而不是静默出错）：`stock_zh_a_spot_em`、`stock_zh_index_spot_em`/`stock_zh_index_spot_sina`、`stock_board_industry_name_em`、`stock_board_industry_cons_em`、`stock_zh_a_hist`、`index_zh_a_hist`、`stock_zh_a_hist_min_em`、`tool_trade_date_hist_sina`。
4. Windows Job Object / 硬性工作集上限在你电脑上的实际生效情况（ctypes 结构体按 Windows SDK 定义编写，未在 Windows 上运行）。
5. 实际内存占用（预计稳定在数百 MB，但未测量；依赖 pandas/akshare 版本）。
6. `start.bat` / `stop.bat` 在中文路径、不同控制台代码页下的表现。
7. 全A日K首次回填耗时、接口限流下的成功率。
8. 交易日历、数据状态判定、清理、导出在真实交易日中的端到端行为。

建议首次使用时：先 `--check-config`，再运行 `pytest`，然后在非交易时段启动一次观察 `logs\astock.log` 和 `work_data\market_status.json`。

---

## 14. 免责声明

本项目只是数据采集与整理工具。技术指标、评分与异动记录是对历史数据的客观描述和可调整的研究假设，不构成任何投资建议或买卖结论。项目不进行任何真实证券交易。
