# 数据字段说明

## 通用约定

| 约定 | 说明 |
|---|---|
| 编码 | 全部文件 UTF-8（CSV 可在配置中改为 utf-8-sig 以便 Excel 打开） |
| 时间 | work_data 文件中一律为北京时间 ISO-8601，带 `+08:00`；数据库中为 UTC（`+00:00`） |
| 交易日期 | `YYYY-MM-DD`，按北京时间划分 |
| 缺失值 | CSV 空单元格 / JSON `null` / SQLite `NULL`。缺失就是缺失，从不填补 |
| 股票代码 | 6 位字符串，含前导零（读取 CSV 时请按文本读取 `code` 列） |
| 成交量 | `volume_lots` 单位为“手”（1手=100股）；指数 `volume` 单位以数据源为准 |
| 金额 | `*_yuan` 单位为人民币元 |
| 百分比 | `pct_change`、`turnover_rate`、`amplitude_pct` 为百分数（1.5 表示 1.5%）；`*_ratio`、`ret_20`、`volatility_20` 等为小数（0.015 表示 1.5%） |

### data_status（数据状态）

| 值 | 含义 |
|---|---|
| `realtime` | 有行情时间戳，且与获取时间相差在阈值内（默认 150 秒） |
| `delayed` | 有行情时间戳，但明显落后于获取时间 |
| `historical` | 已收盘的历史数据（日K、指标、非交易时段的分钟K） |
| `unknown_time` | 数据源没有提供行情时间戳，只有本地获取时间 `fetched_at`，实时性未经验证 |
| `stale` | 判定为过期：交易时段内内容长时间完全不变，或文件已超过有效期 |
| `invalid` | 未通过校验（如行数明显不足），不应使用；此类数据不入库、不替换旧数据 |

### time_basis（时间依据）

`quote_time`（行情自带时间）/ `bar_time`（K线时间）/ `trade_date`（交易日期）/ `fetch_time_only`（仅本地获取时间）。

---

## work_data/manifest.json

| 字段 | 说明 |
|---|---|
| `manifest_seq` | 清单序号，每次写入 +1（跨程序重启延续） |
| `generated_at` | 本清单写入时间 |
| `writer.running` | 写入时程序是否在运行；正常退出时最后一次写为 `false` |
| `writer.heartbeat_expires_at` | 若早于当前时间，说明程序已停止或卡住，所有数据都应视为可能过期 |
| `files.<key>.version` | 该文件的版本号，每次成功重写 +1 |
| `files.<key>.generated_at` | 文件生成时间 |
| `files.<key>.data_time` | 文件所含数据的时间（多个来源时取最旧的一个） |
| `files.<key>.expires_at` | 有效期：交易时段内行情 = 数据时间 + 180 秒；非交易时段 = 下一次行情会变化的时刻 |
| `files.<key>.freshness` | 写清单那一刻的判断：`fresh` / `expired` / `unknown`。程序停止后不会再更新 |
| `files.<key>.data_status` / `time_basis` / `source` | 见上 |
| `files.<key>.rows` / `size_bytes` / `sha256` | 行数、字节数、SHA-256 校验值（与文件内容逐字节对应） |
| `files.<key>.write_status` | `ok` / `last_write_failed: ...`（文件被占用等，旧文件保持不变）/ `not_generated_yet` |
| `files.<key>.from_previous_session` | 该文件是上次运行留下的，本次尚未重写 |

## work_data/stocks_latest.csv（最新全A快照）

`code, name, exchange(SH/SZ/BJ), price, pct_change, change, volume_lots, amount_yuan, amplitude_pct, high, low, open, prev_close, volume_ratio(数据源盘中量比), turnover_rate, pe_dynamic, pb, total_mv_yuan, float_mv_yuan, validation_flags, fetched_at, quote_time(空=数据源未提供), time_basis, data_status, source`

`validation_flags`：`price_missing`（停牌或无成交）、`price_non_positive`、`high_lt_low`、`price_above_high`、`price_below_low`、`pct_change_extreme`（|涨跌幅|>35%，新股首日可能出现）、`negative_volume`、`negative_amount`。标记只用于提示，原始数值不做任何修改。

## work_data/market_latest.json

| 字段 | 说明 |
|---|---|
| `session` | 当前交易时段、交易日历判断依据、最近已收盘交易日 |
| `indices` / `indices_meta` | 上证指数、深证成指、创业板指的最新快照及来源/时间/状态 |
| `breadth` | 由全A快照派生：`total`、`traded`、`up`、`down`、`flat`、`suspended`、`limit_up_approx`、`limit_down_approx`（近似）、`advance_ratio = up/(up+down)`、`median_pct_change`、`amount_total_yuan` |
| `breadth_intraday_peak_advance_ratio` | 当日盘中上涨占比最高值 |
| `industry_boards_top10` / `bottom10` / `industry_meta` | 行业板块涨幅前后 10 名 |
| `spot_meta` | 全A快照的来源、获取时间、状态、警告 |
| `watchlist` | 自选股快照 + 最近一根分钟K（`minute_last_bar_time`、`minute_close`、`minute_data_status`） |
| `spot_crosscheck` | 自选股快照价与实时分钟K收盘价的一致性核对（`compared`、`matched`） |
| `status_legend` | data_status 含义 |

## work_data/candidates.csv（最近一次筛选）

| 字段 | 说明 |
|---|---|
| `run_id` / `run_at` | 筛选批次与时间；`run_id` 前缀 `intraday-`（盘中）或 `close-`（收盘后） |
| `rank` | 排名；`0` 表示未参与排名（仅自选股会以 rank=0 出现，原因见 `reason_codes` 中的 `NOT_RANKED:*`） |
| `in_watchlist` | 1 = 自选股 |
| `price`、`pct_change`、`amount_yuan` | 来自筛选时的全A快照 |
| `total_score` | 已得分合计 |
| `available_max` | 可得满分（缺失子项不计入） |
| `score_normalized` | `total_score / available_max × 100`，排名依据 |
| `sector_score/_max`、`volume_price_score/_max`、`market_score/_max`、`risk_reward_score/_max` | 分项得分与该项可得满分；可得满分为 0 时得分为空 |
| `catalyst_score` / `catalyst_max` / `catalyst_status` | V1.0 恒为 空 / 0 / `missing:no_reliable_news_or_fundamental_source` |
| `missing_components` | 缺失原因，如 `sector:no_board_membership_or_board_data`、`volume_price:no_indicator_history`、`market:no_breadth`、`catalyst:no_reliable_source`、`risk_reward:no_overhead_reference_above_high20` |
| `reason_codes` | 满足的条件代码：`ABOVE_MA20`、`MA_BULL_ALIGN`、`VOL_RATIO_IN_RANGE`、`BREAKOUT_VOLUME_LAST_CLOSE`、`PULLBACK_LOW_VOLUME_LAST_CLOSE`、`PRICE_ABOVE_HIGH20_WITH_VOLUME`、`MACD_HIST_POSITIVE`、`RSI_IN_RANGE`、`BOARD_TOP_QUARTILE`、`BOARD_BREADTH_STRONG`、`MARKET_BREADTH_POSITIVE`、`INDEX_ABOVE_MA20_MAJORITY`、`RR_FAVORABLE`、`LOW_VOLATILITY`、`AT_OR_BELOW_LOW20`、`AT_OR_ABOVE_HIGH20`、`NOT_RANKED:<过滤原因>` |
| `board_name` | 所属东方财富行业板块 |
| `indicator_trade_date` | 所用指标的交易日（盘中筛选为前一交易日） |
| `spot_fetched_at` / `spot_data_status` | 所用快照的获取时间与状态 |
| `model_version` | 评分模型版本 |

过滤原因：`FILTER_ST`、`FILTER_DELISTING`、`FILTER_BSE`、`FILTER_NO_PRICE`、`FILTER_LOW_AMOUNT`、`FILTER_DATA_FLAGS`、`INSUFFICIENT_DATA`（可得满分不足）。

评分子项（权重 w 为配置值）：

| 组成 | 子项与计算 |
|---|---|
| 板块强度 | 板块涨幅升序排名分位 `(rank-1)/(n-1)` × 0.6w；板块上涨家数占比 × 0.4w |
| 量价结构（每项 w/5） | 价格 > MA20；MA5 > MA10 > MA20；量比 ∈ [1.2, 3.0] 满分，[1.0,1.2) 或 (3.0,5.0] 半分；上一交易日放量突破或缩量回踩信号，或盘中价格 > 20日高点且量比 ≥ 1.5；MACD柱 > 0 且 RSI ∈ [50,75] 满分，满足其一半分 |
| 市场环境（每项 w/2） | `clip((上涨占比-0.3)/(0.7-0.3), 0, 1)`；主要指数现价高于其 MA20 的比例 |
| 风险收益（2w/3 + w/3） | 区间内：`clip(((high_20-价)/(价-low_20))/3, 0, 1)`；价格 ≤ low_20 为 0；价格 ≥ high_20 时该子项缺失。波动率：`clip((0.8-σ)/(0.8-0.3), 0, 1)` |

## work_data/indicators.csv（每只股票最近一个交易日的指标）

`code, name, trade_date, computed_at, adjust, bars_available, close, ma5, ma10, ma20, ma60, vol_ma5, vol_ma20, volume_ratio_5d, turnover_rate, rsi14, macd_dif, macd_dea, macd_hist, high_20, low_20, prior_high_20, prior_low_20, high_60, low_60, pivot, support_s1, resistance_r1, ret_20, benchmark_ret_20, rs_20, volatility_20, ma20_slope_5, sig_breakout_volume, sig_pullback_low_volume, sig_trend_weakening, source, data_status`

公式见 `src/indicators.py` 文件头。要点：
- `volume_ratio_5d` = 当日量 ÷ 前5日均量（日线口径，与快照中的盘中 `volume_ratio` 不同）。
- `prior_high_20` / `prior_low_20` 不含当日，用于当日突破/跌破判断；`high_20` / `low_20` 含当日，作为下一交易日的参考位。
- `support_s1` / `resistance_r1` 为经典枢轴点（P=(H+L+C)/3，S1=2P-H，R1=2P-L），只是参考位。
- 信号为 1/0；数据不足时为空（不等于“没有信号”）。
- 价格为前复权（`adjust=qfq`）。

## work_data/alerts.json（最近异动，默认最多 500 条）

每个事件：`id, dedup_key, trade_date, code, name, event_type, triggered_at, data_time, time_basis, source, price, metrics{...}, thresholds{...}, data_status, indicator_trade_date`

| event_type | code | 触发条件（阈值记录在 thresholds 中） |
|---|---|---|
| `VOLUME_BREAKOUT` | 股票 | 现价 > 上一交易日的 high_20，量比 ≥ 2.0，涨幅 > 0 |
| `LOW_VOLUME_PULLBACK` | 股票 | MA20 上行，当日下跌，量比 ≤ 0.8，距 MA10 或 MA20 ≤ 1.5% |
| `SUPPORT_BREAK` | 股票 | 现价 < 上一交易日的 low_20 × 0.995 |
| `SPIKE_FADE` | 股票 | 最高涨幅 ≥ 5%，且从最高点回落 ≥ 3%（相对昨收） |
| `SECTOR_RESONANCE` | 板块代码 | 板块涨幅 ≥ 2%，上涨家数占比 ≥ 75%，涨幅 ≥ 5% 的成分股 ≥ 3 只 |
| `BREADTH_DETERIORATION` | `MARKET` | 上涨占比 ≤ 30%，或较当日盘中最高值回落 ≥ 25 个百分点 |

同一交易日、同一事件类型、同一代码只记录第一次触发。

## work_data/market_status.json（系统状态）

`program`（版本、进程号、启动时间、Python、平台）、`session`、`memory`（级别、阈值、最新采样各口径、Job 硬限制状态与峰值）、`database`（大小、上限、是否暂停写入）、`logs_dir_size_mb`、`trade_calendar`、`data_sources`（各数据集最近成功/失败时间、错误、连续失败次数）、`adapter`（熔断状态、最近一次调用耗时）、`jobs`（各任务最近运行状态）、`recent_errors`（最近 50 条）、`watchlist`、`latest_screen_run`、`spot_crosscheck`。

---

## SQLite 数据表（data/market.db）

| 表 | 主要字段 | 保留 |
|---|---|---|
| `spot_snapshots` | snapshot_id, code, 快照全部字段, quote_time, fetched_at, source, time_basis, data_status, validation_flags | 3 天 |
| `index_snapshots` | snapshot_id, code, name, price, pct_change, volume, amount_yuan, ... | 3 天 |
| `industry_snapshots` | snapshot_id, board_code, board_name, pct_change, up_count, down_count, leader_* | 3 天 |
| `breadth_snapshots` | 市场宽度派生统计 + derived_from | 3 天 |
| `minute_bars` | code, period, bar_time, OHLC, volume_lots, amount_yuan | 7 天 |
| `daily_bars` | code, adjust, trade_date, OHLC, volume_lots, amount_yuan, pct_change, turnover_rate | 365 天 |
| `index_daily` | code, trade_date, OHLC, volume, amount_yuan | 365 天 |
| `indicator_values` | 与 indicators.csv 相同 | 365 天 |
| `anomalies` | 与 alerts.json 相同（metrics_json / thresholds_json） | 30 天 |
| `screen_runs` / `screen_results` | 筛选批次与结果 | 90 天 |
| `collection_runs` | 每次采集的数据集、来源、开始/结束时间、状态、行数、错误类型与信息 | 30 天 |
| `daily_update_progress` | 日K更新进度（续传用） | 30 天 |
| `industry_members` | 行业板块成分 | 30 天未更新则删除 |
| `job_state` | 每日任务完成标记、进度偏移 | 30 天 |
| `daily_stats` | 每个交易日收盘时的市场宽度与指数收盘 | **长期** |
| `cleanup_runs` | 每次清理的时间、状态、删除明细、库大小变化 | **长期** |
| `trade_calendar` / `stocks` / `meta` | 交易日历、股票列表、结构版本 | **长期** |
