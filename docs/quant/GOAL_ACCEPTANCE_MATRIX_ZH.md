# AI-Trader 15 项系统验收矩阵

本文把原定 15 项系统目标与当前代码、测试和服务器证据对应起来。`代码完成` 表示实现和自动化测试已经存在；`运行验收` 还要求服务器实时状态或外部依赖满足条件。

| # | 目标 | 主要实现 | 当前证据 | 状态 |
|---:|---|---|---|---|
| 1 | 全市场动态币池 | `quant/lab_worker.py`、`quant/market_ranker.py` | 服务器 `universe_count=522`，候选池动态轮换 | 代码完成，运行通过 |
| 2 | 全市场逐笔成交采集 | `quant/lab_worker.py` 的分片 `@trade` 订阅 | 5 个分片连接，`trade_symbols_seen=522`，`raw_dropped=0` | 代码完成，运行通过 |
| 3 | 候选盘口重建 | `quant/order_book.py`、快照与 `U/u/pu` 校验 | 当前候选盘口就绪，运行时 `depth_gaps=0` | 代码完成，运行通过 |
| 4 | Redis 实时层 | `quant/lab_store.py`、`quant/cache.py` | Redis healthcheck 正常，实时状态和事件 Stream 可读 | 代码完成，运行通过 |
| 5 | Parquet/ClickHouse 历史库 | `quant/event_lake.py`、`quant/event_export.py` | Parquet/ZSTD 已写入并校验；ClickHouse exporter 有测试 | Parquet 运行通过；生产 ClickHouse 待外部地址/凭据 |
| 6 | 订单流特征 | `quant/lab_features.py` | `microstructure-v3` 固定 14 维特征写入实时层和 SQLite | 代码完成，运行通过 |
| 7 | 向量模型 | `lab_train.py` 的 StandardScaler + MiniBatchKMeans 原型融合 | 模型文件格式、特征 schema 和推理门禁有测试 | 代码完成；真实模型待训练门禁 |
| 8 | 方向预测模型 | `lab_train.py` 的多分类逻辑回归 | 类别、权重、模型 hash 和推理校验有测试 | 代码完成；真实模型待 OOS 验证 |
| 9 | 全市场排序器 | `quant/market_ranker.py` | Redis `ranking` 返回方向、活跃度、波动和成交量排序 | 代码完成，运行通过 |
| 10 | 历史盘口回放 | `quant/book_replay.py`、`quant/event_validation.py` | 当前候选集合最新严格窗口 `valid=true`：32,951 条事件、11,149 次盘口重建、0 个序号缺口、12/12 币种；报告 `strict-window-search-live.json` | 代码完成，当前窗口通过；每个新模型仍需独立 OOS 窗口 |
| 11 | 毫秒级执行统计 | `quant/execution_state.py`、`quant/book_replay.py` | 回放报告记录 ACK、首次成交、撤单和本地处理 P50/P95/P99 | 代码完成，运行通过 |
| 12 | 三重障碍标签 | `quant/event_labels.py` | 使用逐笔成交路径，缺口/重连/覆盖不足样本标记无效 | 代码完成，运行中累计标签 |
| 13 | Walk-forward 与防泄漏 | `lab_train.py`、`event_validation.py` | chronological split、purge/embargo、validation-only 阈值选择 | 代码完成，模型未达到数据门禁 |
| 14 | 组合相关性风控 | `quant/correlation_risk.py`、`engine.py` | EWMA 协方差、收缩、带符号敞口纳入开仓容量 | 代码完成，自动化测试通过 |
| 15 | 部分成交和订单状态机 | `quant/execution_state.py`、`quant/paper_execution.py` | IOC/GTC、撤单、重启恢复、事务回滚和盘口消耗有测试 | 代码完成，运行通过 |

## 当前硬门禁

- 服务器采集器当前持续运行，但有效训练数据覆盖约 0.29 天；训练要求至少 10,000 条干净样本和 7 天连续覆盖。门禁未通过时模型保持 `ready=false`，模拟盘不开新仓。
- 本轮手动训练检查已读取最新数据：60,413 原始样本、7,595 条干净样本、0.295 天干净覆盖，结果仍为 `insufficient_data`。
- 服务器已部署只读运行验收命令 `python -m quant.runtime_acceptance`。最新执行中采集、全市场逐笔覆盖、候选盘口、Redis、Parquet 上限、动态杠杆、模拟安全状态和实盘只读检查均通过；命令返回 `1` 仅因为模型 OOS 门禁仍未通过。最新统计为 7,917 条干净样本、0.307 天覆盖，ClickHouse 生产目标仍未配置。
- 随后手动训练检查读取到 62,462 条原始样本、7,987 条干净样本、0.310 天干净覆盖，结果仍为 `insufficient_data`；模型没有被启用。
- 候选池轮换期间的 `Feature warmup` / `warming after reconnect` 已在验收脚本中分类为暂态；只有深度缺口、采集错误或未知阻断才使运行检查失败。修复已部署并验证当前 12/12 候选就绪、`depth_gaps=0`。最新验收统计为 8,169 条干净样本、0.316 天覆盖，模型 OOS 门禁仍未通过。
- 验收脚本现在同时检查严格历史盘口回放；兼容旧回放报告中缺省的 `valid` 字段，但明确的 `valid=false` 或序号缺口仍会失败。`Event OFI needs five seconds of continuous depth` 也已分类为候选建立盘口后的暂态。部署后验证 12/12 候选就绪、严格回放 4,321 条事件且序号缺口为 0；最新干净样本为 8,294 条、覆盖 0.321 天，模型 OOS 门禁仍未通过。
- 本地完整回归为 **390 passed、2 skipped**；仅有 Starlette/httpx 弃用警告，没有量化测试失败。
- 事件库当前使用 Parquet/ZSTD；生产 ClickHouse 目标为空，不能把 exporter 适配器测试当成生产 ClickHouse 已上线。
- 历史成交缺口中仍有 unresolved 区间。没有配置只读 `BINANCE_MARKET_DATA_KEY` 时，系统只尝试有限公共补采，无法声称旧历史完全连续。
- 实盘账户 WebSocket 只读连接保持开启，`execution_enabled=false`；模拟账本独立、暂停、空仓。

## 验收规则

模型只有在样本数量、连续覆盖、独立 validation/test/stress 事件回放、模型版本、执行策略 hash 和事件清单 hash 全部匹配后，才允许进入独立模拟。历史回放的毫秒数是本机处理和模拟撮合观测，不是币安真实订单延迟或盈利保证。

## 一键只读检查

在 API 容器内执行，不会改变暂停状态、订单、密钥或模型文件：

```bash
cd /home/ai-trader-quant
sudo docker compose -f deploy/compose.yaml exec -T api \
  python -m quant.runtime_acceptance
```

返回码为 `0` 才表示运行健康且模型样本外门禁已通过；返回码为 `1` 表示服务运行正常但仍有硬门禁未满足（例如数据覆盖不足）；返回码为 `2` 表示 API 读取或认证失败。输出只包含计数、状态和门禁原因，不输出 API 密钥、邮件授权码或账户令牌。
