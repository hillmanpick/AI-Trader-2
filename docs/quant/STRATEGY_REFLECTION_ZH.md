# 模拟盘策略反思

模拟盘现在会在每次平仓后记录策略版本、币种、方向、市场状态、AI 审核结果、持仓时间、毛盈亏、开平手续费、滑点和净盈亏。SQLite 保留最近的交易上下文，后台的模拟账户页面会显示当前反思版本、样本数、窗口净盈亏和成本占比。

反思器按 `QUANT_PAPER_REFLECTION_INTERVAL_TRADES` 笔交易触发一次，使用最近 `QUANT_PAPER_REFLECTION_WINDOW` 笔已完成交易；少于 `QUANT_PAPER_REFLECTION_MIN_TRADES` 笔时只记录，不调整策略。当前允许自动推广的候选只有一个：当窗口净亏损且手续费、滑点成本至少占毛波动 50% 时，模拟盘启用已有的 `QUANT_PAPER_ENTRY_FILTER=enforce` 成本过滤。

候选版本只在 `paper` 模式生效，不会修改币安 API、杠杆、保证金、交易对、实盘开关或实盘账户。DeepSeek 可以继续用于解释和开仓审核，但不能直接写策略参数。历史回放仍然用于独立验证；反思结果不是未来盈利保证。

默认配置：

```dotenv
QUANT_PAPER_REFLECTION_ENABLED=true
QUANT_PAPER_REFLECTION_MIN_TRADES=12
QUANT_PAPER_REFLECTION_WINDOW=50
QUANT_PAPER_REFLECTION_INTERVAL_TRADES=10
```

技术选择是 Python + SQLite 键值账本 + 确定性统计。这样每笔交易可追溯、进程重启后状态仍在，而且候选变更可回滚。没有让大模型直接逐笔改参数，因为那会受到最近噪声影响，容易过拟合并在高杠杆下放大错误；没有只依赖历史回放，因为回放无法覆盖当前订单簿、资金费和延迟。
