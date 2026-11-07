# 当前模拟账户的 DeepSeek 自主操盘

本功能使用原有 `QUANT_PAPER_DB_PATH`、余额、持仓和成交账本，不创建新账户、不重置盈亏。
`QUANT_AI_ENABLED` 继续关闭，实盘观察进程不调用 DS、不获得自动下单权限。

## 决策与执行

设置 `QUANT_PAPER_AI_AUTOPILOT=true`、`QUANT_PAPER_STRATEGY=lab_model`、
`QUANT_PAPER_LAB_REQUIRED=true`、`QUANT_PAPER_EXPLORATION_MODE=true`，保留动态杠杆及动态保护配置。
已有的 `DEEPSEEK_API_KEY`、`DEEPSEEK_BASE_URL` 和 `DEEPSEEK_MODEL` 用于请求，密钥不发给模型。

`paper_autopilot.py` 每 `QUANT_PAPER_AI_AUTOPILOT_INTERVAL_SECONDS=60` 秒最多发送一批候选币、当前模拟持仓和滚动扣费反馈。
DS 可以选币、开多、开空、观望、平仓，选择保证金、杠杆上限、止盈止损比例及 60–3600 秒持仓时间；`update` 允许调整保护参数，止损只收紧。
旧数值模型的方向输出、验证状态和历史币种冷却不决定 DS 是否开仓；它们的研究数据和历史记录保留。
DS 策略本身仍是未验证的模拟实验，不能把数值模型报告当作 DS 策略盈利证明。

数量精度、最小成交量、每仓与总保证金、最大杠杆、强平距离、单仓风险预算、组合相关性、盘口容量和成本过滤继续由程序限制。
DS 选择的杠杆是上限，执行器可调低；流动性不足时缩小模拟订单，不能虚构成交。
保护止损、模拟强平和持仓到期退出由程序持续检查，不等待 DS；本功能没有毫秒级 DS 推理能力。
反向交易必须先平仓，下一批再考虑反向，不在一条决策内翻仓或加仓。

## 调用与账本

使用固定 system 与固定顺序的策略参数作为前缀，动态特征、模拟账户和反馈放在后部。
一个后台线程只运行一个请求；不按币排队。重启保留上次请求时间，不会每次重启立即重复付费调用。
HTTP/JSON/参数校验失败、置信度不足、响应超过审核有效期、持仓变化、价格变化超过 0.5% 都不会执行该动作。
每条动作只消费一次，交易 API 密钥不注入模拟容器。模型没有 shell 或任意工具执行权限。

网页与 `/api/quant/paper/status` 的 `autopilot` 展示请求状态、最近判断、累计 token、缓存命中及未命中数、响应耗时。
`signals` 展示 DS 动作的检查结果，`sizing` 展示最近仓位参数受阻原因，实际成交以 `orders`、`execution_fills` 与持仓账本为准。
模拟邮件仍只按北京时间 08、10、12、14、16、18、20、22 点发送汇总，逐笔成交不发邮件。

## 回退

将 `QUANT_PAPER_AI_AUTOPILOT=false` 后只重建 `api paper-worker`，恢复原数值策略；不删除账本。
服务器部署前需备份 compose、环境文件和用 SQLite backup API 生成的模拟账本副本。
切换策略不会改变已有仓位的开仓保证金，原有保护和到期退出继续运行。
