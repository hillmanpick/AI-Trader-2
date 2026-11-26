# 币安私有执行、只读观测与断线恢复

本轮复用生产 Key/Secret 读取账户，不开启实盘下单。模拟 worker 不接收
生产凭据，不改变模拟资金、仓位、策略审批和撮合假设。

## 模块与边界

- `private_gateway.py`：aiohttp 常连接 WebSocket RPC，请求 ID 关联、HMAC-SHA256
  排序签名、有界并发、发送节流、响应限额观察、P50/P95/P99。生产写请求在
  适配层拒绝；只有显式选择 testnet 并解锁的 DemoExecutor 可下单/撤单。
- `private_journal.py`：独立 SQLite WAL/FULL 账本，绑定环境及 Key 哈希。
  本地意图先落盘，再发送；未知结果不重发，按 symbol/orderId、symbol/tradeId
  去重。只有本地持久化意图可被视为系统订单，手动仓位和手动订单不接管。
- `account_stream.py`：继续处理 ACCOUNT_UPDATE，同时传递 ORDER_TRADE_UPDATE；
  断线/重连强制失效旧快照；带版本令牌的 REST seed 防止覆盖更晚的账户事件。
- `private_reconcile.py`：恢复时封锁写入，缓冲订单流，读账户、持仓和当前挂单；
  消失/未知订单逐笔查单；成交历史用重叠窗口和 fromId 分页补查，单页事务落盘。
  超时、查无订单、分页未结束、连接代际变化或缓冲溢出不能被报告为恢复成功。
- `observe.py`：现有观察进程持续记录私有订单，启动/重连/定期执行只读对账，
  订单事件直接唤醒持久化，不必等账户轮询间隔；通过原账户 API 输出脱敏摘要。
  没有接入任何策略的生产发单调用。
- `private_probe.py`：只读 RTT 及主动断线恢复探针，不提供下单选项。

## 配置

生产继续使用服务器原有 `BINANCE_API_KEY` / `BINANCE_API_SECRET`。新增：

```dotenv
QUANT_PRIVATE_JOURNAL_PATH=/data/private-production-observer.db
# 只有验证官方 testnet 的下单时才需要另一套测试凭据；绝不回退到生产密钥。
BINANCE_TESTNET_API_KEY=
BINANCE_TESTNET_API_SECRET=
```

原 `QUANT_ENABLED=false`、`QUANT_LIVE_ENABLED=false`、
`QUANT_OBSERVATION_ONLY=true` 保留；不更改交易权限或账户杠杆。

## 只读真实测量

```bash
sudo docker exec ai-trader-quant-quant-1 python -m quant.private_probe \
  --environment production --samples 30 --interval 0.5 \
  --recovery-journal /data/private-recovery-probe.db \
  --status-db /data/quant-paper.db --output /data/private-gateway-evidence.json
```

探针重新连接的只是自己创建的 socket，不主动断开主观察器。结果分别报告
公共 depth 请求和签名 v2/account.status 读取的 RTT、错误数、重连耗时以及
用户数据流强制重连后对账结果。RTT 包含客户端、网络及网关处理；不能分解
交易所内部处理，不能冒充真实订单 ACK、成交或交易所排队优先级。

## 验证与局限

本机 aiohttp WebSocket 假交易所测试覆盖响应乱序、送达后超时、发送中断、
重复/乱序订单事件、账户快照竞争、断线恢复、溢出恢复、未知订单不重发、
成交分页和持久化身份隔离。这些是协议/状态机测试，不是真实交易所成交证据。

对账只回补**已跟踪币种**（配置、仓位、挂单或本进程见过订单事件的币种）。
初始历史从启动前六分钟开始；进程重启保留恢复游标。不会声称扫描全账户全部
历史。当前实现针对标准订单，尚未把条件 Algo 订单完整纳入统一生命周期。
仓位/账户 REST 快照也没有交易所提供的跨接口原子快照保证，账户事件竞争时
拒绝发布恢复成功，留待下一轮。用户数据流不是无限保留的可回溯消息队列。

生产订单的真实 ACK、成交回报和撤单延迟没有测试，生产下单保持禁用。官方
testnet 的真实下单闭环需要测试凭据，不能用当前生产 Key 在 testnet 试单。
普通共享云机和互联网 API 不因接入 WebSocket 自动成为交易所级高频系统。

## 发布

在已验证的原镜像上叠加本模块，`patch_private_image.py` 仅对必要配置/API
字段做一次精确替换；源码漂移则构建失败，不覆盖整个服务器项目。
`private-execution.compose.yaml` 只更新观察器和 API，模拟/采集/训练镜像保留。
Web 静态资源保留旧哈希文件，最后替换 index，避免正在打开的页面请求旧资源失败。

## 2026-11-26 线上只读验收

北京时间 10:42 开始测量，生产环境 depth 和 v2/account.status 各发送 30 次只读
RPC，均成功，错误数 0。盘口 RTT P50/P95/P99 约 71.85/74.70/80.50 ms；
签名账户读取约 72.65/74.58/78.12 ms。中途重新建立 RPC socket 约 223 ms。
独立探针账户流强制断开后，重连并完成跟踪币种对账耗时约 1949 ms；恢复后的
REST 对账约 436 ms。探针资源上限为 192 MiB、0.5 CPU，结果包含调度开销。

真实账户当时没有新订单/成交事件，不能把空账户恢复成功等同于真实成交回补
全部验证完成。生产订单 ACK 和成交延迟明确未测量，探针发送订单数为 0。
服务器没有配置 BINANCE_TESTNET_API_KEY/SECRET，官方 testnet 实际发单尚未验证。

已上线观察器 private-observer-20261126、状态 API private-api-20261126 及前端诊断；
模拟 worker/采集器/训练器容器不变。原完整后端回归 638 通过、2 跳过，前端构建
通过；后续增加的两项 Demo 生命周期边界测试见本模块测试文件。
