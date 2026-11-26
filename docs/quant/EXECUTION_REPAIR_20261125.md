# 2026-11-25 模拟执行恢复

此次更新只作用于原模拟账本 `/home/ai-trader-quant/deploy/data/quant-aggressive-paper.db`，实盘执行与 DeepSeek 保持关闭，不修改方向模型审批门槛。

## 已确认问题

- 过去 24 小时没有成交，五个旧仓位的退出请求持续数百至上千次重试。
- 退出单出现 `IOC intent expired before acknowledgement`，有的超过 40 秒才处理。
- 撮合时间在新订单提交前获取，导致本轮跳过新单；随后同步计算全历史绩效与统计，让订单超过两秒有效期。
- 线上策略仍是 `systematic-flow-multi-v1`，按五秒信号工作，但 LabExecution 错误地要求亚秒特征；线上采集器不提供这个条件。
- 特征采样与历史 SQLite 写盘串行，形成 `Sample gap in recent history`，所有候选币长期无法提供新仓特征。

## 修复

1. 撮合前重新获取当前时间，提交后优先处理二十毫秒确认，不延长 IOC 有效期，不填造流动性。
2. 反思关闭时不重算历史绩效；启用时限频。盘口订单尚待确认时不运行历史统计。
3. 挂单查询增加部分索引。实时统计状态计数仍覆盖全部订单，延迟与部分成交指标仅取最近二百笔，输出中注明统计范围；完整研究统计仍支持全量。
4. 根据实际策略 grid_ms 选择特征新鲜度。五秒策略不要求亚秒字段；亚秒策略仍保留 1500ms 与 fast.ready 门槛。
5. 线上采集器单独每秒生成实时特征，历史归档仍使用原采样频率。重启/数据间断后重新预热，不把缺失数据改为有效样本。

## 版本与运维

线上模拟 worker：`ai-trader-quant:execution-repair-20261125`。
线上采集器：`ai-trader-quant:collector-cadence-20261125`。
保留原基础镜像，避免混入本地尚未部署的 `systematic-flow-event-v1` 亚秒策略。

```bash
cd /home/ai-trader-quant/deploy
sudo docker compose -f compose.yaml -f radar.compose.yaml \
  -f hybrid-paper-worker.compose.yaml -f collector-cadence.compose.yaml \
  up -d --no-deps paper-worker lab-collector
```

训练器继续使用独立 `hybrid.compose.yaml`，以上操作不重建训练器。服务器修复构建目录位于 `/home/ubuntu/execution-repair-20261125` 与 `/home/ubuntu/collector-repair-20261125`，保留原源码快照及补丁。

## 验证

- 执行、盘口、账本、回放完整性、多仓和特征等测试共 101 项通过。
- 五个旧仓位通过新增持久化成交退出；最新四个退出单观测延迟约 30–109 ms，这是模拟接收时钟统计，不是交易所执行延迟。
- 原账本于北京时间 18:30:37 出现 OPGUSDT 新多仓成交，三倍杠杆，证明原模拟开仓链路恢复。
- 采集器预热后观测到十二个有效候选特征，worker 心跳 running、无错误。
- 策略仍标记 `approved=false`、`paper_qualified=false`；成交恢复不构成盈利验证。

方向训练实际使用标准化的收益率、波动、订单流、主动成交量、盘口不平衡、BTC 环境和延迟特征，拟合逻辑回归与标准化原型，随后做时间顺序的 walk-forward、purge/embargo 和净成本回放。它与当前规则策略的模拟开平仓是两条不同的链路。重复训练相近窗口不能当成多份独立盈利证据。
