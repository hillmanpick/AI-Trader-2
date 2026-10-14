# 官方社区与 DeepSeek 开仓审核

入口：`https://124.156.199.59:8443/community`。量化状态：`https://124.156.199.59:8443/quant`。

官方公开接口实际使用 `https://ai4trade.ai/api`，公开加密货币操作、策略、讨论及排行榜均已验证能读取，无需登录 Token。
文档中的 `api.ai4trade.ai` 在本次验证时无法解析，因此没有使用。

这是只读社区集成，不向官方发布内容，不修改关注关系，不自动照搬别人订单。官方排行榜为平台模拟记录，涵盖平台市场，不是你的币安收益。
本地原有社区账户仍独立。官方网页的登录不会自动传到本地服务。Agent 22728 仅作为页面筛选项；没有读取或复制浏览器登录凭据。

## 填写 DS 密钥

```bash
sudo nano /home/ai-trader-quant/deploy/.env
```

相关配置：

```dotenv
AI4TRADE_ENABLED=true
AI4TRADE_AGENT_ID=22728
AI4TRADE_TOKEN=
AI4TRADE_POLL_SECONDS=60
QUANT_AI_ENABLED=true
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat
QUANT_AI_MIN_CONFIDENCE=0.65
QUANT_AI_REVIEW_MAX_AGE_SECONDS=90
```

只需把 DeepSeek 控制台的 API Key 填到 `DEEPSEEK_API_KEY=` 后面。公开数据不需要填写 `AI4TRADE_TOKEN`；预留此字段仅用于将来有权限的官方接口，不应填写币安密钥。
模型名称、可用性和额度以你的 DeepSeek 账户为准；本次没有 DS 密钥，未验证实际模型推理。
保存后执行：

```bash
cd /home/ai-trader-quant/deploy
sudo docker compose up -d --no-build --pull never --force-recreate api quant community-worker
```

然后到“币安合约量化”的“DeepSeek AI 判断”点击“测试 DeepSeek 连接”。连接测试不下单。
`docker restart` 不会重新加载 Compose `.env` 中的环境变量，需要重建容器。

## 审核规则

规则策略用已收盘 K 线产生候选开仓信号，DeepSeek 只能放行或否决，不能设定订单金额、杠杆、止损或方向。
输入仅含候选方向、最近已收盘价格和相关的公开社区内容，不发送币安密钥、邮箱授权码或账户余额。
不新建跟单策略，不因社区的买入文字直接开仓。AI 不是收益保证，应继续在模拟盘观察。

AI 已启用但未配置 DS 密钥时不开新仓；止盈、止损、规则平仓继续运行。模型超时、异常、格式错误、审核过期或置信度不足均不开仓。
模型后台异步运行，最多两个待审核任务，不阻塞风险检查；审核结果返回后重新检查当前仓位、余额、暂停状态、每日亏损和总风险，使用当前价格执行。
跨越下一根 K 线或超过 90 秒的结果不执行；重启丢弃未完成审核，不重放旧信号。
已有候选信号按消费记录处理，不会在填入密钥后追单旧信号；等待下一个信号。

社区由独立 `community-worker` 每分钟拉取，网页读取缓存。单个接口失败时保留旧数据并标注异常，不会阻塞交易。
送给 AI 的社区内容只取缓存三分钟内更新、发布一小时内、与对应交易对相关的操作或策略，最多八条，重复转载文本去重；未来异常时间与过期数据剔除。
没有有效社区内容时使用行情审核；社区内容作为不可信文本处理，JSON 响应严格校验，最终固定风控始终生效。

目前保持 `QUANT_MODE=paper`、`QUANT_LIVE_ENABLED=false`。填写 DS 密钥不会切换实盘。

## 查看状态

```bash
cd /home/ai-trader-quant/deploy
sudo docker compose ps
sudo docker compose logs --tail=50 community-worker quant
```

代码与镜像已保留更新前备份。新功能无额外 pip/npm 依赖，复用现有镜像依赖层，避免全量升级引入版本差异。
