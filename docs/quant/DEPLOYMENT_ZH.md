# 币安 USDT 本位永续合约部署说明

## 当前部署

本地代码：`D:\aispea\jioayi1\AI-Trader`。
策略来源：`D:\aispea\jiaoyi\Qbot\qbot\strategies`，原仓库未改动。
服务器：`ubuntu@170.106.189.16`，安装目录 `/opt/ai-trader-quant`。
入口：`https://170.106.189.16:9443/quant`。
采用自签名 HTTPS 证书，浏览器会提示证书不受信任；生产长期使用可接入域名及受信任证书。
网页登录账号与随机密码仅保存在服务器 `/opt/ai-trader-quant/deploy/access.txt`（权限 600）。

```powershell
ssh -i C:\Users\10194\.ssh\password.pem ubuntu@170.106.189.16
```

登录服务器后：

```bash
sudo cat /opt/ai-trader-quant/deploy/access.txt
cd /opt/ai-trader-quant/deploy
sudo docker compose ps
sudo docker compose logs --tail=80 api quant
```

2026-10-13 实测：本机访问正式 `fapi.binance.com` 和测试 `demo-fapi.binance.com` 均返回 HTTP 451，提示所在地区不可用。
程序会显示 blocked 并停止下单；模拟盘也需要币安真实行情，因此在此地区限制解决前无法生成真实行情驱动的模拟成交。
需要把交易服务迁移到币安允许访问地区的服务器；不会通过替换主机或代理绕过地区限制。
没有币安凭据、邮箱凭据及可用地区条件，无法验收真实下单或实际邮件投递。

## 服务组成

- web：Nginx HTTPS、页面和量化接口访问密码、React 构建产物；原平台 API/WS 保留原项目认证规则，避免 Basic 与 Agent Bearer 令牌冲突。
- api：AI-Trader FastAPI，新增 `/api/quant/*` 私有接口。
- postgres：原社区平台账户、信号、模拟记录。
- platform-worker：原平台持仓刷新、收益历史任务。
- quant：独立合约策略与执行进程，SQLite WAL 持久化订单及邮件队列。

交易进程不把真实合约订单塞进原来的社区模拟账本。新增“币安合约量化”菜单展示独立持仓、权益、状态、事件。
交易账本使用 SQLite FULL 同步和进程文件锁，适合本机单执行进程；不要对同一账户启动多套实例。
后台进程均由 Docker restart 策略自动恢复，数据挂载持久化。
服务器约 2GB 内存，容器设置内存上限；前端在本地构建，减少服务器构建内存需求。

## 填写配置

```bash
sudo nano /opt/ai-trader-quant/deploy/.env
```

在服务器填写，不必发送到聊天：

```dotenv
BINANCE_API_KEY=
BINANCE_API_SECRET=
QQ_SMTP_USER=你的发件QQ邮箱
QQ_SMTP_AUTH_CODE=QQ邮箱SMTP授权码
QQ_MAIL_TO=收件邮箱
```

QQ 邮箱网页设置中启用 SMTP，使用授权码而不是 QQ 登录密码。
币安 API 使用专用账户或子账户，启用合约交易权限，限制可信服务器 IP，不需要提现权限。
程序只支持 USDT 保证金永续、单向持仓、逐仓；检测到双向持仓或未知仓位时阻止运行，不会自动接管。
测试网与正式账户的 API Key 不可混用。

配置更新后要重新创建容器，单纯 `restart` 不会更新 env_file：

```bash
cd /opt/ai-trader-quant/deploy
sudo docker compose up -d --force-recreate api quant web
```

默认 `QUANT_MODE=paper`，不会发送交易订单，但从币安正式公共接口读取行情。
测试网模式设置 `QUANT_MODE=testnet`，使用独立账本 `QUANT_DB_PATH=/data/quant-testnet.db` 和测试网密钥。
正式模式需要同时设置：

```dotenv
QUANT_MODE=live
QUANT_LIVE_ENABLED=true
QUANT_DB_PATH=/data/quant-live.db
```

正式模式会下真实订单。切换账户或模式必须使用独立数据库，不要删除旧订单账本；重启前核对是否还有仓位和未完成订单。
填写密钥本身不会把模拟模式切换成正式模式。

## 策略与默认风控

`QUANT_STRATEGY=sma_cross`：Qbot SMA 10/30，金叉开多，死叉平多；启用 `QUANT_ALLOW_SHORT=true` 后镜像死叉开空、金叉平空。
`QUANT_STRATEGY=bollinger`：Qbot 13 周期、2 标准差，下轨外开多、上轨外平多；可选镜像做空。
Qbot 原规则为股票回测示例，做空镜像、币安执行、止盈止损是新增逻辑，未宣称已验证盈利。
不直接导入原脚本，避免导入时抓股票数据、拉起绘图、执行演示主程序或依赖 TA-Lib/桌面 GUI。

默认 BTCUSDT/ETHUSDT，15m，1 倍，单笔名义金额 50 USDT，总名义金额上限 100 USDT，最多 2 个仓位。
止损价格距离 2%，止盈 4%；每日权益下降 20 USDT 后停止开新仓，北京时间换日重置。
名义金额不是保证金；约占用保证金 = 名义金额 / 杠杆。止损比例为价格变化，不是账户权益百分比。
实际最小名义金额及数量精度从币安 exchangeInfo 获取；50 USDT 若不满足 BTC 等标的的限制会跳过，不自动加大订单。
修改 `QUANT_ORDER_NOTIONAL_USDT` 时同时检查 `QUANT_MAX_TOTAL_NOTIONAL_USDT`。
仅收盘 K 线产生信号；每根 K 线最多处理一次。行情缓存到下一收盘，标记价每 10 秒检查。
模拟手续费为单边 0.05%，未计入资金费和真实订单簿滑点，不可将模拟收益等同实盘收益。

## 订单保护与限制

订单提交前持久化 clientOrderId；网络超时仅查询状态，不盲目重发。
未解决订单会阻止新订单。查询不确定时保留 PENDING，需核对交易所记录后处理，不能直接删除账本重试。
平仓采用 reduceOnly，保护单采用币安 `/fapi/v1/algoOrder` 的 STOP_MARKET / TAKE_PROFIT_MARKET、MARK_PRICE、closePosition。
保护单创建/核验失败会暂停开新仓，并尝试只减仓清仓；网络中断下不能保证清仓成功，应核对币安持仓。
程序定期对账，发现已观察持仓归零会记录清仓并清理本程序的遗留保护单。
每个标的只能有一个本程序仓位，策略退出后不会在同一根 K 线反向开仓。
禁止同一账户混用其它机器人；手动加仓、反向开仓可能破坏策略状态。

## 邮件规则（固定 UTC+8）

- 开仓、清仓及持仓数量变化入持久化邮件队列；白天在执行周期内发送。
- 00:00（含）至 08:00（不含）所有邮件静默，交易与止损继续。
- 夜间开平仓事件不丢弃，08:00 后合并补发。
- 持仓报告在 08、10、12、14、16、18、20、22 点时段各一次；空仓也报告。
- 进程若在报告点停机，恢复后在当前两小时时段补一份当前快照，不补夜间快照。
- SMTP 失败保留队列，至少间隔 60 秒重试；邮件已投递但本地尚未记账时宕机可能重复通知（至少一次投递）。
- 配置好邮箱后，可在网页点击“排队发送测试邮件”；夜间测试同样延迟到 08:00。

## 私有接口

通过 HTTPS 入口使用网页登录密码；Nginx 给后端注入私有操作令牌，不向网页返回密钥。
直连内部 API 需 `X-Quant-Token`，写操作另需 `X-Quant-Action: operator`。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/quant/status` | 配置摘要、心跳、仓位、事件、邮箱状态 |
| GET | `/api/quant/orders` | 本地持久化订单记录 |
| POST | `/api/quant/pause` | 暂停开新仓，保留既有保护 |
| POST | `/api/quant/resume` | 恢复开仓，仍受实盘开关、风险、连通性限制 |
| POST | `/api/quant/mail-test` | 排队邮件测试 |

## 运维与验证

```bash
cd /opt/ai-trader-quant/deploy
sudo python3 verify.py
sudo docker compose exec -T api python -m pytest tests/test_quant.py -q
sudo docker compose logs --tail=100 quant
sudo docker compose ps
```

备份必须包含部署配置（安全保存）、deploy/data 交易账本和 PostgreSQL 数据；SQLite 运行中备份使用 sqlite3 backup API，不仅复制 .db 文件。
本机原有 80/443、8443、发票服务及 WEEX 容器保持原运行方式，新服务仅发布 9443。
若外部访问不到 9443，检查云安全组 TCP 9443；可先用 SSH 隧道访问。

2026-10-13 从本地实测公网 9443 连接超时，服务器本机端口已监听，需进一步检查云安全组。
不用等待安全组修改，也可在自己的电脑运行：

```powershell
ssh -i C:\Users\10194\.ssh\password.pem -N -L 19443:127.0.0.1:9443 ubuntu@170.106.189.16
```

保持命令窗口打开，浏览器访问 `https://localhost:19443/quant`，使用 access.txt 中账号密码。
本地隧道地址与自签名证书 IP 不同，浏览器仍有证书提示。

部署脚本不会自动启用实盘，也不会创建远程交易所账户。

## 本次验收记录（2026-10-13）

- 本地 TypeScript 类型检查及 Vite 生产构建通过。
- 新增 18 项策略、订单、风控、私有 API 和邮件调度测试，在本地及服务器容器中通过。
- 五个部署容器运行，API 与 PostgreSQL 健康检查通过。
- 服务器本机 HTTPS：页面、状态、订单接口均 200；未登录访问量化接口为 401。
- 暂停/恢复接口验证通过，原暂停状态已恢复。
- QQ SMTP 465 端口 SSL 握手通过；未填写邮箱授权码，尚未发送真实邮件。
- 正式/测试币安合约均 HTTP 451；未发出真实订单，未验证真实保护单执行。
- 公网 9443 连接超时；云安全组待检查，SSH 隧道可作为访问入口。
- 看板使用服务器真实状态快照在本地只读预览中完成视觉检查。
- `.env` 与网页登录凭据文件均为 root 所有、权限 600。
- Nginx 动态解析 Docker 服务地址，避免 API 容器更新后仍连接旧地址。
