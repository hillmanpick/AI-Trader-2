# 2026-10-14 迁移记录与新服务器操作说明

旧服务器：170.106.189.16。新服务器：124.156.199.59，Ubuntu，SSH 用户 ubuntu。
本机 `C:\Users\10194\.ssh\config` 添加 Host trade，密钥 `C:\Users\10194\.ssh\trade.pem`。

```powershell
ssh trade
```

## 新位置与入口

| 服务 | 入口/位置 |
|---|---|
| 量化看板 | https://124.156.199.59:8443/quant |
| 量化代码与部署 | /home/ai-trader-quant |
| 量化配置 | /home/ai-trader-quant/deploy/.env |
| 量化账本与邮件队列 | /home/ai-trader-quant/deploy/data |
| PostgreSQL 数据 | /home/ai-trader-quant/deploy/postgres-data |
| 网页登录账号密码 | /home/ai-trader-quant/deploy/access.txt |
| Xray | VLESS / TCP，124.156.199.59:443 |
| Xray 程序 | /home/xray/bin/xray |
| Xray 配置 | /home/xray/conf/config.json |
| Xray 导入链接 | /home/xray/node-info.txt |
| Xray 地理库 | /home/xray/share |
| Xray systemd 定义原本 | /home/xray/xray.service |
| 服务注册文件 | /etc/systemd/system/xray.service |
| 原服务器迁移备份 | /home/ai-trader-migration-backup |

HTTPS 采用针对新 IP 生成的自签名证书，浏览器会有证书提示；旧网页登录密码已保留。
Xray 保留原 VLESS 客户端身份、TCP 和未开启 TLS/REALITY 的传输配置，不把量化服务路由到 Xray。
节点地址变更到新 IP；用户客户端导入新的 node-info.txt 中链接，或将原节点服务器地址改为新 IP。

## 运行模式和填写凭据

迁移时沿用模拟模式（paper），不会下真实订单。默认 BTCUSDT/ETHUSDT、15m、SMA 10/30。
币安 API Key/Secret、QQ 发件邮箱/SMTP 授权码/收件邮箱在迁移时均为空，预留位置保留。

```bash
sudo nano /home/ai-trader-quant/deploy/.env
```

填写以下字段，不需要在聊天中发送：

```dotenv
BINANCE_API_KEY=
BINANCE_API_SECRET=
QQ_SMTP_USER=
QQ_SMTP_AUTH_CODE=
QQ_MAIL_TO=
```

仅填密钥不会启用实盘。配置更新后执行：

```bash
cd /home/ai-trader-quant/deploy
sudo docker compose up -d --force-recreate api quant web
```

邮件仍为北京时间开仓/清仓通知，08、10、12、14、16、18、20、22 点持仓报告；00:00–08:00 全部静默，夜间交易事件 08:00 后补发。
真实邮件投递仍待填写邮箱配置，真实交易/保护单执行仍待测试网验收及实盘配置。
原部署说明中的 170.106.189.16、/opt/ai-trader-quant、9443 是历史地址，日常使用本文件中的新地址。

## 验证与回退

服务文件、数据库归档与 Xray 二进制 SHA-256 与源端一致。
Docker 镜像直接加密传输，三个镜像 ID 与源端相同，没有重新升级依赖。
PostgreSQL 49 张表恢复后逐表校验记录数；单进程交易账本在停止写入、WAL checkpoint 后备份。
数据库的旧 Docker volume 保留为迁移初始快照；当前 PostgreSQL 使用 /home 下 bind mount。
临时 SSH 传输密钥仅能接收白名单迁移文件或加载镜像，完成后撤销授权并删除临时私钥。
旧服务器量化进程停止，Xray 停止并取消开机启动，旧文件保留用于回退。
如回退，应先停止新交易进程，再恢复旧端服务；新端已经产生的数据需另行备份同步，不能同时启动两台交易进程。

```bash
cd /home/ai-trader-quant/deploy
sudo docker compose ps
sudo docker compose logs --tail=80 quant api
sudo python3 verify.py
sudo systemctl status xray --no-pager
sudo journalctl -u xray --since '1 hour ago' --no-pager
```

查看凭据与节点（仅在自己的终端）：

```bash
sudo cat /home/ai-trader-quant/deploy/access.txt
sudo cat /home/xray/node-info.txt
```

本次结果：新服务器币安正式、测试合约 time 接口与正式 exchangeInfo 均 HTTP 200；量化进程 running；公网 8443 HTTPS 认证通过；Xray 443 实际 VLESS 转发请求得到 HTTP 200；QQ SMTP SSL 握手通过；18 项量化测试通过。
