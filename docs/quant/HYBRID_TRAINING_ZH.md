# 本地优先训练，服务器兜底

行情采集、Redis、原模拟账本和模拟执行仍在服务器。Windows 电脑只做快照训练与回放，不启动交易进程，不接收币安/DeepSeek 密钥，不开放数据库公网端口。

本地用现有 OpenSSH 密钥认证，每 30 秒向服务器报告心跳。服务器时间和共享 SQLite 控制事务决定任务归属；每个任务租约 180 秒，运行期间续约，最多 4 小时。服务器 10 分钟未见本地心跳后，在任务到期且旧租约失效时接手。本地进程重叠由 Windows 文件锁阻止。

默认每 6 小时一轮。已经完成训练时，本地断网不会强制重复训练；服务器到下一轮再接手。训练失败后暂时停止本地心跳，避免电脑在线但程序持续出错时压住服务器兜底。

数据通过 SQLite online backup 生成一致性快照，包含 WAL 已提交数据；不能直接复制运行中的 .db 文件。事件目录使用冻结目录库和不可变 Parquet 文件的硬链接固定输入。DB 单独 gzip 压缩，Parquet 不重复压缩，打包后校验 SHA256，通过 SSH/SCP 传输。每轮仍为完整快照，首次及后续传输可能为 GB 级，暂未实现增量数据库同步。需要为快照保留临时磁盘余量。

本地使用服务器训练容器的冻结 Python 源码，不使用本地其他未部署策略改动。训练参数只导出不含密钥的研究配置，工作目录中不加载生产 .env。训练结果先写候选目录，过期任务不能回传覆盖新结果；未通过原有 walk-forward、逐事件验证与压力测试的模型只保存研究报告，不替换当前模型，不更改任何交易开关。服务器兜底也使用独立候选目录。

## 本地管理

配置文件位于仓库 `work/hybrid-training/config.json`，已在 Git 忽略范围。Python 使用本机 Python 3.12，依赖与服务器保持 scikit-learn 1.9.1、PyArrow 23.0.1。Windows 计划任务 `AI-Trader-LocalTraining` 在当前用户登录后启动，隐藏运行；电脑休眠或断网时由服务器规则兜底。

```powershell
# 查看本地后台日志与最近结果
Get-Content D:\aispea\jioayi1\AI-Trader\work\hybrid-training\worker.log -Tail 30
Get-Content D:\aispea\jioayi1\AI-Trader\work\hybrid-training\last-result.json
Get-Content D:\aispea\jioayi1\AI-Trader\work\hybrid-training\status.json

# 手动启动（重复进程会被文件锁拒绝）
powershell -NoProfile -ExecutionPolicy Bypass -File D:\aispea\jioayi1\AI-Trader\deploy\start-local-training.ps1
```

## 服务器管理

```bash
cd /home/ai-trader-quant/deploy
sudo docker compose -f compose.yaml -f hybrid.compose.yaml ps lab-trainer
sudo docker logs --tail 30 ai-trader-quant-lab-trainer-1
echo '{"action":"status"}' | sudo python3 hybrid_training.py rpc --lab /home/ai-trader-quant/deploy/data/lab
```

后续更新训练器必须带 `-f hybrid.compose.yaml`，否则会恢复旧的独立训练循环。避免同时运行旧的 `quant.lab_train --watch` 与新的兜底进程。容器重启本身会保留已部署命令与租约文件。

训练迁移只缓解服务器计算和内存争用，不证明策略盈利，也不会修复已经发生的历史行情缺口。
