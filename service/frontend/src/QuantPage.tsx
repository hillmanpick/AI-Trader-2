import { useEffect, useState } from 'react'
import { LiveAccount } from './LiveAccount'
import { PaperAccount } from './PaperAccount'
import { formatLeverage } from './positionFormat'

type Position = { amount: number; entry: number; mark: number; pnl: number; leverage?: number }
type Snapshot = {
  config: { observation_only: boolean; ai_advice_seconds: number; execution_enabled: boolean; mode: string; strategy: string; interval: string; symbols: string[]; order_notional: number; max_notional: number; leverage: number; stop_pct: number; take_pct: number; credentials_configured: boolean; mail_configured: boolean; ai_enabled: boolean; ai_configured: boolean; ai_model: string; community_enabled: boolean }
  worker: { status: string; error?: string; heartbeat?: string; market_stream?: { status?: string; reconnects?: number; fresh_marks?: number } }
  stale: boolean; paused: boolean; daily_loss_blocked: boolean
  ai_advice?: { status: string; at?: string; completed_at?: string; reason?: string; model?: string; symbols?: Record<string, { status: string; bias: string; confidence: number; reason: string; risk: string; price: number; rule_candidate: string }> }
  ai_advice_job?: { status?: string }
  account: { equity?: number; available?: number }
  positions: Record<string, Position>
  mail: { ok?: boolean; error?: string }
  events: { id: number; kind: string; body: string; created_at: string; mailed_at?: string }[]
  ai?: Record<string, { status: string; decision?: string; reason: string; confidence?: number; action?: string; at?: string } | null>
}

export function QuantPage() {
  const [data, setData] = useState<Snapshot | null>(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [aiTest, setAiTest] = useState('')
  const refresh = async () => {
    try {
      const r = await fetch('/api/quant/status')
      if (!r.ok) throw new Error(`读取失败 (${r.status})；请通过受保护的服务器入口访问`)
      setData(await r.json()); setError('')
    } catch (e) { setError(String(e)) }
  }
  useEffect(() => { refresh(); const timer = setInterval(refresh, 15000); return () => clearInterval(timer) }, [])
  const action = async (name: string) => {
    setBusy(true)
    try {
      const r = await fetch(`/api/quant/${name}`, { method: 'POST', headers: { 'X-Quant-Action': 'operator' } })
      if (!r.ok) throw new Error((await r.json()).detail || '操作失败')
      await refresh()
    } catch (e) { setError(String(e)) } finally { setBusy(false) }
  }
  const time = (s?: string) => s ? new Date(s).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '—'
  const testAI = async () => {
    setBusy(true); setAiTest('正在测试连接…')
    try {
      const response = await fetch('/api/quant/ai-test', { method: 'POST', headers: { 'X-Quant-Action': 'operator' } })
      const result = await response.json()
      if (!response.ok) throw new Error(result.detail || '连接失败')
      setAiTest(result.ok ? 'DeepSeek 连接及 JSON 响应验证成功（未执行交易）' : result.reason)
    } catch (err) { setAiTest(String(err)) } finally { setBusy(false) }
  }
  const box = { padding: '20px', border: '1px solid var(--border-color, #394052)', borderRadius: '12px', marginBottom: '20px' }
  const button = { padding: '10px 16px', borderRadius: '8px', border: '1px solid #b8954b', background: '#b8954b22', color: 'inherit', cursor: 'pointer', margin: '8px 0' }
  const statusNames: Record<string, string> = { observing: '只读观察运行中', observation_error: '实盘同步异常', blocked: '运行受阻', running: '运行中', paused_entries: '已暂停开新仓', not_started: '尚未启动' }
  return <section>
    <h1>币安 USDT 合约量化</h1>
    <p>Qbot 策略 · DeepSeek 分析 · 官方社区参考 · 北京时间</p>
    {error && <p role="alert" style={{ color: '#f87171' }}>{error}</p>}
    {!data ? <p>正在读取运行状态…</p> : <>
      <div style={box}>
        <h2>{data.config.observation_only ? '只读观察 · 自动交易关闭' : data.config.mode === 'paper' ? '模拟盘' : data.config.mode === 'testnet' ? '币安测试网' : '币安实盘'} · {data.stale ? '进程心跳异常' : statusNames[data.worker.status] || data.worker.status}</h2>
        {data.worker.error && <p role="alert" style={{ color: '#f87171' }}>{data.worker.error.includes('451') ? '币安限制此服务器所在地区访问（HTTP 451），当前无法获取行情或下单。' : data.worker.error}</p>}
        <p>策略：{data.config.strategy} / {data.config.interval}　币种：{data.config.symbols.join('、')}</p>
        {!data.config.observation_only && <><p>策略单笔名义金额：{data.config.order_notional} USDT　总额上限：{data.config.max_notional} USDT　策略杠杆：{data.config.leverage} 倍</p>
        <p>止损：{data.config.stop_pct * 100}%　止盈：{data.config.take_pct * 100}%　每日亏损限制：{data.daily_loss_blocked ? '已触发' : '未触发'}</p></>}
        {!data.config.observation_only && <p>权益：{data.account.equity?.toFixed(2) ?? '—'} USDT　可用：{data.account.available?.toFixed(2) ?? '—'} USDT</p>}
        <p>最后心跳：{time(data.worker.heartbeat)}</p>
        {data.worker.market_stream && <p>行情通道：{data.worker.market_stream.status === 'connected' ? 'WebSocket 实时行情已收到' : data.worker.market_stream.status === 'no_market_events' ? 'WebSocket 暂无行情帧，使用 REST 回退并重试' : data.worker.market_stream.status === 'reconnecting' ? `WebSocket 重连中（${data.worker.market_stream.reconnects || 0} 次），使用 REST 回退` : '等待 WebSocket · REST 回退可用'} · 新鲜标记价 {data.worker.market_stream.fresh_marks || 0}/{data.config.symbols.length}</p>}
        {!data.config.observation_only && <button style={button} disabled={busy} onClick={() => action(data.paused ? 'resume' : 'pause')}>{data.paused ? '恢复开新仓' : '暂停开新仓'}</button>}
        {data.config.observation_only ? <p>实盘仅同步数据和展示 AI 建议。实盘开仓、平仓、止盈止损和账户参数修改全部关闭；独立模拟交易见下方。</p> : <p>暂停后，已有持仓的止盈止损继续运行。此按钮不会切换模拟/实盘模式。</p>}
      </div>
      <LiveAccount />
      <PaperAccount />
      <div style={box}>
        <h2>DeepSeek AI 判断</h2>
        <p className="quant-ai-info">{data.config.ai_enabled ? data.config.ai_configured ? (data.config.observation_only ? 'AI 分析已启用，仅给建议，不执行交易' : 'AI 开仓审核已启用') : '等待填写 DS 密钥：当前暂不开新仓' : 'AI 审核未启用，使用规则策略'} · 模型：{data.config.ai_model || 'deepseek-chat'}</p>
        <p className="quant-ai-info">{data.config.observation_only ? `已收盘行情 + Qbot 规则 + 社区参考 → DeepSeek 给出方向和风险建议；约每 ${Math.round(data.config.ai_advice_seconds / 60)} 分钟分析一次。AI 无下单工具，当前建议不会执行。` : '均线 / 布林带产生候选信号 → DeepSeek 参考近期社区观点审核 → 固定风控 → 执行。止盈、止损和平仓持续运行，AI 无法修改金额或杠杆。'}</p>
        <p className="quant-ai-info">官方社区：{data.config.community_enabled ? '已启用同步' : '未启用'} · <a href="/community">查看社区信号与策略</a></p>
        {!data.config.ai_configured && <p className="quant-ai-info">填写服务器 .env 中的 DEEPSEEK_API_KEY 后重新加载服务，即可启用判断。</p>}
        <button style={button} disabled={busy || !data.config.ai_configured} onClick={testAI}>测试 DeepSeek 连接</button>
        {aiTest && <p role="status">{aiTest}</p>}
        {data.config.observation_only && <>
          <p>最近分析：{time(data.ai_advice?.completed_at || data.ai_advice?.at)} · {data.ai_advice_job?.status === 'pending' ? '正在分析…' : '等待下一轮'}</p>
          {data.ai_advice?.at && Date.now() - new Date(data.ai_advice.at).getTime() > data.config.ai_advice_seconds * 2000 && <p style={{ color: '#edac65' }}>此分析已过期，请等待新一轮；不作为当前行情判断。</p>}
          {data.ai_advice?.reason && <p>{data.ai_advice.reason}</p>}
          {Object.entries(data.ai_advice?.symbols || {}).map(([symbol, a]) => <article className="quant-ai-decision" key={symbol}><strong>{symbol} · {{ wait: '观望', bullish: '偏多', bearish: '偏空', reduce_risk: '降低风险' }[a.bias] || '未形成分析'}（仅建议）</strong><p>{a.reason}</p><p>风险：{a.risk}</p><small>置信度 {(a.confidence * 100).toFixed(0)}% · 参考价格 {a.price} · 规则信号 {a.rule_candidate}</small></article>)}
          {!Object.keys(data.ai_advice?.symbols || {}).length && <p>正在准备首次市场分析；账户同步成功后开始。</p>}
        </>}
        {!data.config.observation_only && Object.entries(data.ai || {}).map(([symbol, result]) => result && <div className="quant-ai-decision" key={symbol}><strong>{symbol} · {result.status === 'pending' ? '审核中' : result.decision === 'allow' ? '审核放行' : '未放行'}</strong><p>{result.reason}</p><small>{time(result.at)}{result.confidence !== undefined ? ` · 置信度 ${(result.confidence * 100).toFixed(0)}%` : ''}</small></div>)}
        {!data.config.observation_only && !Object.values(data.ai || {}).some(Boolean) && <p className="quant-ai-info">暂无审核记录，等待规则策略产生候选开仓信号。</p>}
      </div>
      <div style={box}>
        <h2>{data.config.observation_only ? '历史模拟持仓（已冻结）' : '实际交易进程持仓'}</h2>
        <p>{data.config.observation_only ? '保留原模拟账本，仅作历史参考；此处金额不是实盘余额，也不再执行止盈止损。' : '此处独立于原平台的社区模拟持仓。'}</p>
        {Object.keys(data.positions).length === 0 ? <p>当前空仓</p> : Object.entries(data.positions).map(([s, p]) => <p key={s}><strong>{s} {p.amount > 0 ? '多' : '空'}</strong>　杠杆 {formatLeverage(p.leverage)}　数量 {Math.abs(p.amount)}　入场 {p.entry}　标记 {p.mark}　浮盈亏 {p.pnl.toFixed(2)} USDT</p>)}
      </div>
      <div style={box}>
        <h2>QQ 邮件通知</h2>
        <p>{data.config.mail_configured ? '邮箱已配置' : '等待填写发件邮箱、SMTP 授权码和收件邮箱'} · 币安密钥：{data.config.credentials_configured ? '已配置' : '待填写'}</p>
        <p>开仓、清仓通知；08:00 起每两小时持仓报告。00:00–08:00 静默，交易事件在 08:00 后补发。观察模式只监测实盘账户变化，不下单。</p>
        {data.mail.error && <p>{data.mail.error}</p>}
        <button style={button} disabled={busy} onClick={() => action('mail-test')}>排队发送测试邮件</button>
      </div>
      <div style={box}>
        <h2>最近事件</h2>
        {data.events.map(e => <article key={e.id} style={{ borderBottom: '1px solid #5555', padding: '12px 0' }}><strong>{e.kind}</strong>　<small>{time(e.created_at)} · {e.mailed_at ? '邮件已发送' : '邮件待发送'}</small><p style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{e.body}</p></article>)}
      </div>
    </>}
  </section>
}
