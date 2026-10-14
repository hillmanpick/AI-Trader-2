import { useEffect, useState } from 'react'
import { beijingTime } from './officialData'

const fmt = (value: unknown, digits = 2) => value == null || !Number.isFinite(Number(value)) ? '—' : Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits })

export function PaperAccount() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState('')
  useEffect(() => {
    let active = true
    const load = async () => {
      try {
        const response = await fetch('/api/quant/paper/status')
        const body = await response.json()
        if (!response.ok) throw new Error('模拟账户读取失败')
        if (active) { setData(body); setError('') }
      } catch { if (active) setError('模拟账户暂时无法读取') }
    }
    void load(); const timer = setInterval(load, 15_000)
    return () => { active = false; clearInterval(timer) }
  }, [])
  const names: Record<string, string> = { running: '运行中', blocked: '运行受阻', paused_entries: '暂停开新仓', not_started: '尚未启动' }
  const actions: Record<string, string> = { long: '候选做多', short: '候选做空', close: '模拟平仓', hold: '等待信号' }
  return <section className="card" style={{ padding: 22, marginBottom: 22, lineHeight: 1.8 }}>
    <h2>独立模拟交易 <span className="tag">虚拟成交 · 实盘只读</span></h2>
    <p>使用币安公开行情、Qbot 均线策略和 DeepSeek 开仓审核。模拟进程没有币安密钥，不能向交易所提交订单。</p>
    {error && <p role="alert">{error}</p>}
    {!data ? <p>正在读取模拟账户…</p> : <>
      <p>状态：{data.stale ? '进程心跳过期' : names[data.worker.status] || data.worker.status} · 最后心跳 {beijingTime(data.worker.heartbeat)}（UTC+8）</p>
      <p>行情通道：{data.worker.market_stream?.status === 'connected' ? 'WebSocket 实时行情已收到' : data.worker.market_stream?.status === 'no_market_events' ? 'WebSocket 暂无行情帧，使用 REST 回退并重试' : data.worker.market_stream?.status === 'reconnecting' ? `WebSocket 重连中（${data.worker.market_stream.reconnects || 0} 次），使用 REST 回退` : '等待 WebSocket · REST 回退可用'} · 新鲜标记价 {data.worker.market_stream?.fresh_marks || 0}/{data.config.symbols.length}</p>
      {data.worker.error && <p role="alert" style={{ color: '#edac65' }}>{data.worker.error}</p>}
      <p>初始虚拟资金 10,000 USDT · 每笔 {fmt(data.config.order_notional)} USDT · 总上限 {fmt(data.config.max_notional)} USDT · 最多 {data.config.max_positions} 个仓位</p>
      <p>{data.config.symbols.join('、')} · {data.config.interval} K 线 · 均线 {data.config.fast}/{data.config.slow} · 模拟止损 {data.config.stop_pct * 100}% / 止盈 {data.config.take_pct * 100}% · 每日亏损上限 {fmt(data.config.daily_loss)} USDT</p>
      <p>虚拟权益 {fmt(data.account.equity)} USDT · 虚拟可用 {fmt(data.account.available)} USDT · {data.daily_loss_blocked ? '每日亏损限制已触发，停止开新仓' : '等待策略信号，通过 AI 审核后才模拟成交'}</p>
      <p>启动时不补做历史信号，不强制开仓。模拟费用已计入，未模拟资金费、滑点及强平。</p>
      {Object.entries(data.positions || {}).length ? <div className="table-container"><table className="table"><thead><tr><th>合约</th><th>方向</th><th>数量</th><th>入场价</th><th>标记价</th><th>模拟浮盈亏</th></tr></thead><tbody>{Object.entries(data.positions).map(([symbol, raw]) => { const p = raw as any; return <tr key={symbol}><td>{symbol}</td><td>{p.amount > 0 ? '多' : '空'}</td><td>{fmt(Math.abs(p.amount), 8)}</td><td>{fmt(p.entry, 6)}</td><td>{fmt(p.mark, 6)}</td><td>{fmt(p.pnl, 4)} USDT</td></tr> })}</tbody></table></div> : <p>独立模拟账户当前空仓。</p>}
      <h3>模拟信号与 AI 审核</h3>
      {data.config.symbols.map((symbol: string) => <article key={symbol}><strong>{symbol} · {actions[data.signals?.[symbol]?.action] || '等待下一根收盘 K 线'}</strong>{data.ai?.[symbol] && <p>{data.ai[symbol].decision === 'allow' ? 'AI 放行' : 'AI 未放行'}：{data.ai[symbol].reason} · {beijingTime(data.ai[symbol].at)}</p>}</article>)}
      <h3>最近模拟事件</h3>
      {(data.events || []).slice(0, 8).map((event: any) => <article key={event.id}><strong>{event.kind}</strong> · {beijingTime(event.created_at)}<p style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{event.body}</p></article>)}
      {!data.events?.length && <p>暂无模拟成交或事件。</p>}
    </>}
  </section>
}
