import { useEffect, useState } from 'react'

type Signal = { signal_id: string; agent_id: string; agent_name: string; kind: string; symbol: string; symbols: string[]; side: string; title: string; content: string; created_at: string }
type Community = {
  enabled: boolean; source?: string; checked_at?: string; account_agent_id?: string; leaderboard_updated_at?: string
  feeds?: Record<string, Signal[]>; feed_updated_at?: Record<string, string>; totals?: Record<string, number>
  errors?: Record<string, string>; stale: Record<string, boolean>
  top_agents?: { agent_id: number; name: string; total_profit_percent: number; trade_count: number }[]
}
const names: Record<string, string> = { operation: '交易操作', strategy: '策略观点', discussion: '社区讨论', leaderboard: '排行榜' }
const sides: Record<string, string> = { buy: '买入 / 加多', sell: '卖出 / 减多', short: '开空', cover: '平空' }
const time = (value?: string) => value ? new Date(value).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '等待首次同步'

export function OfficialCommunityPage() {
  const [data, setData] = useState<Community | null>(null)
  const [error, setError] = useState('')
  const [kind, setKind] = useState('operation')
  const [mine, setMine] = useState(false)
  const refresh = async () => {
    try {
      const response = await fetch('/api/quant/community', { cache: 'no-store' })
      if (!response.ok) throw new Error(`社区数据读取失败（${response.status}）`)
      setData(await response.json()); setError('')
    } catch (err) { setError(String(err)) }
  }
  useEffect(() => { refresh(); const timer = setInterval(refresh, 30000); return () => clearInterval(timer) }, [])
  const signals = (data?.feeds?.[kind] || []).filter(row => !mine || row.agent_id === data?.account_agent_id)
  return <section className="official-community">
    <div className="official-header">
      <div><span className="official-eyebrow">AI4TRADE / PUBLIC COMMUNITY</span><h1>官方社区</h1><p>加密货币信号、策略观点与交易员动态 · 北京时间</p></div>
      <a href="https://ai4trade.ai/market?agent=22728" target="_blank" rel="noreferrer" className="btn btn-secondary">打开官方社区 ↗</a>
    </div>
    <div className="official-status" role="status">
      <strong>{!data ? '正在读取社区同步状态…' : data.enabled ? '官方公开数据已接入' : '社区同步未启用'}</strong>
      <span>每分钟同步 · 无需官方登录 · 仅供参考，社区操作不会直接下单</span>
    </div>
    {error && <p role="alert">{error}</p>}
    <div className="official-tabs">{Object.entries(names).map(([key, label]) => <button key={key} className={kind === key ? 'active' : ''} onClick={() => setKind(key)}>{label}</button>)}</div>
    {kind !== 'leaderboard' && <div className="official-feed-meta">
      <span>同步时间：{time(data?.feed_updated_at?.[kind])} · 最新 30 条 / 官方共 {data?.totals?.[kind]?.toLocaleString() ?? '—'} 条</span>
      <label><input type="checkbox" checked={mine} onChange={event => setMine(event.target.checked)} /> 仅看 Agent {data?.account_agent_id || '22728'}（最新记录内）</label>
    </div>}
    {data?.errors?.[kind] && <p role="alert" className="official-warning">{data.errors[kind]}；保留上次同步结果。</p>}
    {kind !== 'leaderboard' && data?.stale[kind] && <p className="official-warning">数据尚未同步或已超过 3 分钟，请勿将旧记录视为当前信号。</p>}
    {kind === 'leaderboard' ? <div className="official-card"><h2>官方模拟账户排行榜</h2><p>收益来自官方平台记录，不代表币安实盘收益；排名覆盖官方平台市场。</p><p>同步时间：{time(data?.leaderboard_updated_at)}</p>
      {(data?.top_agents || []).map((agent, index) => <div className="official-rank" key={agent.agent_id}><span>{String(index + 1).padStart(2, '0')}</span><strong>{agent.name}</strong><span>{Number(agent.total_profit_percent).toFixed(2)}%</span><small>{agent.trade_count} 笔</small></div>)}
      {!data?.top_agents?.length && <p>等待排行榜同步。</p>}
    </div> : <div className="official-feed">
      {signals.map(row => <article key={row.signal_id} className="official-card">
        <div className="official-card-meta"><strong>{row.agent_name}</strong><span>Agent {row.agent_id}</span><span>{time(row.created_at)}</span></div>
        <h2>{row.title || `${row.symbol || row.symbols.join(' / ')} · ${sides[row.side] || '交易操作'}`}</h2>
        <div className="official-tags">{[row.symbol, ...row.symbols].filter(Boolean).map((symbol, index) => <span key={`${symbol}-${index}`}>{symbol}</span>)}<span>{names[row.kind]}</span></div>
        <p className="official-content">{row.content || '暂无正文'}</p>
        <small>来源：ai4trade.ai · 信号 {row.signal_id}</small>
      </article>)}
      {!signals.length && <div className="official-card"><p>{mine ? '最近同步的记录中没有该 Agent 的内容。可取消筛选，或前往官方社区查看更早的记录。' : '等待社区内容同步。'}</p></div>}
    </div>}
  </section>
}
