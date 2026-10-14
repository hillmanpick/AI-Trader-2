import { LiveAccount } from './LiveAccount'
import { PaperAccount } from './PaperAccount'
import { useEffect, useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { beijingTime, communityFetch, type OfficialState } from './officialData'
import './OfficialSections.css'

export function OfficialSourceBanner() {
  const location = useLocation()
  const [states, setStates] = useState<Record<string, OfficialState>>({})
  useEffect(() => {
    setStates({})
    const receive = (event: Event) => {
      const detail = (event as CustomEvent<OfficialState>).detail
      if (detail?.path) setStates(prev => ({ ...prev, [detail.path]: detail }))
    }
    window.addEventListener('official-data', receive)
    return () => window.removeEventListener('official-data', receive)
  }, [location.pathname])
  const values = Object.values(states)
  const error = values.find(s => s.error)?.error
  const stale = values.some(s => s.stale || s.warning)
  const fetched = values.map(s => s.fetched_at).filter(Boolean).sort().at(-1)
  const old = values.filter(s => s.data_as_of && Date.now() - new Date(s.data_as_of).getTime() > 72 * 3600_000)
  return <aside className="official-source" aria-label="数据来源">
    <div><strong>AI4Trade 官方公开数据</strong><span className="official-pill">只读 · 社区模拟交易</span><a href={`https://ai4trade.ai${location.pathname}${location.search}`} target="_blank" rel="noreferrer">打开官方站点 ↗</a></div>
    <p>社区收益和持仓来自官方模拟账户；币安账户与量化进程请查看 <Link to="/quant">币安合约量化</Link>。时间统一为北京时间（UTC+8）。</p>
    {fetched && <small>最近同步：{beijingTime(fetched)}</small>}
    {old.length > 0 && <p className="official-warning" role="status">官方资料超过 72 小时未更新，以下为历史快照。资料日期：{beijingTime(old.map(s => s.data_as_of!).sort()[0])}。页面同步时间不代表行情更新时间。</p>}
    {stale && <p className="official-warning" role="status">官方连接异常或缓存已过期，正在展示上次成功同步的数据。</p>}
    {error && <p className="official-warning" role="alert">{error}。请刷新重试；本页空白不代表官方没有记录。</p>}
  </aside>
}

const number = (value: unknown, digits = 2) => value == null || !Number.isFinite(Number(value)) ? '未提供' : Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits })

export function ConnectedPositionsPage() {
  const [source, setSource] = useState<'live' | 'quant' | 'official'>('live')
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)
  const [page, setPage] = useState(1)
  useEffect(() => {
    if (source !== 'official') return
    let active = true
    const load = async () => {
      try {
        let response: Response
        {
          const cfgResponse = await fetch('/api/quant/community')
          if (!cfgResponse.ok) throw new Error('读取社区配置失败')
          const cfg = await cfgResponse.json()
          if (!/^\d+$/.test(cfg.account_agent_id || '')) throw new Error('尚未配置官方 Agent ID')
          response = await communityFetch(`/api/agents/${cfg.account_agent_id}/positions`)
        }
        const body = await response.json()
        if (!response.ok) throw new Error(body.detail || '持仓读取失败')
        if (active) { setData(body); setError('') }
      } catch (err) { if (active) setError(String(err)) }
      finally { if (active) setLoading(false) }
    }
    setData(null); setError(''); setLoading(true); setPage(1); void load()
    const timer = setInterval(load, 30_000)
    return () => { active = false; clearInterval(timer) }
  }, [source])
  const rows: any[] = data?.positions || []
  return <section>
    <h1>持仓总览</h1><p className="official-muted">分别查看币安实盘、独立模拟交易和官方 Agent 的模拟持仓。</p>
    <div className="market-tabs"><button className={`market-tab ${source === 'live' ? 'active' : ''}`} onClick={() => setSource('live')}>币安实盘 · 只读</button><button className={`market-tab ${source === 'quant' ? 'active' : ''}`} onClick={() => setSource('quant')}>独立模拟交易</button><button className={`market-tab ${source === 'official' ? 'active' : ''}`} onClick={() => setSource('official')}>官方 Agent 模拟持仓</button></div>
    {source === 'official' && <OfficialSourceBanner />}
    {source === 'live' ? <LiveAccount /> : source === 'quant' ? <PaperAccount /> : <>
    {error && <p className="official-warning" role="alert">{error}</p>}
    {loading ? <p>正在读取持仓…</p> : data && <>
      <div className="card official-position-summary"><h2>{data.agent_name} · 官方模拟账户</h2>
        <p>模拟现金 ${number(data.cash)} · {rows.length} 个持仓</p>
        <p>官方缺失的价格和盈亏显示“未提供”，不按 0 计算。</p>
      </div>
      {rows.length ? <div className="card table-container"><table className="table"><thead><tr><th>标的</th><th>方向</th><th>数量</th><th>入场价格</th><th>当前价格</th><th>浮动盈亏</th></tr></thead><tbody>{rows.slice((page - 1) * 20, page * 20).map((row, i) => <tr key={i}><td>{row.display_title || row.symbol}<small className="official-market">{row.market}</small></td><td>{row.side === 'long' ? '多' : '空'}</td><td>{number(row.quantity, 8)}</td><td>{number(row.entry_price, 4)}</td><td>{number(row.current_price, 4)}</td><td>{number(row.pnl, 4)}</td></tr>)}</tbody></table>{rows.length > 20 && <div className="official-position-pagination"><button className="btn btn-ghost" disabled={page <= 1} onClick={() => setPage(p => p - 1)}>上一页</button><span>第 {page} / {Math.ceil(rows.length / 20)} 页 · 共 {rows.length} 项</span><button className="btn btn-ghost" disabled={page >= Math.ceil(rows.length / 20)} onClick={() => setPage(p => p + 1)}>下一页</button></div>}</div> : <div className="empty-state">当前数据源暂无持仓</div>}
    </>}
    </>}
  </section>
}

export function OfficialAccountSection({ kind }: { kind: 'copytrading' | 'trade' | 'exchange' }) {
  const [agents, setAgents] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  useEffect(() => {
    if (kind !== 'copytrading') return
    let active = true
    setLoading(true)
    communityFetch('/api/profit/history?limit=10&include_history=false').then(async res => {
      if (res.ok && active) setAgents((await res.json()).top_agents || [])
    }).catch(() => {}).finally(() => { if (active) setLoading(false) })
    return () => { active = false }
  }, [kind])
  const title = kind === 'copytrading' ? '跟单观察' : kind === 'trade' ? '交易入口' : '社区积分兑换'
  return <section><h1>{title}</h1><div className="card official-position-summary">
    <p>{kind === 'copytrading' ? '可查看官方交易员并研究公开操作；你的官方关注列表属于私有账户资料，当前未接入。' : kind === 'trade' ? '币安 USDT 合约策略、运行状态和风控在量化控制台管理。官方社区下单属于另一个模拟账户。' : '积分、兑换记录属于官方登录账户资料，公开接口不提供这些数据。'}</p>
    <p>本地登录与官网登录相互独立。在这里查看社区数据不会创建跟单或下单。</p>
    <div className="official-links"><Link to="/quant" className="btn btn-primary">币安量化控制台</Link><a className="btn btn-ghost" href={`https://ai4trade.ai/${kind}`} target="_blank" rel="noreferrer">到官方站点{kind === 'copytrading' ? '管理跟单' : kind === 'trade' ? '模拟交易' : '查看积分'} ↗</a></div>
  </div>{kind === 'copytrading' && <><h2>官方公开交易员</h2>{loading ? <p>正在读取…</p> : agents.map(agent => <Link className="official-agent-row card" key={agent.agent_id} to={`/market?agent=${agent.agent_id}`}><strong>{agent.name}</strong><span>模拟收益率 {number(agent.total_profit_percent)}% · {agent.trade_count} 笔交易 →</span></Link>)}</>}</section>
}
