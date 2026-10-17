import { useEffect, useState } from 'react'
import { beijingTime } from './officialData'
import { formatLeverage } from './positionFormat'

const fmt = (value: unknown, digits = 2) => value == null || !Number.isFinite(Number(value)) ? '—' : Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits })

export function LiveAccount() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState('')
  useEffect(() => {
    let active = true
    const load = async () => {
      try {
        const response = await fetch('/api/quant/live-account')
        const value = await response.json()
        if (!response.ok) throw new Error(value.detail || '实盘读取失败')
        if (active) { setData(value); setError('') }
      } catch { if (active) setError('实盘账户暂时无法读取，请检查连接') }
    }
    void load(); const timer = setInterval(load, 15_000)
    return () => { active = false; clearInterval(timer) }
  }, [])
  const account = data?.account
  return <section className="card live-account" style={{ padding: '22px', marginBottom: '22px', lineHeight: 1.8 }}>
    <h2>币安 USDT 合约实盘账户 <span className="tag">只读观察 · 自动交易关闭</span></h2>
    <p>此处读取币安正式账户，包含手动仓位和双向仓位。系统不接管持仓、不下单、不撤单、不调整杠杆或保证金。</p>
    {(error || data?.error) && <p role="alert" style={{ color: '#edac65' }}>{error || data.error}</p>}
    {!data ? <p>正在读取实盘账户…</p> : <>
      {!data.credentials_configured && <p>等待填写币安 API Key 和 Secret。</p>}
      {!data.observer_enabled && <p>只读观察进程尚未启用。</p>}
      {data.stale && <p role="status" style={{ color: '#edac65' }}>实盘数据尚未同步或已超过 90 秒；以下若有余额和仓位，为上次成功快照。</p>}
      <small>最后成功同步：{beijingTime(data.updated_at)}（UTC+8）</small>
      <p>账户通道：{data.account_stream?.status === 'connected' ? 'WebSocket 实时账户同步' : data.account_stream?.status === 'reconnecting' ? '账户 WebSocket 重连中，REST 校准中' : data.account_stream?.status === 'stopped' ? '账户 WebSocket 已停止，REST 校准中' : '账户 WebSocket 正在启动，REST 校准可用'}</p>
      {account && <>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(140px,1fr))', gap: '14px', margin: '20px 0' }}>
          {[['钱包余额', account.wallet], ['账户权益', account.equity], ['可用余额', account.available], ['未实现盈亏', account.unrealized], ['已用初始保证金', account.initial_margin], ['维持保证金', account.maintenance_margin]].map(([name, value]) => <div key={name as string}><small style={{ color: 'var(--text-secondary)' }}>{name}</small><div style={{ fontSize: '20px', fontWeight: 600 }}>{fmt(value)} <small>USDT</small></div></div>)}
        </div>
        <p>维持保证金 / 权益：{account.margin_ratio == null ? '—' : `${(account.margin_ratio * 100).toFixed(2)}%`} · 当前持仓 {(data.positions || []).length} 项</p>
        {data.positions?.some((p: any) => formatLeverage(p.leverage) === '未提供') && <p>部分仓位的杠杆暂未获取，显示“未提供”。</p>}
        {data.positions?.length ? <div className="table-container"><table className="table"><thead><tr><th>合约</th><th>持仓方向</th><th>杠杆</th><th>持仓初始保证金</th><th>数量</th><th>入场价</th><th>标记价</th><th>浮动盈亏</th><th>强平价</th></tr></thead><tbody>{data.positions.map((p: any) => <tr key={`${p.symbol}-${p.position_side}`}><td>{p.symbol}</td><td>{p.amount > 0 ? '多' : '空'} · {p.position_side}</td><td><strong>{formatLeverage(p.leverage)}</strong></td><td>{fmt(p.initial_margin)} USDT</td><td>{fmt(Math.abs(p.amount), 8)}</td><td>{fmt(p.entry, 6)}</td><td>{fmt(p.mark, 6)}</td><td style={{ color: p.pnl >= 0 ? 'var(--success)' : 'var(--error)' }}>{fmt(p.pnl, 4)} USDT</td><td>{p.liquidation > 0 ? fmt(p.liquidation, 6) : '未提供'}</td></tr>)}</tbody></table></div> : <p>币安实盘账户当前空仓。</p>}
      </>}
    </>}
  </section>
}
