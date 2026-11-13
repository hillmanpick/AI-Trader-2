import { useEffect, useState } from 'react'
import { beijingTime } from './officialData'

const number = (n: any, digits = 2) => typeof n === 'number' && Number.isFinite(n) ? n.toLocaleString('zh-CN', { maximumFractionDigits: digits }) : '—'
const pct = (n: any) => typeof n === 'number' && Number.isFinite(n) ? `${number(n * 100)}%` : '—'

export function PotentialRadar() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState('')
  useEffect(() => {
    let active = true
    const load = async () => {
      try {
        const response = await fetch('/api/quant/radar/status')
        if (!response.ok) throw new Error('雷达读取失败')
        const value = await response.json()
        if (active) { setData(value); setError('') }
      } catch { if (active) setError('潜力币雷达暂时无法连接') }
    }
    void load()
    const timer = setInterval(load, 15000)
    return () => { active = false; clearInterval(timer) }
  }, [])
  const rows = data?.candidates || []
  const stale = !data || data.stale
  const verified = data?.mode === 'validated_probability'
  return <section className="card" style={{ padding: 22, marginBottom: 22, lineHeight: 1.8 }}>
    <h2>潜力币雷达 <span className="tag">观察排名 · 不自动下单</span></h2>
    <p>筛选未来几小时可能走强的币，覆盖币安已上市 USDT 永续。分数表示信号共振程度，不能保证上涨；不覆盖尚未上市的链上新币。</p>
    {error && <p role="alert">{error}</p>}
    {!data ? <p>正在读取候选…</p> : <>
      <p><strong>{stale ? '观察心跳已过期，请勿使用旧排名' : data.status === 'running' ? '雷达运行中' : data.reason || '等待行情与历史数据'}</strong> · 最近更新 {data.ts ? beijingTime(new Date(data.ts).toISOString()) : '—'} · 实时成交流就绪 {number(data.universe_ready, 0)} 个 · 流动性筛选后 {number(data.liquid_universe, 0)} 个 · 本次补充历史 {number(data.enriched_candidates, 0)} 个</p>
      <p>{verified ? '概率模型已通过历史排序独立测试，仍不授权交易。' : '当前使用可解释规则分数，不显示未经验证的上涨概率。'} {data.model?.reason}</p>
      {rows.length > 0 ? <div className="table-container"><table className="table"><thead><tr><th>候选</th><th>{verified ? '4 小时先涨 10% 概率' : '信号分数 / 100'}</th><th>1 小时涨幅</th><th>强于 BTC</th><th>15 分钟量能倍数</th><th>持仓量 1 小时</th><th>资金费率</th><th>现货 1 小时</th><th>信号与风险</th></tr></thead><tbody>{rows.map((row: any) => <tr key={row.symbol} style={{ opacity: stale ? 0.5 : 1 }}><td><strong>{row.symbol}</strong><br />{number(row.price, 8)} U<br /><small>{beijingTime(new Date(row.ts).toISOString())}</small></td><td>{verified ? pct(row.prediction) : number(row.score)}</td><td>{pct(row.features.return_1h)}</td><td>{pct(row.features.relative_btc_1h)}</td><td>{number(row.features.volume_multiple)}×</td><td>{row.features.oi_available ? pct(row.features.oi_change_1h) : '未取得'}</td><td>{row.features.funding_available ? pct(row.features.funding_rate) : '未取得'}</td><td>{row.features.spot_available ? pct(row.features.spot_return_1h) : '无可比数据'}</td><td><details><summary>{row.reasons[0]} · {row.risks.length} 项待注意</summary>{row.reasons.map((s: string) => <p key={s}>{s}</p>)}{row.risks.map((s: string) => <p key={s}>注意：{s}</p>)}</details></td></tr>)}</tbody></table></div> : <p>暂无完成数据检查的候选。历史补查采用限速队列，完成后自动展示。</p>}
      <p>已记录 {number(data.observations, 0)} 个币种小时快照 · 等待完整 4 小时路径 {number(data.pending, 0)} 个 · 有效结果 {number(data.valid_outcomes, 0)} 个 · 路径无法确定 {number(data.invalid_outcomes, 0)} 个 · 先触及上涨目标 {number(data.up_first, 0)} 个。雷达数据库约 {number((data.database_bytes || 0) / 1024 ** 2)} MB。</p>
      <p>训练：{{ insufficient_forward_labels: '有效前向标签不足，继续记录', approved_for_radar: '已通过历史排序独立测试', rejected: '未通过独立测试，保留规则分数', error: '训练暂不可用，规则观察继续' }[data.training?.status as string] || '等待首次数据检查'}。样本来自候选与轮换对照币，不能代表全市场的无偏命中率。</p>
      <details><summary>候选后来涨了吗</summary><p>假设在记录后的下一个 5 分钟边界开盘观察，统计 4 小时内先上涨 10%、先下跌 5% 或到期；同根 K 线同时触及上下障碍的样本判为无法确定。净变化计入假设双边费用和不利滑点，不是模拟账户盈亏。</p>{data.recent_outcomes?.length ? <div className="table-container"><table className="table"><thead><tr><th>币种</th><th>记录时间</th><th>原始分数</th><th>后续结果</th><th>假设扣费变化</th><th>全窗口最大不利幅度</th></tr></thead><tbody>{data.recent_outcomes.map((r: any) => <tr key={`${r.symbol}-${r.ts}`}><td>{r.symbol}</td><td>{beijingTime(new Date(r.ts).toISOString())}</td><td>{number(r.score)}</td><td>{!r.outcome.valid ? '路径无法确定' : ({ up_first: '先触及 +10%', down_first: '先触及 -5%', timeout: '4 小时到期' }[r.outcome.outcome as string] || '—')}</td><td>{pct(r.outcome.net_return)}</td><td>{pct(r.outcome.max_drawdown)}</td></tr>)}</tbody></table></div> : <p>尚无完整的后续观察结果。</p>}</details>
    </>}
  </section>
}
