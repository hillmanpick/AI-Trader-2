import { useEffect, useState } from 'react'
import { beijingTime } from './officialData'

const num = (v: any, digits = 2) => typeof v === 'number' && Number.isFinite(v) ? v.toLocaleString('zh-CN', { maximumFractionDigits: digits }) : '—'

export function QuantLab() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState('')
  useEffect(() => {
    let active = true
    const load = async () => {
      try {
        const r = await fetch('/api/quant/lab/status')
        if (!r.ok) throw new Error('研究系统状态读取失败')
        const value = await r.json()
        if (active) { setData(value); setError('') }
      } catch { if (active) setError('研究系统暂时无法连接') }
    }
    void load()
    const timer = setInterval(load, 5000)
    return () => { active = false; clearInterval(timer) }
  }, [])
  const c = data?.collector || {}, h = data?.history || {}, clean = h.clean_training || {}, m = data?.model || {}, req = data?.requirements || {}
  const reports = h.latest_report
  const eventHistory = data?.event_history || {}, ranking = data?.ranking?.rows || []
  const opening = data?.opening
  return <section className="card" style={{ padding: 22, marginBottom: 22, lineHeight: 1.8 }}>
    <h2>全市场研究与模型 <span className="tag">独立模拟 · 实盘只读</span></h2>
    {error && <p role="alert">{error}</p>}
    {!data ? <p>正在读取采集与模型状态…</p> : <>
      <p><strong>{data.enabled ? data.stale ? '采集心跳过期，禁止模型新仓' : '行情采集中' : '采集未启用'}</strong> · Redis {data.redis_connected ? '已连接' : '不可用'} · 全市场 USDT 永续 {num(c.universe_count, 0)} 个 · 候选 {c.candidates?.length || 0} / {req.candidate_limit} 个 · 特征就绪 {c.ready_symbols?.length || 0} 个</p>
      {opening && <div role="status"><p><strong>模拟开仓：{opening.entry_ready ? '门槛已通过，等待方向信号' : opening.armed ? '已待命，等待验证或数据恢复' : '总开关关闭或已暂停'}</strong> · {opening.automatic_after_validation ? '通过验证后自动允许检查开仓，无需手动恢复' : '当前配置不会自动恢复已验证模拟交易'}</p>
        {opening.blockers?.length > 0 && <ul>{opening.blockers.map((b: any) => <li key={b.code}>{b.reason}</li>)}</ul>}
        <p>门槛通过不代表立即成交；仍检查实时信号、成本、流动性和组合风险。</p></div>}
      <p>候选币：{c.candidates?.join('、') || '等待币安交易规则与成交额数据'}。币池每 5 分钟刷新；成交额、盘口深度、价差、数据质量和组合风险共同决定是否允许入场。</p>
      <p>距最近全市场行情 {num(c.ticker_age_ms, 0)} 毫秒 · 深度序号缺口 {num(c.depth_gaps, 0)} · 成交缺口 {num(c.trade_gaps, 0)} · 重连 {num(c.reconnects, 0)} · 原始记录丢弃 {num(c.raw_dropped, 0)}</p>
      {c.quote_pipeline && <p>连续报价覆盖 {c.observed_symbols?.length || 0} 个币 · 仅供退出观察 {c.exit_only_symbols?.length || 0} 个 · 订阅确认 {c.depth_subscriptions_confirmed?.length || 0} 个 · 等待确认 {num(c.depth_commands_pending, 0)} 条。特征预热期间仍记录有效报价；离池观察不能触发新仓。</p>}
      <p>全市场逐笔：{c.all_market_trades ? '已启用' : '候选采集'} · 已观察 {num(c.trade_symbols_seen, 0)} 个币 · 订阅分片 {Object.keys(c.trade_shards || {}).length} 个 · 事件排队 {num(c.event_queue, 0)} 条 · Parquet 保存 {num(eventHistory.rows, 0)} 条 / {num((eventHistory.bytes || 0) / 1024 / 1024)} MB</p>
      {c.archive_scope === 'candidate_trades_and_depth' && <p>历史归档：候选币逐笔成交和盘口；离池后继续保存标签观察期。其他币的逐笔流用于实时排序，不全量落盘。容量预算 {num((eventHistory.max_bytes || 0) / 1024 ** 3)} GiB，达到预算会淘汰旧分片；特征和标签单独保存。</p>}
      <p>ClickHouse：{eventHistory.clickhouse?.configured ? `已配置地址 · 本地检查点：已导出 ${num(eventHistory.clickhouse.exported_segments, 0)} / ${num(eventHistory.clickhouse.segments, 0)} 个保留分片 · 待导出 ${num(eventHistory.clickhouse.pending_segments, 0)}；不代表远端当前连通` : '未配置外部地址，Parquet 为当前主历史库'}</p>
      <p>全市场订单流排序就绪 {num(c.ranked_ready, 0)} 个币；未预热币种不会被标为有效预测。</p>
      {ranking.length > 0 && <details><summary>全市场排序前 20 名</summary><div className="table-container"><table className="table"><thead><tr><th>合约</th><th>状态</th><th>倾向</th><th>分数</th></tr></thead><tbody>{ranking.slice(0, 20).map((r: any) => <tr key={r.symbol}><td>{r.symbol}</td><td>{r.ready ? '订单流就绪' : '等待数据'}</td><td>{r.bias === 'long' ? '偏多' : r.bias === 'short' ? '偏空' : '—'}</td><td>{num(r.score, 4)}</td></tr>)}</tbody></table></div></details>}
      <p>原始特征 {num(h.samples, 0)} 条；可训练特征 {num(clean.samples, 0)} 条 · 可训练覆盖 {num(clean.days)} 天 · 每 {req.sample_seconds} 秒保存 · 历史保留 {req.retention_days} 天。训练至少需要 {num(req.min_samples, 0)} 条有效样本和 {req.min_days} 天；满足数量后仍须通过样本外回放。</p>
      {reports?.policy?.paper_margin_is_ceiling && <p>每笔保证金最多 {num(reports.policy.paper_margin)} U；按可成交盘口及组合风险缩小，禁止超出上限。缩小金额不豁免模型验证。</p>}
      <p>逐笔路径标签：有效 {num(h.event_labels?.valid, 0)} 条，缺口或过期无效 {num(h.event_labels?.invalid, 0)} 条。首次触及上下价格障碍决定方向，到期后的价格不会改变标签；完整观察结束后才可用于训练。</p>
      <p><strong>模型：{m.exploration ? '探索模型，尚未通过验证' : m.ready ? '历史验证通过，可供模拟使用' : '尚未就绪，禁止模型新仓'}</strong> · {m.reason} · 版本 {m.version || '—'} · 最近训练 {m.trained_ms ? beijingTime(new Date(m.trained_ms).toISOString()) : '—'}</p>
      {reports && <p>最近训练结果：{{ insufficient_data: '等待更多数据', insufficient_clean_labels: '有效逐笔标签不足', insufficient_event_holdout: '独立事件验证时段不足', rejected: '未通过样本外门槛', approved: '历史验证通过' }[reports.status as string] || reports.status} · {reports.at_ms ? beijingTime(new Date(reports.at_ms).toISOString()) : '—'}</p>}
      <p>最终模型另外保留两段各 {num((req.event_validation_seconds || 3600) / 60, 0)} 分钟的原始事件：验证段选阈值，测试段和压力测试不再调参。手续费、滑点、保证金或杠杆范围变化后须重新验证。</p>
      {reports?.execution_validation && <p>逐事件晋级：{reports.execution_validation.passed ? '通过，仅供模拟' : '未通过或证据不足'} · {reports.execution_validation.reason || reports.execution_validation.criteria} · 模型 {reports.execution_validation.model_version || '—'}</p>}
      {reports?.execution_validation?.test && <div className="table-container"><table className="table"><thead><tr><th>最终模型事件回放</th><th>已平仓笔数</th><th>扣费净收益</th><th>最大回撤</th><th>数据有效</th></tr></thead><tbody>{['validation', 'test', 'stress'].map((key) => { const r = reports.execution_validation[key]; return r && <tr key={key}><td>{{ validation: '验证段', test: '独立测试段', stress: '延迟与滑点压力测试' }[key]}</td><td>{num(r.trades, 0)}</td><td>{num(r.net_pnl)} U</td><td>{num(r.max_drawdown)} U</td><td>{r.valid ? '有效' : r.invalid_reasons?.join('、') || '无效'}</td></tr> })}</tbody></table></div>}
      {reports?.folds && <p>以下为早期采样研究窗口，不能单独作为模型启用依据：</p>}
      {reports?.folds && <div className="table-container"><table className="table"><thead><tr><th>时间窗口</th><th>测试笔数</th><th>扣费净收益</th><th>最大回撤</th><th>胜率</th><th>压力测试净收益</th></tr></thead><tbody>{reports.folds.map((f: any, i: number) => <tr key={i}><td>{i + 1}</td><td>{num(f.test.trades, 0)}</td><td>{num(f.test.net_pnl)} U</td><td>{num(f.test.max_drawdown)} U</td><td>{num(f.test.win_rate * 100)}%</td><td>{num(f.stress.net_pnl)} U</td></tr>)}</tbody></table></div>}
      {Object.entries(c.blocked || {}).length > 0 && <details><summary>数据未就绪原因</summary>{Object.entries(c.blocked).map(([s, why]) => <p key={s}>{s}：{String(why)}</p>)}</details>}
      <p>模型采用标准化特征、向量原型与逻辑回归，按时间隔离标签窗口。最终晋级使用真实历史盘口和逐笔成交，计入手续费、价差、额外不利滑点、模拟延迟、部分成交和保守资金费。历史结果不保证未来收益；模拟延迟不是交易所真实下单延迟。DeepSeek 用于低频分析，不参与每一帧行情处理。</p>
    </>}
  </section>
}
