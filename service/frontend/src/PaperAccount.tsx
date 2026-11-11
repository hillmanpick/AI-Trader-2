import { useEffect, useState } from 'react'
import { beijingTime } from './officialData'
import { formatLeverage } from './positionFormat'

const fmt = (value: unknown, digits = 2) => value == null || !Number.isFinite(Number(value)) ? '—' : Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits })
const signedFmt = (value: unknown, digits = 2) => {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  const amount = Number(value)
  return `${amount > 0 ? '+' : ''}${amount.toLocaleString('zh-CN', { maximumFractionDigits: digits })}`
}

export function PaperAccount() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState('')
  const [historyOpen, setHistoryOpen] = useState(false)
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
  const actions: Record<string, string> = { long: '做多', short: '做空', close: '平仓', hold: '观望', update: '调整保护参数' }
  const positions = Object.values(data?.positions || {}) as { initial_margin?: number }[]
  const totalMargin = positions.every(p => typeof p.initial_margin === 'number' && Number.isFinite(p.initial_margin) && p.initial_margin >= 0)
    ? positions.reduce((sum, p) => sum + p.initial_margin!, 0) : null
  return <section className="card" style={{ padding: 22, marginBottom: 22, lineHeight: 1.8 }}>
    <h2>独立模拟交易 <span className="tag">虚拟成交 · 实盘只读</span></h2>
    <p>使用币安公开行情与规则或数值模型。模拟进程没有币安密钥，不能向交易所提交订单。</p>
    {error && <p role="alert">{error}</p>}
    {!data ? <p>正在读取模拟账户…</p> : <>
      <p>状态：{data.stale ? '进程心跳过期' : names[data.worker.status] || data.worker.status} · 最后心跳 {beijingTime(data.worker.heartbeat)}（UTC+8）</p>
      <div className="paper-performance-grid" aria-label="模拟盘收益摘要">
        <div className="paper-performance-card paper-performance-balance"><span>总余额</span><strong>{fmt(data.performance?.total_balance ?? data.account.equity)} USDT</strong><small>权益，包含当前浮动盈亏</small></div>
        <div className="paper-performance-card"><span>今日盈利</span><strong className={(data.performance?.today_net || 0) >= 0 ? 'is-profit' : 'is-loss'}>{signedFmt(data.performance?.today_net)} USDT</strong><small>北京时间 00:00 起</small></div>
        <div className="paper-performance-card"><span>昨日盈利</span><strong className={(data.performance?.yesterday_net || 0) >= 0 ? 'is-profit' : 'is-loss'}>{signedFmt(data.performance?.yesterday_net)} USDT</strong><small>北京时间上一自然日</small></div>
        <div className="paper-performance-card"><span>全部盈利</span><strong className={(data.performance?.net || 0) >= 0 ? 'is-profit' : 'is-loss'}>{signedFmt(data.performance?.net)} USDT</strong><small>已实现净盈亏，已扣手续费</small></div>
      </div>
      {data.config.paper_ai_autopilot && <div style={{ border: '1px solid var(--accent-primary)', borderRadius: 10, padding: 16, marginBottom: 16 }}>
        <h3>DeepSeek 自主操盘 · 当前模拟账户</h3>
        <p>DS 选择候选币、开平仓方向、保证金、杠杆上限、止盈止损和持仓时间；程序校验后按实际盘口模拟成交。当前模型：{data.config.ai_model}。</p>
        <p>状态：{{ pending: '正在分析', complete: '已完成本轮决策', expired: '响应已过期，等待下一轮', error: '请求失败，本轮观望', waiting_data: '等待实时数据' }[data.autopilot?.status as string] || '等待首次分析'} · 最近完成 {beijingTime(data.autopilot?.at)} · 每 {data.config.paper_ai_autopilot_interval_seconds} 秒最多一批请求</p>
        {data.autopilot?.summary && <p>{data.autopilot.summary}</p>}
        {data.autopilot?.reason && <p>{data.autopilot.reason}</p>}
        <p>已完成请求 {fmt(data.autopilot?.requests || 0, 0)} · 累计输入 {fmt(data.autopilot?.usage_total?.prompt_tokens || 0, 0)} tokens · 缓存命中 {fmt(data.autopilot?.usage_total?.prompt_cache_hit_tokens || 0, 0)} · 缓存未命中 {fmt(data.autopilot?.usage_total?.prompt_cache_miss_tokens || 0, 0)} · 最近响应 {fmt(data.autopilot?.telemetry?.elapsed_ms, 0)} 毫秒</p>
        <small>这是新的模拟策略，尚未通过盈利验证；此前成交和盈亏继续保留，实盘只读。</small>
      </div>}
      <p>行情通道：{data.worker.market_stream?.status === 'connected' ? 'WebSocket 实时行情已收到' : data.worker.market_stream?.status === 'no_market_events' ? 'WebSocket 暂无行情帧，使用 REST 回退并重试' : data.worker.market_stream?.status === 'reconnecting' ? `WebSocket 重连中（${data.worker.market_stream.reconnects || 0} 次），使用 REST 回退` : '等待 WebSocket · REST 回退可用'} · 新鲜标记价 {data.worker.market_stream?.fresh_marks || 0}/{data.config.symbols.length}</p>
      {data.worker.error && <p role="alert" style={{ color: '#edac65' }}>{data.worker.error}</p>}
      <p><strong>新仓模拟杠杆：{data.config.paper_dynamic_leverage ? `${data.config.paper_min_leverage}–${data.config.paper_max_leverage} 倍动态计算（BTC/ETH 上限 ${data.config.paper_major_max_leverage} 倍）` : formatLeverage(data.config.leverage)}</strong> · 当前虚拟钱包 {fmt(data.account.wallet)} USDT · 新仓每仓保证金{data.config.paper_margin_is_ceiling || data.config.paper_exploration_mode ? '上限' : ''} {fmt(data.config.paper_margin || data.config.order_notional / data.config.leverage)} USDT · 总保证金上限 {fmt(data.config.paper_max_margin || data.config.max_notional / data.config.leverage)} USDT · 最多 {data.config.max_positions} 个仓位</p>
      {data.config.paper_margin_is_ceiling && <p>实际保证金根据实时盘口和组合风险向下调整，持仓卡片显示实际占用。缩小订单不豁免成本检查和模型验证。</p>}
      {data.config.paper_dynamic_leverage && <p>杠杆由波动止损距离、往返成本、盘口容量和剩余组合风险计算；单仓风险预算为保证金的 {fmt(data.config.paper_risk_fraction * 100)}%，无法支持最低杠杆时跳过。达到最高倍数不是目标。</p>}
      {data.config.paper_lab_required && <p><strong>{data.config.paper_ai_autopilot ? 'DS 行情门槛' : '模型开仓门槛'}：{data.config.paper_ai_autopilot ? data.lab_gate?.ready ? '实时数据可用' : '等待数据就绪' : data.lab_gate?.exploration ? '探索模拟 · 尚未通过验证' : data.lab_gate?.ready ? '验证通过' : '未就绪'}</strong> · {data.lab_gate?.reason || '等待采集与模型验证'}；已有仓位保护独立执行。</p>}
      {data.config.paper_exploration_mode && <p>探索模拟按实际盘口容量缩小订单，每仓保证金以配置金额为上限；虚拟成交用于收集前向测试结果，不表示模型已通过盈利验证。</p>}
      {data.config.paper_lab_required && <p>模拟订单采用盘口分档 IOC 成交，未成交余额过期，剩余持仓继续管理。累计订单 {fmt(data.execution_statistics?.orders, 0)} · 首次成交 P50 {fmt(data.execution_statistics?.first_fill?.p50_ms)} / P95 {fmt(data.execution_statistics?.first_fill?.p95_ms)} 毫秒。这里测量模拟处理链路，不代表币安真实下单延迟。</p>}
      <p>{data.config.symbols.join('、')} · {data.config.interval} K 线 · 均线 {data.config.fast}/{data.config.slow} · {data.config.paper_dynamic_exits ? '新仓使用 ATR14 动态目标与移动止损，止损只收紧，不放宽' : `模拟止损 ${data.config.stop_pct * 100}% / 止盈 ${data.config.take_pct * 100}%`} · 模拟盘不设每日亏损上限</p>
      <p>{data.config.paper_ai_autopilot ? 'DS 判断方向和仓位参数 · 行情事件唤醒保护检查' : data.config.strategy === 'lab_model' ? '数值模型判断方向 · 行情事件唤醒退出检查' : data.config.strategy === 'adaptive' ? '趋势：均线交叉跟随 · 震荡：布林带区间回归' : data.config.strategy} · {data.config.paper_dynamic_leverage ? '新仓动态杠杆' : `新仓 ${data.config.leverage} 倍杠杆`}。旧仓保留开仓时的保证金与风控参数。</p>
      <p>AI 模式：{data.config.paper_ai_autopilot ? 'DeepSeek 自主决策' : data.config.ai_enabled ? '开仓审核' : '自动调用关闭'} · 置信度阈值 {fmt(data.config.ai_min_confidence * 100)}% · 资金、风险上限和成交由模拟执行器约束。</p>
      <p><strong>策略反思：{data.reflection?.enabled ? `版本 ${data.reflection.active?.version || 1}，已完成 ${data.reflection.report?.window_trades || 0} 笔归因` : '未启用'}</strong> · 最近窗口净盈亏 {fmt(data.reflection?.report?.net_pnl)} USDT · 成本/逐笔毛盈亏绝对值之和 {fmt((data.reflection?.report?.cost_ratio || 0) * 100)}% · {data.reflection?.active?.reason || '等待足够样本'}</p>
      <p>虚拟权益 {fmt(data.account.equity)} USDT · 虚拟可用 {fmt(data.account.available)} USDT · <strong>当前持仓保证金合计 {fmt(totalMargin)} USDT</strong> · {data.daily_loss_blocked ? '每日亏损限制已触发，停止开新仓' : data.config.strategy === 'lab_model' ? '等待有效模型信号与风险检查' : '等待策略信号，通过 AI 审核后才模拟成交'}</p>
      <p>启动时不补做历史信号，不强制开仓。已计手续费及每次成交 {fmt(data.config.paper_slippage_bps)} 基点的不利滑点；逐仓强平按固定维持保证金率 {fmt(data.config.paper_maintenance_rate * 100)}% 近似计算，未覆盖币安风险档位、资金费和真实订单簿冲击，模拟杠杆不代表交易所允许的杠杆。这里是分钟级短线与区间交易，没有执行双腿套利。</p>
      {Object.entries(data.positions || {}).length ? <div className="table-container"><table className="table"><thead><tr><th>合约</th><th>方向</th><th>杠杆</th><th>开仓保证金</th><th>计划风险</th><th>MAE / MFE</th><th>数量</th><th>入场价</th><th>标记价</th><th>模拟浮盈亏</th><th>移动止损价</th><th>动态止盈价</th></tr></thead><tbody>{Object.entries(data.positions).map(([symbol, raw]) => { const p = raw as any; return <tr key={symbol}><td>{symbol}</td><td>{p.amount > 0 ? '多' : '空'}</td><td><strong>{formatLeverage(p.leverage)}</strong></td><td><strong>{fmt(p.initial_margin)} USDT</strong></td><td>{fmt(p.exit_plan?.planned_risk_usdt)} USDT</td><td>{fmt((p.mae_pct || 0) * 100)}% / {fmt((p.mfe_pct || 0) * 100)}%</td><td>{fmt(Math.abs(p.amount), 8)}</td><td>{fmt(p.entry, 6)}</td><td>{fmt(p.mark, 6)}</td><td>{fmt(p.pnl, 4)} USDT</td><td>{fmt(p.stop_price, 6)}</td><td>{fmt(p.take_price, 6)}</td></tr> })}</tbody></table></div> : <p>独立模拟账户当前空仓。</p>}
      <h3>{data.config.paper_ai_autopilot ? 'DS 决策与执行检查' : data.config.strategy === 'lab_model' ? '数值模型信号' : '模拟信号与 AI 审核'}</h3>
      {data.config.symbols.map((symbol: string) => <article key={symbol}><strong>{symbol} · {actions[data.signals?.[symbol]?.action] || '等待新决策'}</strong>
        {data.config.paper_ai_autopilot ? <><p>{data.signals?.[symbol]?.reason || '等待 DS 下一批分析'} · {beijingTime(data.signals?.[symbol]?.at)}</p>
          {data.sizing?.[symbol]?.status === 'blocked' && <p>最近仓位校验未通过：{data.sizing[symbol].reason} · {beijingTime(data.sizing[symbol].at)}</p>}</> : data.ai?.[symbol] && <><p>{data.ai[symbol].decision === 'allow' ? 'AI 放行' : 'AI 未放行'}：{data.ai[symbol].reason} · {beijingTime(data.ai[symbol].at)}</p>{data.ai[symbol].telemetry?.usage && <p>本次输入 {fmt(data.ai[symbol].telemetry.usage.prompt_tokens, 0)} tokens · 缓存命中 {fmt(data.ai[symbol].telemetry.usage.prompt_cache_hit_tokens, 0)} · 未命中 {fmt(data.ai[symbol].telemetry.usage.prompt_cache_miss_tokens, 0)} · 命中率 {fmt((data.ai[symbol].telemetry.usage.prompt_cache_hit_rate || 0) * 100, 2)}%</p>}</>}
      </article>)}
      <details className="paper-history" open={historyOpen} onToggle={(event) => setHistoryOpen(event.currentTarget.open)}>
        <summary>开仓历史记录（{(data.events || []).filter((event: any) => String(event.kind).includes('开仓')).length} 条，默认收起）</summary>
        <div className="paper-history-list">
          {(data.events || []).filter((event: any) => String(event.kind).includes('开仓')).slice(0, 30).map((event: any) => <article key={event.id}><strong>{event.kind}</strong> · {beijingTime(event.created_at)}<p style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{event.body}</p></article>)}
          {!(data.events || []).some((event: any) => String(event.kind).includes('开仓')) && <p>暂无开仓记录。</p>}
        </div>
      </details>
    </>}
  </section>
}
