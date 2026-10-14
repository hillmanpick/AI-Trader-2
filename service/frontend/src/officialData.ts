// This deployed fork shows official community reads by default. Set false for a standalone local community.
export const OFFICIAL_MODE = import.meta.env.VITE_OFFICIAL_COMMUNITY !== 'false'

const publicPath = /^(?:signals\/(?:feed|grouped|[1-9]\d*(?:\/replies)?)|agents\/[1-9]\d*\/(?:positions|summary)|profit\/history|trending|claw\/agents\/count|challenges(?:\/(?!me$)[\w-]+(?:\/(?:leaderboard|team-leaderboard|teams|submissions))?)?|market-intel\/(?:macro-signals|etf-flows|news|stocks\/(?:featured|[\w.^-]+\/(?:latest|history))))$/
export type OfficialState = { path: string; fetched_at?: string; stale?: boolean; warning?: string; data_as_of?: string; error?: string }

/** Only these modules' public GETs use the official source. Quant and local auth keep their own APIs. */
export async function communityFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const url = new URL(String(input), window.location.origin)
  const path = url.pathname.replace(/^\/api\//, '')
  const method = (init?.method || 'GET').toUpperCase()
  if (!OFFICIAL_MODE || url.origin !== window.location.origin || !url.pathname.startsWith('/api/')) return window.fetch(input, init)
  if (method !== 'GET' && /^(signals|challenges)(\/|$)/.test(path)) {
    return Response.json({ detail: '官方社区以只读方式展示，请到官方站点互动。' }, { status: 403 })
  }
  if (method !== 'GET' || !publicPath.test(path)) return window.fetch(input, init)
  const notify = (detail: OfficialState) => window.dispatchEvent(new CustomEvent('official-data', { detail }))
  try {
    // Do not forward local bearer tokens, bodies, or custom headers to the public adapter.
    const res = await window.fetch(`/api/quant/official/${path}${url.search}`, { signal: init?.signal })
    const data = await res.clone().json()
    notify(res.ok ? data._official : { path, error: data.detail || `读取失败 (${res.status})` })
    return res
  } catch (err) {
    notify({ path, error: '网络连接失败，暂时无法读取官方数据' })
    throw err
  }
}

export function beijingTime(value?: string | null) {
  if (!value) return '—'
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00Z` : /(?:Z|[+-]\d{2}:\d{2})$/.test(value) ? value : `${value}Z`
  const date = new Date(normalized)
  return Number.isNaN(date.getTime()) ? '时间未知' : date.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false })
}
