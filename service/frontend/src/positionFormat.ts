export function formatLeverage(value: unknown): string {
  if (typeof value !== 'number' && typeof value !== 'string') return '未提供'
  const leverage = Number(value)
  return Number.isInteger(leverage) && leverage > 0 ? `${leverage} 倍` : '未提供'
}
