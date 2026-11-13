# Binance REST denial on 2026-11-13

The web application remained reachable. The production observer's last successful
account snapshot was 11:56:41 Asia/Shanghai. Public `/fapi/v1/time` and
`/fapi/v1/exchangeInfo` subsequently returned HTTP 403, HTML `Request blocked`,
`Server: CloudFront`. Later the original paper ledger recorded HTTP 418 with
Binance code -1003, confirming an exchange request ban affecting this egress IP.
Successful public WebSocket marks did not establish that REST/account reads worked.

The depth collector could request another snapshot on the next diff after a failed
snapshot. Several symbols failed in the same millisecond. Optional account leverage
reads also repeated every five seconds. These were confirmed retry amplification
paths; logs do not establish the exact traffic threshold that triggered the first
CloudFront rejection.

## Mitigation

- Serialize collector REST requests with at least one second between starts.
- Back off per symbol after snapshot failures, including network/server errors.
- Pause REST after 403/418/429/451, honor numeric or HTTP-date Retry-After, and
  increase repeated-denial delays without automatically retrying order writes.
- Share the longest active cooldown through Redis using an atomic Lua update.
  Read shared state at most once per second per client; retain local timers if
  Redis is unavailable. Container restarts preserve the Redis deadline while
  Redis remains running. Redis itself remains disposable and nonpersistent.
- Cache optional leverage reads for 60 seconds and suppress immediate retries
  on failure. Expired leverage values are not silently returned as current.
- Show an explicit gateway/cooldown error while retaining the last successful
  account snapshot timestamp. The original paper worker also reports cooldowns.

The exchange-supplied delays observed during deployment were not identical across
clients. The longest observed deadline plus 30 seconds was shared as
2026-11-13 13:49:18 Asia/Shanghai. This is the earliest scheduled recovery attempt,
not a promise that the exchange will accept requests then. No probes should bypass
the active cooldown. Full account reconciliation and candidate depth initialization
remain unavailable during the ban; fresh public marks alone are insufficient for
new paper entries. The original paper ledger had no open positions at verification.

Real trading remains disabled and the real account observer remains read-only.

## Validation

179 related unit/regression tests passed. A server-side integration test against
the existing Redis verified that another client observes the cooldown and a shorter
denial cannot shorten it. Website API health endpoints returned HTTP 200.
