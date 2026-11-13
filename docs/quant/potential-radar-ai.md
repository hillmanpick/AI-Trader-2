# Potential radar AI commentary

The radar keeps its numerical rule score and observation labels. A separate
`radar-ai` process produces read-only Chinese commentary from at most five fresh
candidate feature vectors with rule score >= 20. It cannot change rankings,
approve a model, adjust leverage, submit orders or control existing positions.
Only the official DeepSeek HTTPS origin is accepted. No Binance credentials or
operator token are supplied to this process.

Deploy with both Compose files, preserving the server's existing base config:

```sh
docker compose -f deploy/compose.yaml -f deploy/radar.compose.yaml config -q
docker compose -f deploy/compose.yaml -f deploy/radar.compose.yaml up -d --no-deps api potential-radar radar-ai
```

The existing server `.env` supplies `DEEPSEEK_API_KEY`, `DEEPSEEK_MODEL` and
`DEEPSEEK_BASE_URL`. The overlay enables only `QUANT_RADAR_AI_ENABLED`;
`QUANT_AI_ENABLED`, live trading and paper AI autopilot remain disabled. Stop
`radar-ai` to stop these paid requests without stopping public data collection.

Calls are bounded to once per 900 seconds and at most 48 attempts per UTC day.
Attempts are persisted in `potential-radar.db` before network I/O, including
errors; there is no immediate provider retry. Each call has at most 1400 output
tokens, a response size cap and request timeout. The stable system prefix and
compact feature batch reduce changing prompt content. Cache hits are measured
from provider usage, not promised; reducing request frequency and input size is
the primary cost control. Request counts bound usage, not a fixed currency cost.

`GET /api/quant/radar/status` includes `ai_analysis` with input timestamps,
model, candidate reasoning, risks, invalidation conditions, usage and daily
attempt totals. A commentary based on data older than 20 minutes is marked
stale. Commentary may refer to an earlier watchlist snapshot, which the UI
explicitly distinguishes from the live ranking. Neither a rule score nor an AI
view is an empirically validated probability or guaranteed return.
