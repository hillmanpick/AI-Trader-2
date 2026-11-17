# Paper short-horizon mover scan

The paper account remains the only execution target. This change makes the
existing DeepSeek autopilot act on the selected market leaders sooner and
preserves the market scanner's ranking when building its portfolio request.

## Changes

- Lower the autopilot's hard minimum decision interval from 300 seconds to
  60 seconds. The configured daily request ceiling remains 288.
- Keep every open paper position first, then order eligible candidates by the
  fresh potential radar's stronger long/short score and the all-market flow
  ranking. The AI still sees no more than twelve candidates per request.
- Clarify that the simulation is collecting fills and exits before model
  approval. Ask DS to compare long and short opportunities, choose the strongest
  after-cost setup, and normally target a 1-15 minute holding period.
- Preserve real simulated fees, slippage, the existing account and order caps,
  and the real-account read-only boundary.

## Validation

- `test_paper_autopilot.py`: 57 passed.
- Full backend suite: 561 passed, 2 skipped.
- The prompt prefix changes, so provider-side cache reuse may need to warm up
  again. Daily maximum AI requests are unchanged.

## Deployment

Pending.
