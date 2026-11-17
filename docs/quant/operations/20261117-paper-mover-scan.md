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
  approval. Ask DS to compare long and short opportunities, take a small paper
  exploration when at least two independent direction signals agree, and
  normally target a 1-15 minute holding period. Costs remain in the PnL record;
  they are not treated as proof that an unvalidated opportunity must be rejected.
- Preserve real simulated fees, slippage, the existing account and order caps,
  and the real-account read-only boundary.
- Enable the previously disabled paper AI autopilot and DS authority flags.
  Keep dynamic exits enabled as required by configuration validation.

## Validation

- `test_paper_autopilot.py`: 58 passed.
- Full backend suite: 561 passed, 2 skipped.
- The prompt prefix changes, so provider-side cache reuse may need to warm up
  again. Daily maximum AI requests are unchanged.

## Deployment

Release tag and post-deploy sample are recorded after rollout.
