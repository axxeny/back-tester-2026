# Prompt: Developer — Result buffers, positions/PnL recording, and pandas hand-off

You own result recording and bulk export. You consume fill/order/mark events already decided by the trading engine; you do not decide matching or state transitions.

## Read first

- `../../../AGENTS.md`;
- `architecture/06_python_api_and_results.md` result schemas;
- `architecture/05_order_matching_and_state.md` position semantics;
- merged M1 schemas and current PositionKeeper/OrderManager recorder interfaces;
- active task spec.

## Mission

Implement typed native buffers and a safe Python-facing `Result` with:

- `fills_df`;
- `order_log_df`;
- `pnl_series`.

No per-row Python append is allowed.

## Required native behavior

- One typed contiguous column per result field, pre-reserved from config/estimate.
- Append fill rows and order-transition rows in deterministic event order.
- Track per-instrument mark and aggregate PnL using contract multiplier.
- Sample PnL on fills and mark-changing book updates for held instruments; coalesce equal timestamps deterministically.
- Freeze buffers after run completion.
- Expose read-only access suitable for NumPy/Arrow ownership.

## PnL semantics

Use the frozen project rule:

- signed position quantity;
- realized PnL from the chosen consistent cost-basis method;
- unrealized PnL marked at midpoint when both sides exist;
- retain previous valid mark when midpoint is unavailable;
- apply `InstrumentMeta.contract_multiplier`;
- fees zero by default but keep an extension field/config if already in contracts.

Document the exact formulas and rounding behavior in tests.

## Python hand-off

- Prefer NumPy arrays that reference immutable buffers owned by the returned Result wrapper.
- Arrow is acceptable where it simplifies ownership/dtypes.
- One bulk copy is acceptable when safe zero-copy is impossible; document it honestly.
- The Result wrapper must keep native storage alive while any DataFrame/Series view is alive.
- Enum columns may be integer codes with documented mapping.

## Required tests

- exact fill/order columns and dtypes;
- one transition row per state change;
- signed position reconciles to fills;
- realized/unrealized/total PnL examples for long, partial close, short, and multiplier;
- missing-side/stale mark fallback;
- equal-timestamp coalescing;
- empty result returns correctly typed empty frames;
- buffer lifetime after engine object destruction and garbage collection pressure;
- no Python per-row append path, verified by code inspection/test seam;
- deterministic row order across repeated runs.

## Non-goals

- no matching decisions;
- no exercise/settlement;
- no plotting/UI;
- no database export;
- no speculative analytics.

## Handoff

Use the standard report. Include formulas, ownership/lifetime diagram, which columns are truly zero-copy versus bulk-copied, memory reservation behavior, tests, and QA focus on dangling buffers and PnL reconciliation.
