# Task M4A-001 — native result buffers and deterministic PnL

## Identity

- **Milestone:** M4A Results and PnL
- **Base implementation:** `hw4/backtest-engine-options` /
  `2fa28d042c3d1be6e0199f002e428c1c4f1760bd`
- **Assigned role prompt:** `docs/hw4/prompts/06_dev_results_pnl.md`
- **Owner:** results developer agent

## Scope

Implement native, typed, columnar result storage for fills, order transitions,
and PnL. Consume already-decided trading events; do not match orders or create
state transitions. M4A exposes frozen read-only native buffers and lifetime-safe
views for the later pybind layer, but creates no Python, NumPy, pandas, or Arrow
objects.

## Ownership

- Own: new `src/results/**`, `test/ResultsTest.cpp`, and minimal CMake
  registration.
- A tiny adapter/test seam may consume `trading::Recorder`; changing the trading
  state machine or matching is forbidden.
- Must not edit: `src/core/**`, `src/market/**`, `src/scheduler/**`, Python or
  package files, result schema encodings, or legacy main code.
- Any required change to `src/trading/Strategy.hpp` must be requested before
  implementation; prefer composition/manual mark input.

## Required behavior

1. Store every result field in its own typed contiguous vector with explicit
   pre-reservation; no row objects or Python objects in the event loop.
2. Append fill and order-transition columns in deterministic callback order and
   keep all column lengths equal after every successful append.
3. Recorder append is transactional under allocation/arithmetic failure: a
   partial row cannot become externally visible.
4. Maintain an independent per-instrument FIFO position/PnL ledger reconciled
   from fill rows; raw signed quantity is unaffected by multiplier.
5. Exact native PnL:

```text
realized numerator = sum(delta_price_ticks * closed_qty * multiplier)
unrealized numerator at mark =
    sum((mark_ticks - lot_price_ticks) * signed_open_qty * multiplier)
total account amount = numerator / price_scale
```

   A midpoint is the exact rational `(bid + ask) / 2`; use denominator
   `2 * price_scale` when required. Check every multiply/add before mutation.
6. A book mark changes only when both sides form a valid midpoint. Missing-side
   updates retain the previous valid mark.
7. Sample aggregate total PnL on every fill and on a valid mark change for an
   instrument with a nonzero held position. Equal `engine_ts_ns` samples
   coalesce deterministically by replacing the last value.
8. `freeze()` is one-way. After freeze, appends/marks fail deterministically;
   read-only column spans remain valid for the lifetime of the owning shared
   result storage.
9. Empty frozen results expose correctly typed empty spans.
10. Converting the exact aggregate to `PnlPoint.total_pnl` happens once at the
    public result boundary; no intermediate double accounting.

## Acceptance

- exact fill/order column values, enum underlying types, and equal lengths;
- one transition row per supplied recorder event;
- signed positions reconcile with fills for multiple instruments;
- long add/partial close/close/flip and short equivalents;
- multiplier and non-unit `price_scale`;
- half-tick midpoint, missing-side stale mark, mark change filtering;
- aggregate multi-instrument PnL and equal-timestamp coalescing;
- checked multiplication/addition overflow with state and columns unchanged;
- empty/frozen/lifetime behavior, including owner destruction while a retained
  shared result handle remains alive;
- 20 identical runs produce identical columns;
- fresh Release build/full CTest and sanitizer attempt.

## Performance constraints

- No per-event Python/pandas objects or maps of string columns.
- Reserve all columns consistently and avoid result-buffer reallocations after
  the provided estimate.
- One bulk column hand-off in M4B; no per-row conversion API.

## Done

Developer tests pass, independent QA and review have no unresolved P0/P1, and
the exact candidate fast-forwards into the integration branch.
