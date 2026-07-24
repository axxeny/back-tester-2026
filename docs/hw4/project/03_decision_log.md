# Architecture decision log

Use this file to prevent different agents from silently implementing incompatible assumptions.

## Accepted decisions

| ID | Status | Decision | Reason | Affected contracts |
|---|---|---|---|---|
| ADR-001 | Accepted project default | `SimulatedLOB` is the only synthetic fill authority. | Avoid double execution between trading-side simulation and source-diagram gateway/slippage boxes. | matching, OrderManager |
| ADR-002 | Accepted project default | Use one deterministic scheduled timeline with fixed market-data and order latency. | Matches the homework's lock-step ready barrier without building a second independently advancing exchange simulation. | scheduler, timestamps |
| ADR-003 | Accepted project default | Same-time priority is market event → new order → cancel. | Provides stable deterministic behavior. | scheduler tests |
| ADR-004 | Accepted | Strategy callbacks are top-N after atomic book groups; default N=15. | Avoid raw L3 Python overhead while keeping useful depth. | C++ filtering, Python API |
| ADR-005 | Accepted | Every callback/order/position API is multi-instrument. | Options strategies require option + underlying. | all public APIs |
| ADR-006 | Accepted | Limit GTC + cancel only for HW4. | Meets the checkpoint without speculative order-type scope. | state machine, Python API |
| ADR-007 | Accepted | Price is integer ticks; time is int64 ns; quantity and IDs are integer. | Determinism and hot-path performance. | core types, ingestion |
| ADR-008 | Accepted project default | Fill-at-touch sweeps displayed historical levels up to the limit and supports partial fills. | Prevents crossed private books and defines quantity semantics. | SimulatedLOB tests |
| ADR-009 | Accepted project default | Synthetic fills consume liquidity only in the current EngineView, keyed by historical revision. | Preserve shared replay while avoiding stale depletion. | EngineView, HistoricalLOB |
| ADR-010 | Accepted | State/position update precedes `on_fill()`/`on_reject()`. | Strategy observes consistent post-event state. | OrderManager, callbacks |
| ADR-011 | Accepted | Python errors stop/unblock/join and are rethrown. | No deadlock or swallowed exceptions. | runtime, bindings |
| ADR-012 | Accepted | C++ columnar buffers; one bulk Python conversion; no per-row append. | Assignment requirement and performance. | Result |
| ADR-013 | Accepted project default | `DateRange` includes both endpoints for historical records; command arrivals at the end may execute, while arrivals strictly after the end do not. | Makes replay and scheduler end behavior deterministic. | source filter, scheduler |
| ADR-014 | Accepted project default | Cancel arrival after terminal fill emits `RejectReason::AlreadyTerminal`. | Keeps late cancels observable instead of silently ignoring them. | OrderManager, reject callback |
| ADR-015 | Accepted | Result enum columns use each enum's explicit fixed-width underlying encoding documented in `architecture/10_shared_contracts.md`. | Stabilizes the native/Python schema boundary. | core enums, result conversion |
| ADR-016 | Accepted implementation boundary | M1 freezes result row values only; native buffer ownership and zero-copy lifetime are implemented and tested in M4. | Avoids an unverified ownership claim before result buffers exist. | results, Python boundary |
| ADR-017 | Accepted project default | Instrument prices use a positive integer `price_scale` in ticks per quoted/account currency unit; native PnL remains an unrounded checked rational until one binary64 query/result conversion. Midpoints retain half-tick precision. | Makes tick-to-money conversion and rounding deterministic across runtime and result work. | instrument metadata, PositionKeeper, results |
| ADR-018 | Accepted | Fixed order latency must be strictly positive; market-data latency may be zero but must not be negative. | The assignment requires a realistic non-instant order round trip, and zero-latency callback submissions can otherwise execute at the callback timestamp. | BacktestConfig runtime validation, trading tests |
| ADR-019 | Accepted | A streaming source stages a complete market group in `next()` and mutates the historical book only in `prepare_for_dispatch()` after that market key wins chronological selection. Prepared non-owning buffers live through acknowledgement and are reused by the following `next()`. | Prefetch must expose a key without letting a later market group affect an earlier strategy command. | scheduler source protocol, runtime ingestion, buffer lifetime tests |
| ADR-020 | Accepted | Complete atomic groups strictly before `DateRange.start_ts_ns` warm the historical book and top-N cache without callbacks, matching, marks, or commands; a group exactly at start is delivered normally. | A range may begin during the lifetime of an L3 order, and strategy-visible depth must not report a false change at the boundary. | runtime source, HistoricalLOB, DateRange tests |

## Open or instructor-dependent decisions

| ID | Question | Temporary project default | Trigger to revisit |
|---|---|---|---|
| OPEN-001 | Should cancel arrival have priority before new orders at the same timestamp? | New order before cancel. | Instructor clarification or failing golden test. |
| OPEN-002 | Should event callbacks order trade before book update? | Fills, then trade, then book update. | Team contract change. |
| OPEN-004 | Exact PnL sample frequency. | On fill and mark-changing book update for held instruments. | Result consumer requirement. |

## New ADR template

```markdown
### ADR-NNN — short title

- Status: Proposed / Accepted / Superseded
- Date:
- Owner:
- Context:
- Decision:
- Alternatives considered:
- Consequences:
- Code/contracts affected:
- Tests that lock the decision:
```
