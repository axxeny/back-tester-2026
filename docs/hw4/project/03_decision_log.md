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

## Open or instructor-dependent decisions

| ID | Question | Temporary project default | Trigger to revisit |
|---|---|---|---|
| OPEN-001 | Should cancel arrival have priority before new orders at the same timestamp? | New order before cancel. | Instructor clarification or failing golden test. |
| OPEN-002 | Should event callbacks order trade before book update? | Fills, then trade, then book update. | Team contract change. |
| OPEN-003 | Should pending commands after date-range end execute? | Do not execute arrivals strictly after end time; log/close according to end policy. | Product decision during integration. |
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
