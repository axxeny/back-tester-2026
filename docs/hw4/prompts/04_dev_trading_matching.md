# Prompt: Developer — MarketDataConsumer, SimulatedLOB, matching, OrderManager, and positions

You own the native trading-engine behavior after core market and scheduler contracts are merged. Preserve the teammate's useful shared HistoricalLOB + private EngineView concept, but correct and complete it.

## Read first

- `../../../AGENTS.md`;
- `architecture/05_order_matching_and_state.md`;
- `architecture/04_event_time_and_concurrency.md` callback/event order;
- `architecture/08_repository_gap_analysis.md`;
- current `SimulatedLOB.*`, `LimitOrderBook.*`, tests, and merged scheduler/core APIs;
- active task spec.

## Mission

Implement a deterministic native trading path:

```text
EngineEvent → MarketDataConsumer/VirtualClock → SimulatedLOB
→ OrderManager/PositionKeeper → native Strategy interface / Result recorder
```

## Required behavior

### Consumer and clock

- Consume only scheduler-published high-level events.
- Set virtual engine time to the event's scheduled time and never move backward.
- Reevaluate resting orders before source trade/book callbacks as documented.
- Publish completion only after all state/callback/command work is complete.

### Matching

- Buy matches historical asks `<= limit`; sell matches bids `>= limit`.
- Sweep all marketable displayed levels up to the limit.
- Fill at historical level price.
- Support partial fills and resting remainder.
- Reevaluate resting own orders on later historical updates.
- Own FIFO at one price uses arrival sequence.
- Synthetic fills affect only this EngineView.
- Private consumption is revision-aware; new historical liquidity at the same price becomes visible.
- Never take `historical.snapshot(0)` to match an order.
- Do not match own buy against own sell.

### Order lifecycle

Implement and test:

- `PendingNew`, `Open`, `PartiallyFilled`, `Filled`, `PendingCancel`, `Cancelled`, `Rejected`;
- generated client IDs;
- typed rejects;
- delayed new/cancel arrival integration;
- per-instrument `open_orders()` and `position()`;
- state/position update before callbacks;
- one order-log event emitted per visible transition through a recorder interface.

### Position

Track signed quantity and the agreed cost/realized-PnL inputs. Apply no Python logic here.

## Required tests

At minimum, cover every case in architecture testing section, especially:

- multi-level sweep that previously left a crossed private book;
- later touch fills a resting order;
- stale consumed liquidity after remove/re-add at same price;
- partial fills across multiple events;
- FIFO among own orders;
- full fill removes from `open_orders()` before callback;
- cancel/fill race with deterministic priority;
- invalid order and unknown cancel reject paths;
- independent EngineViews remain private;
- no full-book copy in matching, using instrumentation or a test seam.

Provide a C++ scripted Strategy/context test so behavior is testable before Python integration.

## Performance constraints

- typed fixed-point hot path;
- no string IDs, JSON, pandas, or GIL code;
- price-indexed own-order containers;
- no scan of unrelated instruments;
- reserve temporary fill batches;
- no broad dynamic polymorphism inside the level loop.

## Non-goals

- no queue-position model;
- no stochastic slippage;
- no replace/amend;
- no exercise/assignment;
- no final pandas conversion.

## Handoff

Use the standard report. Include exact matching semantics, state transition table, complexity, known optimistic assumptions, tests that reproduce prior defects, and QA focus on races, partial fills, and callbacks observing post-update state.
