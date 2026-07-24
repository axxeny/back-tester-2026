# Task M3-001 — native trading, matching, order state, and positions

## Identity

- **Milestone:** M3 Trading and matching
- **Base implementation:** `hw4/backtest-engine-options` /
  `01f7d324e724422d3b04722bb011a62afea0ef35`
- **Assigned role prompt:** `docs/hw4/prompts/04_dev_trading_matching.md`
- **Owner:** trading developer agent

## Scope

Build the typed native trading path on the merged market and scheduler
foundations:

```text
ScheduledEvent → MarketDataConsumer/VirtualClock → SimulatedLOB/EngineView
→ OrderManager/PositionKeeper → native Strategy + recorder interfaces
```

Implement limit-GTC submission, delayed new/cancel arrival, immediate and
resting matching, lifecycle transitions, per-instrument queries, fills,
rejects, positions, and scripted native integration tests. Do not add Python,
pandas, or final result-buffer ownership.

## Ownership

- Own: new `src/trading/**`, `test/TradingTest.cpp`, and a native scripted
  integration/benchmark seam if needed.
- May edit only to register owned sources/tests: the smallest relevant CMake
  files.
- Legacy `src/main/SimulatedLOB.*` remains a compatibility path; do not rewrite
  or delete it in M3.
- Must not edit: `src/core/**`, `src/market/**`, `src/scheduler/**`,
  Python/package files, or result schemas.

## Required behavior

1. Every strategy-facing event/order/query carries `instrument_id`; validate
   instrument metadata, including positive tick size, scale, and multiplier.
2. Virtual time is the consumed event key time and never regresses.
3. Submit creates `PendingNew` immediately and enqueues a unique, monotonically
   sequenced command for `now + order_latency`; sequence exhaustion is an error.
4. Cancel submission makes an eligible order `PendingCancel` immediately and
   enqueues a unique future cancel command. Unknown/terminal cancels reject
   deterministically; arrival after fill is `AlreadyTerminal`.
5. Equal-time execution relies on frozen scheduler priority
   Market → New → Cancel; trading must not recurse into newly submitted commands.
6. Immediate buys sweep historical asks `<= limit`; sells sweep bids `>= limit`.
   Fill price is the historical order price. Remainders rest.
7. Later market delivery reevaluates only marketable own resting ranges.
   Own FIFO is price then exchange-arrival sequence. Own orders never self-match.
8. Private consumption key includes at least `instrument_id`, historical side,
   `exchange_order_id`, and `liquidity_revision`. Partial historical fills keep
   identity; modify/remove/re-add exposes genuinely new liquidity.
9. Matching uses `LimitOrderBook::for_each_marketable_liquidity`; no full-book
   snapshot/copy and no string/double/JSON/Python work in the event loop.
10. Before fill/reject callback: order indexes/state, signed position, and
    recorder notification are already updated. Full fills are absent from
    `open_orders()`.
11. Visible lifecycle follows the frozen state machine and emits exactly one
    recorder event per visible transition.
12. The consumer returns only after callbacks and their command enqueues finish;
    SchedulerRuntime publishes `processed_seq` afterward.

## Interfaces and boundaries

- Consume frozen `ScheduledEvent`, `NewOrderCommand`, `CancelCommand`,
  `FillView`, `RejectView`, and query contracts without changing them.
- Use a small native Strategy interface and a small recorder interface suitable
  for M4 adapters; no dynamic dispatch inside the per-liquidity matching loop.
- `PositionKeeper` owns signed quantity per instrument and deterministic cost
  basis/realized-PnL inputs. Final columnar buffers and pandas conversion are M4.
- The historical store is dispatcher-owned and read as stable while the ready
  barrier is outstanding.

## Acceptance

- all matching cases in `architecture/09_testing_and_acceptance.md`;
- multi-level sweep and limit protection;
- resting fill after later market update;
- partial fills across events and stable private consumption;
- remove/modify/re-add at the same price;
- two own orders FIFO and two EngineViews isolated;
- state/position/open-order view already updated inside callbacks;
- invalid instrument/side/price/tick/quantity and duplicate ID paths;
- delayed new/cancel and equal-time cancel/fill race through real
  `SchedulerRuntime`;
- late cancel after fill produces `AlreadyTerminal`;
- command sequences are unique/monotonic, locking the M2B duplicate-key
  producer precondition;
- 20 identical scripted runs;
- fresh Release build/full CTest and sanitizer attempt.

## Performance constraints

- No full-book copy, no scan of unrelated instruments, and no heap snapshot per
  match.
- Price-index own resting orders and reserve short fill batches.
- Numeric typed hot path only.

## Done

Developer tests pass, independent QA has no P0/P1, independent review has no
P0/P1, and the exact candidate fast-forwards into the integration branch.
