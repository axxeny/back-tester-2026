# Scope and frozen decisions

## 1. Goal

Build a deterministic, in-process options backtesting engine suitable for an HFT school assignment. It must be meaningfully performance-conscious and correctly model event ordering, private orders, fills, state, Python callbacks, and results without turning into a production exchange platform.

## 2. Required source behavior

The source assignment requires:

- a market-data consumer receiving `BookUpdate`, `BookSnapshot`, and `Trade` messages;
- virtual engine time with fixed market-data latency;
- an atomic `processed_seq` ready barrier;
- `HistoricalLOB + EngineView = SimulatedLOB`;
- fill-at-touch and fixed order latency;
- an Order Manager / Position Keeper;
- Python strategy callbacks through pybind11 with correct GIL handling;
- editable packaging through one tool and `scikit-build-core`;
- `Result` with PnL, fills, and order log using bulk NumPy/Arrow hand-off;
- synthetic tests, an end-to-end Python strategy, and two benchmarks.

## 3. In scope for the first complete HW4 version

- One OS process.
- One Backtest Engine thread and one Trading Engine thread.
- One mandatory `EngineView`; multi-engine remains a compatible extension point.
- Multiple instruments, with `instrument_id` in every event, order, callback, position, and result row.
- Databento-like MBO JSONL ingestion first.
- Fixed market-data latency and fixed order latency, both configurable.
- Limit GTC orders, partial fills, resting orders, and cancel.
- Top-N strategy book callbacks, configurable, default depth 15.
- Full internal L3 replay even when Python receives filtered top-N callbacks.
- A deterministic fill-at-touch model that consumes displayed historical depth up to the limit.
- Per-instrument positions and contract multipliers.
- Bulk result buffers and pandas-facing views.
- Reproducible unit, integration, QA, and benchmark commands.

## 4. Explicitly out of scope

- Network sockets, IPC, or multiple OS processes.
- A production exchange gateway protocol.
- Queue-position or probabilistic passive fill modeling.
- Market impact on the source historical replay.
- Jitter, stochastic latency, or stochastic slippage.
- Replace/amend, stop, peg, post-only, FOK, IOC, or multi-leg orders.
- Exercise, assignment, expiration settlement, Greeks, or a full options risk engine.
- A generic plugin architecture, dependency injection framework, persistence/database layer, distributed telemetry, or UI.

## 5. Frozen project decisions

These are implementation decisions selected because the source assignment is ambiguous. They are authoritative until changed through the decision log.

### D1. Single fill authority

`SimulatedLOB` is the only component that creates synthetic fills. For HW4, the big-picture Gateway Server and Slippage Simulator are represented by an in-process order-command queue and chronological scheduler, not separate services.
The production implementation is the typed `src/trading/SimulatedLOB` with
its owned `EngineView`; `TradingEngine` applies its decisions to lifecycle,
positions, results, and callbacks. The older `src/main/SimulatedLOB` is not
part of the build; its independent matcher and test were removed when the
typed production authority was connected.

### D2. Deterministic single virtual timeline

- Historical market delivery time: `exchange_ts_ns + market_data_latency_ns`.
- Order arrival time: `submit_engine_ts_ns + order_latency_ns`.
- Cancel arrival time: `cancel_submit_engine_ts_ns + order_latency_ns`.

The dispatcher merges these scheduled events. This is a deliberate homework-compatible simplification, not a claim of full exchange/observer causality under independent market-data delay.

### D3. Same-timestamp priority

At equal scheduled time:

1. historical market events;
2. new-order arrivals;
3. cancel arrivals.

Within a class, source sequence or command sequence is the stable tie-breaker.

### D4. Strict ready barrier

The dispatcher assigns a monotonically increasing `dispatch_seq`, publishes one high-level engine event, and waits until `processed_seq >= dispatch_seq` before mutating shared market state for the next event. Prefetching into a queue is allowed; consuming the next event is not.

### D5. Callback contract

Python receives:

- `on_book_update(BookUpdateView)`;
- `on_trade(TradeView)`;
- `on_fill(FillView)`;
- `on_reject(RejectView)`.

Every payload includes `instrument_id`, exchange time, engine time, and a deterministic sequence.

### D6. Book callback granularity

A book callback fires after an atomic historical group is complete, such as Databento `F_LAST`, and only if the configured top-N view changed. Default N is 15. A smoke test may use N=1. Internal replay remains full L3.

### D7. Order lifecycle

The required states are:

`PendingNew`, `Open`, `PartiallyFilled`, `Filled`, `PendingCancel`, `Cancelled`, `Rejected`.

Replace is not implemented.

### D8. Numeric core types

- nanosecond timestamps: signed 64-bit integer;
- price: fixed-point integer ticks;
- quantity: integer;
- instrument/order IDs: numeric;
- sides and states: strongly typed enums.

Strings and floating-point parsing stay in ingestion or Python boundary code.

### D9. Fill model

Own limit orders match opposite historical displayed liquidity price-by-price up to the limit. Partial fills and multi-level sweeps are supported. Own orders at the same price are FIFO by exchange-arrival sequence. Historical queue position is not modeled.

### D10. Private historical consumption

A synthetic fill does not mutate the shared `HistoricalLOB`. It records consumption in that engine's private overlay. Consumption is keyed by side, price, and historical level revision so new liquidity at the same price is not permanently hidden.

### D11. State before callback

Order state, position, cash/PnL inputs, and result buffers are updated before `on_fill()` or `on_reject()` is called.

### D12. Non-recursive command processing

Orders or cancels submitted inside a callback are enqueued as future scheduler commands. They are never recursively matched inside the same C++ stack frame.

### D13. Python exception policy

A Python exception:

1. is captured;
2. requests engine stop;
3. unblocks any ready/barrier wait;
4. joins threads;
5. is rethrown to the caller of `backtest.run()`.

### D14. Result hand-off

C++ accumulates typed columnar buffers. Python objects are created once after the run. Zero-copy NumPy/Arrow views are used where ownership and dtype permit; per-row Python append is forbidden.

## 6. Decision-change rule

A change to public callbacks, event priority, state transitions, result columns, or numeric core types requires:

1. a decision-log entry;
2. updated architecture docs;
3. migration of all dependent tests and stubs in the same change set;
4. explicit Team Lead approval before implementation branches rebase on it.
