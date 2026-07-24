# Task M4B-001 — real Python Strategy API and native run orchestration

## Identity

- **Milestone:** M4B real Python integration
- **Base implementation:** `hw4/backtest-engine-options` /
  `1a097e56605836ee60a7343c73aaef647d48dc01`
- **Assigned role prompt:** `docs/hw4/prompts/05_dev_python_api.md`
- **Owner:** Python/runtime developer agent

## Scope

Bind the merged core, trading, scheduler, market, and result implementations
into the real user-facing API:

```python
result = backtest.run(strategy, data_path, date_range, config=None,
                      instruments=None)
```

The required three-argument form works with the explicit Databento nanounit
fallback (`tick=1`, `price_scale=1e9`, `multiplier=1`). Callers may supply
instrument metadata for real tick sizes and option multipliers. The runtime
streams typed JSONL groups, mutates the shared historical store before
publication, runs the real `SchedulerRuntime` + `TradingEngine`, invokes Python
callbacks, freezes native results, and returns pandas-facing bulk views.

M2C's stub is deliberately skipped because the real M3 engine is already
merged. No stub may remain on the production path.

## Ownership

- Own: `src/python/**`, `python/back_tester/**`, Python integration tests, and
  new narrow native orchestration under `src/runtime/**`.
- May edit minimal CMake/pyproject/test registration and README/API example.
- Must not change matching, scheduler ordering, core enum/schema encodings, or
  result accounting.
- A tiny integration adapter change in trading/results requires Team Lead
  approval; prefer composition around existing Strategy/Recorder interfaces.

## Runtime requirements

1. JSONL remains streaming; never load/sort the complete file. An optional
   metadata discovery pass for the three-argument fallback may collect only
   unique numeric instrument IDs and must not retain events. Explicit
   `instruments` is the one-pass path.
2. `next()` stages one complete atomic `F_LAST` group and its scheduled key
   without mutating `HistoricalLOBStore`. Once that market event wins scheduler
   selection, `prepare_for_dispatch()` applies the group and builds one
   `MarketDelivery` immediately before publication. The key/priority are
   immutable, and top-N/trade span storage remains stable until acknowledgement
   and the following `next()` call.
3. Delivery time is checked `exchange_ts + market_data_latency`. Source or
   virtual-time regression, malformed JSON, unterminated groups, and overflow
   fail fast.
4. Emit book callback only when the configured top-N changed; preserve best-to-
   worse side order. Emit source trades in stable sequence. The required event
   order remains fills → trades → book callback.
5. The Python optional config must choose a documented strictly positive
   `order_latency_ns` default (ADR-018), never forward `BacktestConfig{}` with
   zero order latency.
6. DateRange is inclusive; commands `== end` execute and commands `> end` do
   not.

## Python API and GIL

- Bind `Side`, `OrderState`, `RejectReason`, `BacktestConfig`, `DateRange`,
  `InstrumentMeta`, callback payloads, position/open-order snapshots, and
  strategy context methods.
- Python callbacks receive one immutable payload argument. During a callback,
  the Strategy base exposes `submit_limit`, `cancel_order`, `position`,
  `open_orders`, and `now_ns`; outside the callback these methods fail clearly.
- Copy callback-scoped book levels into owned Python values before returning
  from the callback; never retain native spans.
- `backtest.run()` retains the strategy and releases the GIL for native
  start/waits/joins. The trading thread acquires it only for callback and
  Python-object access.
- A Python exception from any callback requests stop, wakes rings/barrier,
  joins all threads, and rethrows the original exception on the caller thread.
  A second run in the same interpreter must succeed.
- Strategy callbacks may submit/cancel but commands remain non-recursive.

## Results

- Return a Python `Result` exposing `fills_df`, `order_log_df`, and
  `pnl_series` with frozen schema order and dtypes.
- Native arrays retain shared `FrozenResults` ownership. Use zero-copy NumPy
  views where dtype/layout permit; any unavoidable pandas copy must be one
  explicit bulk conversion and documented/tested.
- Empty results preserve exact dtypes. No per-row Python append/conversion path
  is allowed.

## Acceptance

- clean editable install/import and documented API example;
- real file → historical store → scheduler → trading engine → Python callbacks
  → frozen results, with no stub;
- top-1 and top-15 callback order/payload/lifetime;
- multi-instrument callbacks, submissions, queries, positions, and metadata;
- delayed fill/rest/cancel behavior through Python;
- callback commands non-recursive;
- exception tests for book/trade/fill/reject, bounded join, exact exception,
  then successful second run;
- GIL held inside callbacks and released during an instrumented native wait;
- strategy retained during run and collectible afterward;
- exact DataFrame/Series columns and dtypes, shared-memory/lifetime checks,
  empty results, and no per-row append;
- malformed/order-regressing/unterminated input errors;
- 20 repeated runs return identical callback/result ordering;
- fresh package build, native CTest, Python tests, and sanitizer attempt for
  native orchestration.

## Done

Developer tests pass, independent QA and review have no unresolved P0/P1, and
the exact candidate fast-forwards into the integration branch.
