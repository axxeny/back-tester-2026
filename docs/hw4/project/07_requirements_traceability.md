# Homework 4 requirements traceability

This matrix prevents a source requirement or source-diagram box from becoming
an implicit implementation assumption. The source assignment remains
authoritative; architecture documents define adopted behavior where the source
is ambiguous.

## Assignment requirements

| Source requirement | Adopted contract / design | Delivery milestone | Acceptance evidence |
|---|---|---|---|
| Agree callback granularity | Top-N after an atomic historical group, only when the visible top-N changes; default depth 15 | M1, M2A, M4B | callback filtering tests and exact callback-order integration test |
| Independent fixed market-data and order latency | `historical: exchange_ts + md_latency`; `order/cancel: submit_engine_ts + order_latency` | M1, M2B, M3 | virtual-clock and delayed-arrival tests |
| Consume `BookUpdate`, `BookSnapshot`, and `Trade` | One typed high-level market event can carry a stable book update/snapshot marker and source trades; `BookUpdateView.is_snapshot` distinguishes snapshot callbacks | M1, M2A, M3 | typed ingestion plus snapshot/update/trade callback tests |
| Atomic ready sequence | Release-store `processed_seq` only after matching, state, results, callbacks, and command enqueue | M2B | barrier ordering, prefetch, queue-full, stop/unblock tests and Release benchmark |
| `HistoricalLOB + EngineView = SimulatedLOB` | Shared historical state plus per-engine orders and revision-keyed private consumption | M2A, M3 | matching cases 1–10 in `architecture/09_testing_and_acceptance.md` |
| Fill-at-touch | Deterministic displayed-depth sweep up to the limit, with partial fills and later resting reevaluation | M3 | synthetic buy/sell, multi-level, limit protection, resting, FIFO tests |
| Order latency affects fills | Orders are `PendingNew` locally and cannot match before their scheduled arrival | M2B, M3 | submit/arrival timing and same-time priority tests |
| OrderManager / PositionKeeper | One owner of lifecycle transitions; signed per-instrument positions updated before callbacks | M3 | state-machine, position, cancel-race, callback-observation tests |
| Strategy can query position/open orders and submit | Multi-instrument `StrategyContext` surface with immutable callback-scoped views/snapshots | M1, M2C, M4B | native stub and Python context tests |
| UV plus `scikit-build-core` editable packaging | UV is the only documented environment workflow; pybind11 extension built by `pip install -e .` | M0 | clean-worktree sync/install/import tests |
| Bind core types and order API | One M1 contract header is consumed by native engine and bindings | M1, M2C, M4B | compile-time contract tests and Python enum/payload tests |
| `on_book_update`, `on_trade`, `on_fill`, `on_reject` | Fixed payloads carry instrument, exchange/engine time, and deterministic sequence where applicable | M1, M2C, M4B | callback payload and exception tests |
| Correct GIL discipline | GIL released for native run/waits and acquired only for Python interaction | M2C, M4B | callback exception, second-run, and deadlock tests |
| `Result.pnl_series`, `fills_df`, `order_log_df` | Typed native column buffers retained by the returned result wrapper | M4A, M4B | schema/dtype, arithmetic, and lifetime tests |
| Zero-copy NumPy/Arrow hand-off | Native-to-array views are zero-copy where dtype and ownership permit; any unavoidable pandas materialization copy must be bulk, explicit, and lifetime-tested, never per-row | M4A, M4B | buffer-address/ownership tests plus code review |
| `backtest.run(strategy, data_path, date_range)` | Minimal three-argument form with optional explicit `BacktestConfig` | M4B | Python end-to-end invocation test |
| Synthetic matching harness | Focused native matching suite | M3 | cases in `architecture/09_testing_and_acceptance.md` |
| End-to-end Python strategy | Checked-in deterministic fixture and example strategy through the real engine path | M5 | exact callback/order/fill/position/result assertions |
| Ready round-trip benchmark | Warmed Release dispatcher → consumer → `processed_seq` measurement | M6 | reproducible command and p50/p95/p99/min/mean report |
| 1,000-callback benchmark | Top-1 and top-15 no-op Python callback samples, with native overhead separated where possible | M6 | reproducible command and benchmark report |
| N concurrent EngineViews bonus | Compatible extension point, not required for the mandatory one-engine scheduler | Post-M6 / bonus | independent-view isolation tests if retained |

## Original diagram disposition

The original Mermaid page is context, not an additional list of Homework 4
deliverables. Every box nevertheless has an explicit disposition:

| Original diagram box/flow | HW4 disposition |
|---|---|
| Databento JSON / Feather data | JSONL/MBO ingestion is mandatory first; Feather is not required by the written assignment and is deferred |
| Event Merger / Chronological Dispatcher | Native market source/merger plus deterministic scheduler in M2A/M2B |
| Map of LOBs | Multi-instrument `HistoricalLOBStore` in M2A |
| Market Data Publisher → Consumer | In-process typed SPSC event ring and strict ready barrier in M2B/M3 |
| LOB Simulation / Matching / Fills | `SimulatedLOB`, the single fill authority, in M3 |
| Order Manager / Position Keeper | Mandatory M3 components |
| Strategy Logic | Python Strategy implementation invoked through the C++ adapter; it is not a second native strategy implementation |
| API / Strategy Parameter Setup | Python package, `BacktestConfig`, `DateRange`, and Strategy context in M2C/M4B |
| Visualization & Analysis | Consumer of returned pandas objects; no UI framework is part of HW4 |
| Feature Generator | Not a written HW4 deliverable; strategies may compute features in callbacks. A native feature framework is out of scope unless separately assigned |
| Risk Engine | A full risk engine is explicitly out of scope; deterministic order validation remains required in M3 |
| Gateway Client / Server | Collapsed into the in-process typed command ring and scheduled order/cancel arrivals |
| Slippage Simulator | No second fill/slippage authority; fixed latency plus `SimulatedLOB` fill-at-touch is the adopted homework model |
| API return path | `Result` owns typed buffers and exposes bulk pandas-facing views after native threads join |

## Documentation gaps that must be closed during implementation

The architecture intentionally does not pre-invent final C++ declarations. The
following items are M1/M4 deliverables, not permission for subsystem-local
copies:

1. exact `EngineEvent`, command, callback, order-state, reject, config, and
   instrument metadata declarations in one shared contract location;
2. exact `DateRange` inclusivity and end-of-range pending-command policy;
3. exact cancel-after-fill observable reject/event rule;
4. final result enum encodings and Python dtypes;
5. ownership proof for zero-copy arrays and explicit documentation of any bulk
   pandas copy that cannot be avoided.

Any resolution that changes the frozen decisions must update the decision log
before dependent implementation starts.
