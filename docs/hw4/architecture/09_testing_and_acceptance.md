# Testing strategy and acceptance criteria

## 1. Testing layers

### Native unit tests

Test one component without Python or real files:

- decimal/timestamp parsing;
- L3 add/cancel/modify/fill/clear;
- top-N extraction and revisions;
- SPSC ring boundaries;
- scheduled event ordering;
- order state transitions;
- matching and private consumption;
- position and PnL arithmetic.

### Native integration tests

Run both native threads with a C++ no-op or scripted Strategy:

- market event → callback;
- callback submission → delayed arrival → fill/rest;
- later book move → resting fill;
- cancel/fill race;
- stop and exception unblocking;
- deterministic repeated results.

### Python integration tests

Build/import the extension and verify:

- Strategy subclass callbacks;
- multi-instrument context queries;
- Python submission and cancel;
- Result DataFrame columns/dtypes;
- native-buffer lifetime;
- Python exception propagation;
- second run after a failed run.

### Independent QA

QA must add or run tests not authored by the implementation agent and build from a clean worktree.

## 2. Required matching cases

1. marketable buy at touch;
2. marketable sell at touch;
3. non-marketable order rests;
4. partial fill due to displayed size;
5. sweep across multiple levels up to limit;
6. limit protection stops sweep;
7. later market event fills a resting order;
8. two own orders at one price respect FIFO;
9. stale private consumption resets on historical revision;
10. two EngineViews do not see each other's private orders/consumption;
11. duplicate or invalid order rejects deterministically;
12. unknown cancel is handled deterministically;
13. own private view does not remain crossed after immediate matching.

## 3. Required event/concurrency cases

1. virtual clock equals scheduled event time;
2. order cannot arrive before `submit + order_latency`;
3. equal timestamps obey the documented priority;
4. `processed_seq` advances only after callbacks and command enqueue;
5. producer can prefetch but consumer does not process N+1 early;
6. queue-full behavior does not lose or reorder commands;
7. normal end-of-data joins both threads;
8. Python exception cannot deadlock the dispatcher;
9. 20 repeated runs produce identical normalized results;
10. sanitizer run reports no data race in the mandatory one-engine mode.

## 4. Required result cases

- signed position equals sum of signed fills per instrument;
- cumulative filled quantity never exceeds order quantity;
- terminal orders disappear from `open_orders()`;
- contract multiplier affects PnL but not raw position;
- midpoint marking and stale-mark fallback are deterministic;
- one order-log row exists per visible transition;
- returned arrays remain valid after native engine teardown;
- no per-row Python append path exists.

## 5. Build/package acceptance

From a clean checkout, the documented commands must:

1. install/sync the chosen Python environment;
2. build the editable extension;
3. import `back_tester`;
4. build native targets;
5. run native and Python tests;
6. run the example strategy.

The build must not require an untracked local `3rdparty` directory or a hard-coded data path.

## 6. End-to-end checkpoint

A checked-in tiny deterministic JSONL fixture drives a Python mean-reversion smoke strategy through:

```text
reader → dispatcher → HistoricalLOB → event ring → MarketDataConsumer
→ SimulatedLOB → OrderManager/PositionKeeper → Python callbacks → Result
```

The test must assert exact callback order, at least one real submitted order, expected fill/state/position, and stable result columns.

## 7. Final Homework 4 definition of done

- `pip install -e .` builds and imports the native module.
- A Python strategy receives multi-instrument top-N callbacks.
- `submit_limit()` creates `PendingNew`, respects order latency, and deterministically fills or rests.
- Resting orders fill on later market changes.
- Position and order state are updated before `on_fill()`.
- Cancel passes through `PendingCancel` and reaches a terminal result.
- `Result` returns PnL, fills, and order log through bulk buffers.
- Synthetic and end-to-end tests pass repeatedly.
- Ready-signal and 1,000-callback benchmarks run in Release mode.
- Python errors and shutdown leave no deadlocked thread.
- Independent QA and review have no unresolved P0/P1 finding.
