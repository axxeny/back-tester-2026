# Repository baseline and gap analysis

## 1. Reviewed baseline

The supplied ZIP contains the Git repository. The reviewed `main` commit is:

```text
c4f4c02916f5a9fb5f2636926fd93cd28af0f46d
```

Local observations against that snapshot:

- native build succeeds with `BUILD_TESTS=OFF`;
- test configuration fails because `3rdparty/Catch2/extras/catch_amalgamated.cpp` is absent;
- `../../../src/main/main.cpp` contains a hard-coded Windows data path;
- `../../../pyproject.toml` still contains PDM metadata while `../../../uv.lock` exists;
- no `scikit-build-core`, pybind11 extension, Strategy API, Result object, or `backtest.run()` exists.

## 2. Useful existing foundation

Keep and evolve rather than discarding:

- `LimitOrderBook`: per-instrument historical L3 reconstruction;
- `LobRouter`: per-instrument routing concept;
- `HistoricalLOB`: shared historical state wrapper;
- `EngineView`: private own-order and private-consumption concept;
- `SimulatedLOB`: facade combining historical and private state;
- `../../SimulatedLOB_design.md`: clearly states the shared-basement/private-overlay idea;
- existing synthetic tests as seed cases.

The core architectural idea `HistoricalLOB + per-engine diff` is sound for the assignment.

## 3. Requirement coverage

| Requirement | Current repository | Status |
|---|---|---|
| Historical L3 book per instrument | `LimitOrderBook`, `LobRouter`, `HistoricalLOB` | useful foundation |
| Per-engine overlay | `EngineView` | useful foundation |
| Fill-at-touch | only at submit, one historical level | partial |
| Resting fills after later market movement | absent | missing |
| Market Data Consumer | absent | missing |
| Virtual clock and fixed latency | absent | missing |
| Atomic `processed_seq` barrier | absent | missing |
| Chronological order/cancel arrival | absent | missing |
| OrderManager / PositionKeeper | absent | missing |
| State machine and rejects | absent | missing |
| Working native tests | sources exist, dependency missing | blocked |
| Ready-signal benchmark | absent | missing |
| UV + scikit-build-core | absent/stale PDM config | missing |
| pybind11 and Strategy callbacks | absent | missing |
| Result/pandas hand-off | absent | missing |
| `backtest.run()` | absent | missing |
| End-to-end Python strategy | absent | missing |

Estimated readiness of complete HW4: approximately 15–20%.

## 4. Concrete defects in current SimulatedLOB

### Full-book copy on order placement

`EngineView::placeLimitOrder()` obtains `historical.snapshot(0)`, allocating and copying the complete aggregated book for each own order.

### One-level matching only

A buy 10 @ 102 against historical asks 5 @ 101 and 5 @ 102 fills only the first five and leaves a marketable remainder resting. This can create a crossed private view.

### No resting reevaluation

Matching is called only on placement. A passive own order remains unfilled when a later historical update moves the touch through it.

### Stale private consumption

Consumed historical quantity is keyed only by price. If the old historical order is removed and new liquidity appears at the same price, the new liquidity can remain incorrectly hidden.

### Duplicate ID replacement

The current own-order map can silently overwrite a duplicate client order ID instead of rejecting it.

### Weak fill contract

`SyntheticFill` lacks instrument ID, exchange/engine timestamps, remaining quantity, state transition, and typed side/price.

### Hot-path representation

`MarketDataEvent` stores timestamps and order IDs as strings; price is converted through `std::stod`; book prices are `double`. This is unsuitable for deterministic event ordering and fixed tick arithmetic.

### Historical partial fill

Confirmed at the reviewed baseline: `LimitOrderBook::onFill()` delegates to
`onCancel()` and always removes the complete historical order. A source partial
fill must decrement the resting quantity and remove the order only at zero.

### Whole-file ingestion and hidden input errors

`RunDataIngestionFile()` loads every parsed record into a vector and performs a
full `stable_sort`. This violates the adopted streaming-source boundary and can
mask source-order regressions instead of rejecting corrupted chronology.
Malformed JSON is printed and silently skipped, so a backtest can continue with
an incomplete book. M2A must define fail-fast parse/order behavior and stream an
already-sorted source without loading the complete dataset.

### Historical duplicate-order corruption

`LimitOrderBook::onAdd()` overwrites `orderIndex_[order_id]`, but if the same ID
is re-added at a different side or price it does not remove the old price-level
entry. The index and visible book can therefore disagree. M2A must either reject
an invalid duplicate or implement the exact idempotent replay rule without
leaving stale liquidity, with focused L3 tests.

## 5. Migration strategy

Do not rewrite everything in one PR.

1. Restore a green baseline and package skeleton.
2. Freeze new typed contracts while keeping adapters from old structs.
3. Add scheduler and ready barrier beside existing ingestion.
4. Refactor matching behind tests.
5. Add OrderManager/PositionKeeper.
6. Bind the stable API.
7. Remove deprecated adapters only after end-to-end integration.

This lets small PRs merge frequently and limits cross-agent conflicts.
