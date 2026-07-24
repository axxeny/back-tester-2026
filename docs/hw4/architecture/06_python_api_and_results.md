# Python Strategy API, pybind11 boundary, and results

## 1. Python package surface

Recommended user-facing API:

```python
from back_tester import BacktestConfig, DateRange, Side, Strategy, backtest

class MyStrategy(Strategy):
    def on_book_update(self, update):
        ...

    def on_trade(self, trade):
        ...

    def on_fill(self, fill):
        ...

    def on_reject(self, reject):
        ...

result = backtest.run(
    strategy=MyStrategy(),
    data_path="data/sample.mbo.jsonl",
    date_range=DateRange(...),
    config=BacktestConfig(
        market_data_latency_ns=50_000,
        order_latency_ns=25_000,
        book_depth=15,
    ),
)
```

`config` may be optional to preserve the assignment's minimal three-argument form, but latency and callback settings must not be hidden globals.

## 2. Strategy context

The Strategy object must be able to call, directly or through a bound context:

```python
client_order_id = self.submit_limit(instrument_id, side, price, quantity)
accepted = self.cancel_order(client_order_id)
position = self.position(instrument_id)
orders = self.open_orders(instrument_id)
now_ns = self.now_ns
```

All queries return immutable snapshots/views. Python must not retain references into mutable native containers after the callback ends unless the wrapper owns a copy.

## 3. Callback payloads

Suggested native contracts:

```cpp
struct BookUpdateView {
    InstrumentId instrument_id;
    TimestampNs exchange_ts_ns;
    TimestampNs engine_ts_ns;
    std::uint64_t sequence;
    bool is_snapshot;
    std::span<const BookLevel> bids;
    std::span<const BookLevel> asks;
};

struct TradeView {
    InstrumentId instrument_id;
    TimestampNs exchange_ts_ns;
    TimestampNs engine_ts_ns;
    std::uint64_t sequence;
    Side aggressor_side;
    PriceTicks price;
    Quantity quantity;
};

struct FillView {
    InstrumentId instrument_id;
    ClOrdId client_order_id;
    Side side;
    PriceTicks price;
    Quantity quantity;
    Quantity remaining_quantity;
    TimestampNs exchange_ts_ns;
    TimestampNs engine_ts_ns;
    std::uint64_t fill_sequence;
};

struct RejectView {
    InstrumentId instrument_id;
    ClOrdId client_order_id;
    RejectReason reason;
    TimestampNs engine_ts_ns;
};
```

Python may expose prices as integer ticks plus instrument metadata, or as a lightweight decimal conversion outside the hot path. The native contract remains fixed-point.

## 4. GIL discipline

`backtest.run()` should release the GIL while native threads run. The Trading Engine reacquires it only around each Python callback or strategy API interaction that requires Python objects.

Required pattern:

1. validate and retain the Python strategy object;
2. release the GIL before starting/waiting for native work;
3. acquire the GIL immediately before invoking an override;
4. translate/capture any Python exception;
5. release the GIL immediately after the callback;
6. never wait on queues, atomics, or thread joins while holding the GIL.

A C++ no-op Strategy implementation should exist for native tests and to benchmark the engine without Python.

## 5. Result native storage

During the run, append only to pre-reserved typed C++ buffers.

### `fills_df`

| Column | Native/Python dtype |
|---|---|
| `exchange_ts_ns` | `int64` |
| `engine_ts_ns` | `int64` |
| `instrument_id` | `int64` |
| `client_order_id` | `uint64` |
| `side` | `int8` |
| `price_ticks` | `int64` |
| `quantity` | `int64` |
| `remaining_quantity` | `int64` |
| `liquidity_source` | `uint8` |

### `order_log_df`

One row per transition:

| Column | Meaning |
|---|---|
| `engine_ts_ns` | time visible to engine |
| `instrument_id` | instrument |
| `client_order_id` | client order |
| `event_type` | submit, accepted, fill, cancel request, cancelled, reject |
| `state` | resulting state |
| `side` | buy/sell |
| `limit_price_ticks` | limit price |
| `order_quantity` | original quantity |
| `filled_quantity` | cumulative filled quantity |
| `remaining_quantity` | current remainder |
| `reject_reason` | typed code, zero/none when not rejected |

### `pnl_series`

- index: `engine_ts_ns`;
- value: aggregate total PnL in account currency units;
- mark: midpoint when both sides exist;
- if no valid midpoint exists, retain the previous valid mark;
- fees default to zero;
- apply `InstrumentMeta.contract_multiplier`;
- sample on fill and on a book update that changes a held instrument's mark; coalesce duplicate timestamps deterministically.

## 6. Bulk conversion and lifetime

Acceptable:

- NumPy arrays that reference immutable native buffers owned by the returned `Result` wrapper;
- Arrow arrays built from compatible buffers;
- one bulk copy where pandas dtype/lifetime requirements make zero-copy unsafe.

Not acceptable:

- constructing Python dictionaries per event;
- appending rows to pandas DataFrames during the run;
- exposing a NumPy view to freed or mutable C++ storage;
- claiming zero-copy without an ownership test.

The returned `Result` object must keep native buffers alive as long as any exposed array/DataFrame can reference them.

## 7. Callback exception test

An integration test must raise a Python exception from each callback type and verify:

- `backtest.run()` returns by raising that exception;
- no thread remains alive;
- no barrier deadlock occurs;
- the process can run a second clean backtest afterward.
