# Python Strategy API and result ownership

## Public package

The distribution name is `back-tester-cmf`; the import package is
`back_tester`.

```python
from back_tester import (
    BacktestConfig,
    DateRange,
    InstrumentMeta,
    Side,
    Strategy,
    backtest,
)


class BuyTouch(Strategy):
    def on_book_update(self, update):
        if update.asks:
            self.submit_limit(
                update.instrument_id,
                Side.BUY,
                update.asks[0].price,
                1,
            )


result = backtest.run(
    BuyTouch(),
    "test/data/tiny_mbo.jsonl",
    DateRange(),
    BacktestConfig(
        market_data_latency_ns=0,
        order_latency_ns=1,
        book_depth=15,
    ),
    [
        InstrumentMeta(
            instrument_id=1,
            tick_size_ticks=1,
            price_scale=1_000_000_000,
            contract_multiplier=1,
        )
    ],
)
```

The last two arguments are optional. Without explicit instruments, the binding
performs a metadata discovery pass and uses Databento nanounits, tick size 1,
and multiplier 1. Explicit metadata enables a one-pass replay and real
instrument tick sizes/multipliers.

## Strategy callbacks

Subclass `Strategy` and override any of:

```python
def on_book_update(self, update): ...
def on_trade(self, trade): ...
def on_fill(self, fill): ...
def on_reject(self, reject): ...
```

Payloads are immutable Python-visible objects.

| Payload | Important fields |
|---|---|
| `BookUpdate` | `instrument_id`, exchange/engine time, sequence, snapshot flag, bids, asks |
| `Trade` | instrument, exchange/engine time, sequence, aggressor side, price, quantity |
| `Fill` | instrument, client order ID, side, price, quantity, remaining quantity, times, sequence |
| `Reject` | instrument, client order ID, reason, times, sequence |

The native `BookUpdateView` contains callback-scoped spans. The binding copies
its top-N levels into an owned `BookUpdate` before calling Python, so Python may
safely retain the payload. Other callback payloads are numeric value types.

## Strategy context

The bound Strategy object exposes:

```python
client_order_id = self.submit_limit(instrument_id, side, price_ticks, quantity)
accepted = self.cancel_order(client_order_id)
position = self.position(instrument_id)
orders = self.open_orders(instrument_id)
now_ns = self.now_ns
```

These methods are valid only on the callback thread while a callback is active.
Calling them before a run, after a callback returns, or from another Python
thread raises an error. `open_orders()` returns Python-owned rows rather than
references into mutable native indexes.

## GIL and exception handling

The binding:

1. validates and retains the Python Strategy object;
2. releases the GIL before discovery/native execution;
3. reacquires it immediately around each Python callback;
4. activates the Strategy context only for that callback;
5. releases it again before native scheduling, waits, or joins.

A Python exception leaves the callback, is captured by the native runtime,
requests stop, wakes both queues and the ready barrier, joins both native
threads, and is rethrown by `backtest.run()`. The same Strategy object cannot
run concurrently. A new run remains possible after a failed run.

## Native result storage

`ResultRecorder` writes structure-of-arrays columns during the run and
`freeze()` converts them into immutable reference-counted storage.

### `fills_df`

| Column | dtype |
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

| Column | dtype / meaning |
|---|---|
| `engine_ts_ns` | `int64` callback-visible time |
| `instrument_id` | `int64` |
| `client_order_id` | `uint64` |
| `event_type` | `uint8` stable enum encoding |
| `state` | `uint8` resulting state |
| `side` | `int8` |
| `limit_price_ticks` | `int64` |
| `order_quantity` | `int64` |
| `filled_quantity` | `int64` cumulative fill |
| `remaining_quantity` | `int64` |
| `reject_reason` | `uint8` stable enum encoding |

### `pnl_series`

- index name and dtype: `engine_ts_ns`, `int64`;
- value name and dtype: `total_pnl`, `float64`;
- mark: midpoint when both sides exist;
- missing side: keep the last valid mark;
- sampling: fills and mark-changing book updates for held instruments;
- equal timestamps: deterministically coalesced;
- contract multiplier and price scale: applied before the final `double`
  conversion.

Realized accounting closes FIFO lots. Native arithmetic preserves an exact
rational numerator/denominator and checks overflow before conversion to
`float64`.

## NumPy/pandas ownership

Each NumPy array points directly at one frozen native column and carries a
capsule holding a shared owner. Pandas objects are created in bulk with
`copy=False`; there is no per-row Python append path.

The native storage remains alive as long as an exposed array, DataFrame, Series,
or Result wrapper retains it. Result buffers are immutable after `freeze()`.
