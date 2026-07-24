import ctypes
import gc
import json
import threading
import weakref

import numpy as np
import pytest

import back_tester as bt


def row(
    sequence,
    *,
    instrument=1,
    timestamp="1970-01-01T00:00:00.000000100Z",
    action="A",
    side="B",
    price="100",
    size=10,
    order_id=None,
    flags=128,
):
    value = {
        "ts_recv": timestamp,
        "hd": {"ts_event": timestamp, "instrument_id": instrument},
        "action": action,
        "side": side,
        "price": price,
        "size": size,
        "flags": flags,
        "sequence": sequence,
    }
    if order_id is not None:
        value["order_id"] = str(order_id)
    elif action not in {"T", "R"}:
        value["order_id"] = str(sequence)
    return value


def write_rows(tmp_path, rows, name="events.jsonl"):
    path = tmp_path / name
    path.write_text(
        "".join(json.dumps(value) + "\n" for value in rows), encoding="utf-8"
    )
    return path


def metadata(*ids):
    return [bt.InstrumentMeta(instrument_id=value) for value in ids]


def empty_strategy_run(path, strategy=None, *, config=None, instruments=None):
    strategy = strategy or bt.Strategy()
    return bt.run(
        strategy,
        str(path),
        bt.DateRange(),
        config,
        instruments,
    )


def test_top_depth_trade_filter_owned_payload_and_multi_instrument(tmp_path):
    rows = []
    for level in range(16):
        rows.append(
            row(
                len(rows) + 1,
                price=str(100 - level),
                size=level + 1,
                flags=0,
                order_id=1000 + level,
            )
        )
        rows.append(
            row(
                len(rows) + 1,
                side="A",
                price=str(101 + level),
                size=level + 2,
                flags=128 if level == 15 else 0,
                order_id=2000 + level,
            )
        )
    rows.append(
        row(
            len(rows) + 1,
            timestamp="1970-01-01T00:00:00.000000200Z",
            action="T",
            side="B",
            price="101",
            size=3,
        )
    )
    rows.append(
        row(
            len(rows) + 1,
            instrument=2,
            timestamp="1970-01-01T00:00:00.000000300Z",
            side="A",
            price="50",
            size=7,
        )
    )
    path = write_rows(tmp_path, rows)

    class Capture(bt.Strategy):
        def __init__(self):
            super().__init__()
            self.books = []
            self.trades = []
            self.gil = []

        def on_book_update(self, update):
            self.gil.append(bool(ctypes.pythonapi.PyGILState_Check()))
            self.books.append(update)
            assert (
                self.position(update.instrument_id).instrument_id
                == update.instrument_id
            )
            assert self.open_orders(update.instrument_id) == []

        def on_trade(self, trade):
            self.gil.append(bool(ctypes.pythonapi.PyGILState_Check()))
            self.trades.append(trade)

    strategy = Capture()
    empty_strategy_run(
        path,
        strategy,
        config=bt.BacktestConfig(book_depth=15),
        instruments=metadata(1, 2),
    )
    assert [book.instrument_id for book in strategy.books] == [1, 2]
    assert len(strategy.books[0].bids) == 15
    assert len(strategy.books[0].asks) == 15
    assert [level.price for level in strategy.books[0].bids[:3]] == [
        100_000_000_000,
        99_000_000_000,
        98_000_000_000,
    ]
    assert [level.price for level in strategy.books[0].asks[:3]] == [
        101_000_000_000,
        102_000_000_000,
        103_000_000_000,
    ]
    assert len(strategy.trades) == 1
    assert strategy.trades[0].sequence == 33
    assert all(strategy.gil)
    gc.collect()
    assert strategy.books[0].bids[0].quantity == 1


def test_real_submission_delayed_fill_state_and_bulk_results(tmp_path):
    path = write_rows(
        tmp_path,
        [
            row(1, side="B", price="99", size=8, flags=0, order_id=10),
            row(2, side="A", price="101", size=4, order_id=11),
        ],
    )

    class Buy(bt.Strategy):
        def __init__(self):
            super().__init__()
            self.callbacks = []
            self.order_id = None

        def on_book_update(self, update):
            self.callbacks.append(("book", update.engine_ts_ns))
            self.order_id = self.submit_limit(
                update.instrument_id, bt.Side.BUY, 101_000_000_000, 6
            )
            assert (
                self.open_orders(update.instrument_id)[0].state
                == bt.OrderState.PENDING_NEW
            )

        def on_fill(self, fill):
            self.callbacks.append(("fill", fill.engine_ts_ns))
            assert fill.engine_ts_ns == 105
            assert self.position(fill.instrument_id).net_quantity == 4
            assert self.open_orders(fill.instrument_id)[0].remaining_quantity == 2

    strategy = Buy()
    result = empty_strategy_run(
        path,
        strategy,
        config=bt.BacktestConfig(order_latency_ns=5, book_depth=1),
        instruments=metadata(1),
    )
    assert strategy.callbacks == [("book", 100), ("fill", 105)]
    fills = result.fills_df
    orders = result.order_log_df
    assert fills["quantity"].tolist() == [4]
    assert fills["remaining_quantity"].tolist() == [2]
    assert orders["event_type"].tolist() == [0, 1, 2]
    assert list(fills.columns) == [
        "exchange_ts_ns",
        "engine_ts_ns",
        "instrument_id",
        "client_order_id",
        "side",
        "price_ticks",
        "quantity",
        "remaining_quantity",
        "liquidity_source",
    ]
    assert fills.dtypes.astype(str).tolist() == [
        "int64",
        "int64",
        "int64",
        "uint64",
        "int8",
        "int64",
        "int64",
        "int64",
        "uint8",
    ]
    assert result.pnl_series.index.dtype == np.dtype("int64")
    assert result.pnl_series.dtype == np.dtype("float64")

    retained = fills["quantity"].to_numpy(copy=False)
    assert not retained.flags.owndata
    assert type(retained.base.base).__name__ == "PyCapsule"
    del result, fills
    gc.collect()
    assert retained.tolist() == [4]


def test_three_argument_fallback_discovers_ids_and_uses_positive_latency(tmp_path):
    path = write_rows(
        tmp_path,
        [
            row(1, side="B", price="99", size=8, flags=0, order_id=10),
            row(2, side="A", price="101", size=1, order_id=11),
        ],
    )

    class DefaultConfig(bt.Strategy):
        def __init__(self):
            super().__init__()
            self.book_time = None
            self.fill_time = None

        def on_book_update(self, update):
            self.book_time = update.engine_ts_ns
            self.submit_limit(1, bt.Side.BUY, 101_000_000_000, 1)

        def on_fill(self, fill):
            self.fill_time = fill.engine_ts_ns

    strategy = DefaultConfig()
    result = bt.backtest.run(strategy, str(path), bt.DateRange())
    assert strategy.fill_time == strategy.book_time + 1
    assert result.fills_df["instrument_id"].tolist() == [1]


def test_context_reject_and_cancel_are_non_recursive(tmp_path):
    path = write_rows(
        tmp_path,
        [
            row(1, side="B", price="99", size=8, flags=0, order_id=10),
            row(2, side="A", price="101", size=4, order_id=11),
            row(
                3,
                timestamp="1970-01-01T00:00:00.000000200Z",
                action="T",
                side="B",
                price="100",
                size=1,
            ),
        ],
    )

    class Commands(bt.Strategy):
        def __init__(self):
            super().__init__()
            self.order_id = None
            self.callback_depth = 0
            self.max_depth = 0
            self.rejects = []
            self.cancelled = False

        def on_book_update(self, update):
            self.callback_depth += 1
            self.max_depth = max(self.max_depth, self.callback_depth)
            self.order_id = self.submit_limit(
                update.instrument_id, bt.Side.BUY, 98_000_000_000, 2
            )
            self.submit_limit(999, bt.Side.BUY, 1, 1)
            self.callback_depth -= 1

        def on_reject(self, reject):
            self.callback_depth += 1
            self.max_depth = max(self.max_depth, self.callback_depth)
            self.rejects.append(reject.reason)
            self.callback_depth -= 1

        def on_trade(self, trade):
            self.cancelled = self.cancel_order(self.order_id)

    strategy = Commands()
    result = empty_strategy_run(
        path,
        strategy,
        config=bt.BacktestConfig(order_latency_ns=5, book_depth=1),
        instruments=metadata(1),
    )
    assert strategy.rejects == [bt.RejectReason.UNKNOWN_INSTRUMENT]
    assert strategy.max_depth == 2
    assert strategy.cancelled
    assert result.order_log_df["state"].tolist()[-1] == int(bt.OrderState.CANCELLED)


def test_delivery_callback_order_is_fills_then_trades_then_book(tmp_path):
    path = write_rows(
        tmp_path,
        [
            row(1, side="B", price="99", size=8, flags=0, order_id=10),
            row(2, side="A", price="102", size=4, order_id=11),
            row(
                3,
                timestamp="1970-01-01T00:00:00.000000200Z",
                action="M",
                side="A",
                price="101",
                size=4,
                flags=0,
                order_id=11,
            ),
            row(
                4,
                timestamp="1970-01-01T00:00:00.000000200Z",
                action="T",
                side="B",
                price="101",
                size=1,
            ),
        ],
    )

    class Ordered(bt.Strategy):
        def __init__(self):
            super().__init__()
            self.events = []
            self.submitted = False

        def on_book_update(self, update):
            self.events.append(("book", update.sequence))
            if not self.submitted:
                self.submitted = True
                self.submit_limit(1, bt.Side.BUY, 101_000_000_000, 1)

        def on_fill(self, fill):
            self.events.append(("fill", fill.sequence))

        def on_trade(self, trade):
            self.events.append(("trade", trade.sequence))

    strategy = Ordered()
    empty_strategy_run(
        path,
        strategy,
        config=bt.BacktestConfig(order_latency_ns=5, book_depth=1),
        instruments=metadata(1),
    )
    assert strategy.events == [
        ("book", 2),
        ("fill", 1),
        ("trade", 4),
        ("book", 4),
    ]


@pytest.mark.parametrize("callback", ["book", "trade", "fill", "reject"])
def test_callback_exception_stops_joins_rethrows_and_second_run(tmp_path, callback):
    path = write_rows(
        tmp_path,
        [
            row(1, side="B", price="99", size=8, flags=0, order_id=10),
            row(2, side="A", price="101", size=4, order_id=11),
            row(
                3,
                timestamp="1970-01-01T00:00:00.000000200Z",
                action="T",
                side="B",
                price="100",
                size=1,
            ),
        ],
        name=f"{callback}.jsonl",
    )

    class Failing(bt.Strategy):
        def on_book_update(self, update):
            if callback == "book":
                raise LookupError("exact callback failure")
            if callback == "fill":
                self.submit_limit(1, bt.Side.BUY, 101_000_000_000, 1)
            if callback == "reject":
                self.submit_limit(999, bt.Side.BUY, 1, 1)

        def on_trade(self, trade):
            if callback == "trade":
                raise LookupError("exact callback failure")

        def on_fill(self, fill):
            if callback == "fill":
                raise LookupError("exact callback failure")

        def on_reject(self, reject):
            if callback == "reject":
                raise LookupError("exact callback failure")

    with pytest.raises(LookupError, match="exact callback failure"):
        empty_strategy_run(
            path,
            Failing(),
            config=bt.BacktestConfig(order_latency_ns=5, book_depth=1),
            instruments=metadata(1),
        )

    clean = bt.Strategy()
    empty_strategy_run(path, clean, instruments=metadata(1))
    with pytest.raises(RuntimeError, match="only during a callback"):
        _ = clean.now_ns


def test_strategy_retained_during_run_and_collectible_after(tmp_path):
    path = write_rows(tmp_path, [row(1)])
    seen = []

    class Lifetime(bt.Strategy):
        def on_book_update(self, update):
            seen.append(update.sequence)

    strategy = Lifetime()
    reference = weakref.ref(strategy)
    empty_strategy_run(path, strategy, instruments=metadata(1))
    assert seen == [1]
    del strategy
    gc.collect()
    assert reference() is None


def test_gil_released_during_native_run(tmp_path):
    rows = [
        row(
            sequence,
            timestamp=f"1970-01-01T00:00:00.{sequence:09d}Z",
            action="T",
            side="B",
            price="100",
            size=1,
        )
        for sequence in range(1, 2001)
    ]
    path = write_rows(tmp_path, rows)
    running = True
    counter = 0

    def worker():
        nonlocal counter
        while running:
            counter += 1

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        before = counter
        empty_strategy_run(path, bt.Strategy(), instruments=metadata(1))
        assert counter > before
    finally:
        running = False
        thread.join(timeout=1)
    assert not thread.is_alive()


def test_empty_results_exact_dtypes_and_deterministic_twenty_runs(tmp_path):
    path = write_rows(tmp_path, [row(1)])
    normalized = []
    for _ in range(20):
        result = empty_strategy_run(path, instruments=metadata(1))
        normalized.append(
            (
                tuple(result.fills_df.dtypes.astype(str)),
                tuple(result.order_log_df.dtypes.astype(str)),
                tuple(result.pnl_series),
            )
        )
    assert len(set(normalized)) == 1
    fills = empty_strategy_run(path, instruments=metadata(1)).fills_df
    assert fills.empty
    assert fills["client_order_id"].dtype == np.dtype("uint64")
    assert fills["side"].dtype == np.dtype("int8")


@pytest.mark.parametrize(
    "contents, message",
    [
        ("{broken\\n", "parse error"),
        (
            json.dumps(row(2))
            + "\n"
            + json.dumps(
                row(
                    1,
                    timestamp="1970-01-01T00:00:00.000000200Z",
                )
            )
            + "\n",
            "source sequence is not strictly increasing",
        ),
        (json.dumps(row(1, flags=0)) + "\n", "unterminated atomic market group"),
    ],
)
def test_input_failures_are_not_suppressed(tmp_path, contents, message):
    path = tmp_path / "bad.jsonl"
    path.write_text(contents, encoding="utf-8")
    with pytest.raises(RuntimeError, match=message):
        empty_strategy_run(path, instruments=metadata(1))
