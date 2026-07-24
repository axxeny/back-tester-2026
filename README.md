# CMF Advanced Backtesting Engine for Options

## Prerequisites

- a C++20 compiler;
- [UV](https://docs.astral.sh/uv/).

UV is the repository's environment and dependency workflow. The locked
development environment includes CMake and Ninja, so no project-specific
system CMake installation is required.

## Set up and install

Create the locked environment and build the editable native Python extension:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync --locked
uv run pip install -e .
uv run python -c "import back_tester; print(back_tester.__file__); print(back_tester.version())"
```

The distribution name is `back-tester-cmf`. Its import package is
`back_tester`, which loads `back_tester._backtester`.

## Python strategy API

`backtest.run()` drives the real streaming JSONL reader, historical book,
scheduler, trading engine, and native result recorder:

```python
from back_tester import DateRange, Side, Strategy, backtest


class BuyTouch(Strategy):
    def on_book_update(self, update):
        if update.asks:
            self.submit_limit(
                update.instrument_id, Side.BUY, update.asks[0].price, 1
            )


result = backtest.run(BuyTouch(), "data/sample.mbo.jsonl", DateRange())
print(result.fills_df)
print(result.order_log_df)
print(result.pnl_series)
```

The three-argument form discovers only the unique numeric instrument IDs in a
metadata pass and then replays the file as a stream. It uses Databento
nanounits (`price_scale=1_000_000_000`, tick size 1, multiplier 1). Pass an
explicit `instruments=[InstrumentMeta(...)]` list for a one-pass replay and
real tick sizes or option multipliers.

The optional `BacktestConfig` defaults to zero market-data latency, a strictly
positive one-nanosecond order latency, and top-15 callbacks. Strategy context
methods are intentionally available only while a callback is active. Result
DataFrames and the PnL Series are built in bulk from frozen typed native
columns; callback-scoped book levels are copied into immutable Python-owned
payloads.

## Native build and tests

Configure a clean Release build with tests enabled:

```bash
uv run cmake -S . -B build-m0 -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
uv run cmake --build build-m0 -j
uv run ctest --test-dir build-m0 --output-on-failure
uv run pytest -q python/tests
uv run python python/benchmarks/callback_overhead.py
```

The test executable uses a small checked-in test runner. Native test
configuration does not download dependencies and does not require anything in
`3rdparty/`.

## CLI smoke run

The CLI requires exactly one data path. With no path it prints usage and exits
with status 64:

```bash
build-m0/bin/back-tester
```

Run ingestion against the deterministic checked-in fixture:

```bash
build-m0/bin/back-tester test/data/tiny_mbo.jsonl
```

A missing or unreadable path exits with status 2.

## Development checks

Install the pre-commit hooks or run all configured formatters and linters:

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

The hooks:

- format and lint C++ code with `clang-format`;
- format and lint Python code with `ruff`;
- strip outputs from Jupyter notebooks.
