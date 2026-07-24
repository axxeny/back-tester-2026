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
`back_tester`, which loads the minimal native module `back_tester._backtester`.

## Native build and tests

Configure a clean Release build with tests enabled:

```bash
uv run cmake -S . -B build-m0 -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
uv run cmake --build build-m0 -j
uv run ctest --test-dir build-m0 --output-on-failure
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
