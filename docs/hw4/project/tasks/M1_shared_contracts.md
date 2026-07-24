# Task M1-001 — frozen shared contracts

## Identity

- **Task ID:** M1-001
- **Title:** Freeze native/Python shared contracts
- **Milestone:** M1 Shared contracts
- **Base branch/commit:** `hw4/backtest-engine-options` /
  `4f5914406cda03f336e5cf9cd0e33b7cc00eddbf`
- **Assigned role prompt:** `docs/hw4/prompts/11_generic_task_prompt.md`
- **Owner:** M1 contracts developer

## Context

M0 established a reproducible native/Python build. Before market, scheduler,
trading, result, and Python agents can work independently, they need one numeric
and behavioral contract. The current `BasicTypes.hpp` still uses floating
quantity/price aliases, while `BookTypes.hpp`, `MarketDataEvent`, and
`SyntheticFill` are legacy structures that must remain buildable until their
own migration tasks.

Applicable sources and decisions:

- `docs/hw4/source/01_homework_4_assignment.md`;
- `docs/hw4/architecture/01_scope_and_decisions.md`, D2–D8 and D11–D14;
- `docs/hw4/architecture/03_components_and_boundaries.md`;
- `docs/hw4/architecture/04_event_time_and_concurrency.md`;
- `docs/hw4/architecture/05_order_matching_and_state.md`;
- `docs/hw4/architecture/06_python_api_and_results.md`;
- `docs/hw4/project/03_decision_log.md`.

## Scope

### Must implement

- add one dependency-free `cmf` core contract area under `src/core/`;
- define fixed-width numeric aliases for timestamp, price ticks, quantity,
  instrument ID, client order ID, exchange order ID, and deterministic sequence;
- define strongly typed `Side`, order state, reject reason, event priority,
  command type, order-log event type, and liquidity source enums with explicit
  stable underlying values;
- define `InstrumentMeta`, `BacktestConfig`, and `DateRange`;
- define immutable/value contracts for book levels, book/trade/fill/reject
  callback payloads, new/cancel commands, scheduled keys/events, order query
  rows, position snapshots, and the three result schemas;
- keep `instrument_id`, exchange/engine timestamps, and deterministic sequence
  on every strategy-facing event where the adopted architecture requires them;
- provide compatibility includes/aliases from `src/common/BasicTypes.hpp`
  without creating a second definition of public enums or numeric aliases;
- document callback order, top-N atomic-group granularity, date-range
  inclusivity, end-of-range command policy, cancel-after-fill rule, and enum
  encodings in a concise contract note;
- add compile-time/runtime contract tests for widths, signedness, enum values,
  default config, ordering key, and aggregate construction.

### Must not implement

- JSON/timestamp/decimal parsing or historical-book fixes;
- queues, threads, dispatcher, ready barrier, or matching;
- OrderManager behavior, PnL calculations, Python callbacks/bindings, pandas,
  or `backtest.run()`;
- conversion of legacy `LimitOrderBook`, `SimulatedLOB`, or ingestion hot paths;
- a broad source-tree move.

### Owned files/directories

- new `src/core/`;
- `src/common/BasicTypes.hpp` and `src/common/CMakeLists.txt` only for
  compatibility exposure;
- `src/CMakeLists.txt` only if required to expose a core target/include path;
- new contract tests plus the explicit test source list in
  `test/CMakeLists.txt`;
- a new contract note under `docs/hw4/architecture/`;
- `docs/hw4/project/03_decision_log.md` only to resolve the five documented
  contract gaps without changing accepted ADRs.

### Files requiring Team Lead approval

- root `CMakeLists.txt`, `pyproject.toml`, `uv.lock`;
- `src/main/` implementation and legacy `BookTypes.hpp`;
- Python package/bindings;
- accepted decisions D1–D14.

## Required behavior and interfaces

1. Hot-path aliases are exact-width integers: signed 64-bit timestamps, price,
   quantity, and instrument IDs; unsigned 64-bit client/exchange IDs and
   sequences.
2. `Side` retains `None=0`, `Buy=1`, `Sell=-1` compatibility.
3. `OrderState` contains exactly `PendingNew`, `Open`, `PartiallyFilled`,
   `Filled`, `PendingCancel`, `Cancelled`, and `Rejected`.
4. `EventPriority` sorts market before new order before cancel at equal time;
   `ScheduledKey` comparison is lexicographic by scheduled time, priority, then
   stable source/command sequence.
5. Default config has top-N depth 15 and explicit fixed market/order latency
   fields; invalid configuration is representable but validation remains a
   later runtime concern.
6. Callback views use numeric fields only. Book depth may use callback-scoped
   spans, but the contract note must state their lifetime.
7. Commands are value types and carry submit time, scheduled arrival time, and
   monotonically increasing command sequence. Submission is distinct from
   arrival.
8. Result schema declarations exactly match the columns and dtypes in
   `architecture/06_python_api_and_results.md`; no Python object is present.
9. Legacy native targets and all four existing M0 native cases continue to
   compile and pass without converting legacy doubles during M1.
10. Resolve documentation gaps with these defaults unless an existing ADR
    already says otherwise:
    - `DateRange` is inclusive at start and end for historical source records;
    - command arrivals strictly after the end are not executed;
    - cancel arrival after a terminal fill produces a typed reject;
    - result enum storage uses the enum's explicit fixed-width underlying type;
    - buffer ownership/zero-copy implementation remains M4, not M1.

## Acceptance tests

1. Static assertions prove exact widths, signedness, trivial copyability where
   intended, and explicit enum encodings.
2. A scheduled-key test sorts equal/different timestamps and proves
   market → new → cancel plus stable sequence order.
3. Aggregate construction tests cover every callback payload and command with
   non-zero `instrument_id` and timestamps.
4. Config/default and date-range policy tests lock the documented decisions.
5. Existing M0 native tests and Python import still pass.
6. No public core header includes JSON, pybind11, pandas, legacy LOB containers,
   mutexes, or owning strings in event-loop payloads.

## Required commands

```bash
uv sync --locked
uv run pip install -e .
uv run python -c "import back_tester; print(back_tester.version())"
uv run cmake -S . -B build-m1 -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
uv run cmake --build build-m1 -j
uv run ctest --test-dir build-m1 --output-on-failure
uv run pre-commit run --all-files
```

## Performance constraints

- core headers contain numeric/value contracts only;
- no JSON, strings, Python objects, virtual functions, shared pointers, or
  allocations in event/command records;
- no benchmark is required for M1.

## Deliverables

- implementation and developer-authored contract tests;
- contract/ADR documentation described above;
- standard nine-point developer handoff;
- committed candidate on `agent/m1-shared-contracts`.

## QA focus

- ABI widths and enum encodings across AppleClang/GCC where available;
- lexicographic priority and sequence ties;
- zero/negative/default values and overflow-adjacent construction;
- accidental duplicate definitions between `BasicTypes.hpp` and `src/core`;
- legacy M0 compilation and Python import;
- forbidden heavyweight includes or owning hot-path fields.

## Done condition

M1 is developer-complete when the exact candidate is clean, all commands
available on the host pass, every public type is declared once, contract tests
lock the adopted values/policies, and no runtime subsystem behavior was added.
Merge still requires independent QA and review with no unresolved P0/P1.
