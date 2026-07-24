# Task M0-001 — reproducible green baseline

## Identity

- **Task ID:** M0-001
- **Title:** Reproducible native/Python baseline
- **Milestone:** M0 Green baseline
- **Base branch/commit:** `hw4/backtest-engine-options` (created from
  `main`) / `c4f4c02916f5a9fb5f2636926fd93cd28af0f46d`
- **Assigned role prompt:** `docs/hw4/prompts/01_dev_baseline_packaging.md`
- **Owner:** M0 developer agent

## Context

The reviewed repository builds no Python extension, relies on an absent untracked
Catch2 amalgamation, retains stale PDM metadata, and starts the CLI with a
developer-specific Windows data path. M1 and later engine work must not start
until the baseline can be reproduced from a clean worktree.

Applicable decisions and requirements:

- `docs/hw4/source/01_homework_4_assignment.md`, Group B Packaging;
- `docs/hw4/architecture/08_repository_gap_analysis.md`;
- `docs/hw4/architecture/09_testing_and_acceptance.md`, section 5;
- `docs/hw4/project/01_execution_plan.md`, milestone M0.

## Scope

### Must implement

- select UV as the only documented environment workflow and remove PDM config;
- configure `scikit-build-core` and a minimal pybind11 `_backtester` module;
- expose the native module through an importable `back_tester` Python package;
- make native tests self-contained without configure-time network fetching;
- replace the hard-coded CLI path with an argument/usage contract;
- add a tiny checked-in deterministic JSONL smoke fixture;
- align README and CI commands with the real clean build/install workflow;
- preserve C++20 and warnings-as-errors.

### Must not implement

- scheduler, ready barrier, matching changes, OrderManager, Strategy callbacks,
  Result/PnL, or source-tree migration.

### Owned files/directories

- `pyproject.toml`, `uv.lock`;
- root/package-related `CMakeLists.txt` files;
- `src/python/` and `python/back_tester/` for the minimal module only;
- `test/CMakeLists.txt` and a minimal checked-in test framework replacement;
- `src/main/main.cpp`;
- `test/data/` or `data/test/` smoke fixture;
- `.github/workflows/ci.yml`, `README.md`, `.gitignore` as required by M0.

### Files requiring Team Lead approval

- `src/common/BasicTypes.hpp`;
- existing market/matching implementation;
- architecture decisions and public HW4 contracts.

## Required behavior

1. Running the CLI without a path prints usage and returns a documented non-zero
   code without dereferencing `argv[1]`.
2. Running it with the checked-in fixture invokes ingestion without any absolute
   developer path.
3. Native tests configure and run from a clean checkout without an untracked
   `3rdparty` directory or configure-time download.
4. `uv sync`, editable installation, and `import back_tester` succeed; the import
   exposes one minimal native version/build-info symbol.
5. Repeating editable installation does not depend on stale generated package
   metadata checked into `src/`.

## Acceptance tests

1. Clean Release configure/build with tests enabled.
2. `ctest --output-on-failure` discovers and passes native tests.
3. UV sync plus editable installation/import succeeds.
4. CLI no-argument and checked-in-fixture smoke paths behave as documented.
5. Git status after tests contains no generated files that M0 expects committed.

## Required commands

```bash
uv sync
uv run pip install -e .
uv run python -c "import back_tester; print(back_tester.__file__); print(back_tester.version())"
cmake -S . -B build-m0 -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
cmake --build build-m0 -j
ctest --test-dir build-m0 --output-on-failure
build-m0/bin/back-tester
build-m0/bin/back-tester test/data/tiny_mbo.jsonl
```

Equivalent symbol or fixture names are acceptable if README records them.

## Performance constraints

- no engine hot-path changes;
- no CMake configure-time network fetch;
- keep the extension and test dependency setup minimal.

## Deliverables

- implementation and focused tests;
- updated clean build/install documentation and CI;
- standard handoff report with exact command results;
- a commit on `agent/m0-green-baseline`.

## QA focus

- clean worktree without `3rdparty`;
- Debug and Release builds;
- repeated editable install;
- import outside the repository root;
- CLI missing path, nonexistent path, and tiny fixture;
- no hidden skipped tests.

## Done condition

M0 is developer-complete only when all commands available on the developer
machine pass and the handoff identifies any environment-only command that could
not be run. Merge readiness additionally requires independent QA and review with
no unresolved P0/P1 finding.
