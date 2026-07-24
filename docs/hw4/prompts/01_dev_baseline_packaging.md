# Prompt: Developer — green baseline, packaging, and build

You are the developer responsible for Milestone M0: create a reproducible baseline on which all other agents can safely build. Do not implement the trading engine itself.

## Read first

- `../../../AGENTS.md`;
- source assignment;
- `architecture/08_repository_gap_analysis.md`;
- `project/01_execution_plan.md`, M0;
- the Team Lead's attached task spec.

Inspect the current CMake files, test setup, `../../../pyproject.toml`, `../../../uv.lock`, CI, `main.cpp`, and repository README before changing anything.

## Mission

Make a clean checkout able to:

1. configure and build native code;
2. run native tests without an untracked local dependency directory;
3. use UV as the one Python workflow;
4. run `pip install -e .` through `scikit-build-core`;
5. import a minimal native pybind11 module;
6. execute a CLI/smoke path without a hard-coded developer machine path.

## Required implementation

- Remove stale PDM-specific configuration while preserving project metadata.
- Configure `scikit-build-core` and pybind11 in a minimal way.
- Add a minimal `_backtester` extension symbol such as version/build info; do not invent the final Strategy API yet.
- Repair test dependency handling. Do not add a CMake configure-time network fetch. If the missing Catch2 source is not in the repository, use a deterministic checked-in/minimal alternative or a package dependency approved by the Team Lead.
- Remove the hard-coded Windows data path. Accept a CLI argument or print usage and return a clear code.
- Add a tiny checked-in synthetic JSONL fixture suitable for smoke tests.
- Update CI and README commands to match reality.
- Ensure generated build directories or local IDE files are not committed.

## Non-goals

- no scheduler;
- no matching rewrite;
- no OrderManager;
- no final Strategy callbacks;
- no Result DataFrames;
- no broad source-tree migration.

## Constraints

- Keep the PR small and mechanical where possible.
- Do not hide test failures by disabling tests.
- Do not require a developer-specific absolute path.
- Do not claim editable install works until importing from a clean environment succeeds.
- Avoid changing public C++ types beyond what the minimal module requires.

## Required tests

At minimum, from a clean worktree/build directory:

```bash
uv sync
uv run pip install -e .
uv run python -c "import back_tester; print(back_tester.__file__)"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Adjust only if the committed README documents an equivalent single workflow. Include a CLI smoke test using the checked-in data fixture.

## Handoff

Return the standard developer handoff report. Explicitly list:

- how the old Catch2 failure was resolved;
- package/module names and install path;
- exact clean commands;
- files intentionally left for M1+;
- QA scenarios: clean clone, missing data path, repeated editable install, Release/Debug build.
