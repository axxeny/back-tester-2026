# Project status board

Team Lead updates this file after every handoff, QA run, review, merge, or architecture decision.

## Current baseline

| Field | Value |
|---|---|
| Upstream branch | `main` |
| HW4 integration branch | `hw4/backtest-engine-options` |
| Reviewed starting commit | `c4f4c02916f5a9fb5f2636926fd93cd28af0f46d` |
| Current integration commit | `c4f4c02916f5a9fb5f2636926fd93cd28af0f46d` |
| Current milestone | M0 |
| Build status | Local audit host has no `cmake` in `PATH`; tests also require absent `3rdparty/Catch2/extras/catch_amalgamated.cpp` |
| Python package status | `uv sync` passes; editable install fails at dependency resolution and no native extension exists |

## Milestone status

| Milestone | Owner | Branch/commit | Dev | QA | Review | Merge | Notes |
|---|---|---|---|---|---|---|---|
| M0 Green baseline | M0 developer agent | `agent/m0-green-baseline` from `c4f4c029` | 🟡 | ⬜ | ⬜ | ⬜ | Baseline audit complete; implementation brief `tasks/M0_green_baseline.md` |
| M1 Shared contracts | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M2A Core/market | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M2B Scheduler | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M2C Python stub | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M3 Trading/matching | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M4A Results/PnL | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M4B Python integration | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M5 End-to-end | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M6 Benchmarks/hardening | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |

Legend: ⬜ not started · 🟡 active · ✅ passed · ❌ failed · ⏸ blocked.

## Active tasks

| Task ID | Scope | Agent | Base | Owned files | Dependencies | State | Blocker |
|---|---|---|---|---|---|---|---|
| M0-001 | Reproducible native/Python baseline | M0 developer agent | `c4f4c029` | Packaging/CMake/tests/CLI/CI/README | None | Active | Team Lead host lacks CMake; developer must report its environment |

## Open findings

| Finding | Severity | Source | Owner | Target task | State |
|---|---|---|---|---|---|
| | | QA / Review | | | |

## Recent decisions

| ADR | Summary | Date | Dependents notified? |
|---|---|---|---|
| | | | |

## Integration risks

| Risk | Probability | Impact | Mitigation | Owner |
|---|---:|---:|---|---|
| Public contracts drift between native and Python branches | High | High | Merge M1 first; one contract owner | Team Lead |
| Build baseline remains machine-specific | High | High | M0 clean-worktree QA gate | M0 owner |
| Matching refactor breaks existing shared-overlay semantics | Medium | High | Preserve and extend existing tests | Trading owner |
| GIL/exception path deadlocks ready barrier | Medium | High | Dedicated failure integration tests | Python + Scheduler owners |
