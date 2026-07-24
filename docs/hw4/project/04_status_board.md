# Project status board

Team Lead updates this file after every handoff, QA run, review, merge, or architecture decision.

## Current baseline

| Field | Value |
|---|---|
| Upstream branch | `main` |
| HW4 integration branch | `hw4/backtest-engine-options` |
| Reviewed starting commit | `c4f4c02916f5a9fb5f2636926fd93cd28af0f46d` |
| Current integration commit | `4f5914406cda03f336e5cf9cd0e33b7cc00eddbf` |
| Current milestone | M1 |
| Build status | M0 merged; clean Release/Debug builds and CTest 3/3 passed through locked `uv run cmake` / `uv run ctest` |
| Python package status | M0 merged; repeated editable install and import outside the repository passed |

## Milestone status

| Milestone | Owner | Branch/commit | Dev | QA | Review | Merge | Notes |
|---|---|---|---|---|---|---|---|
| M0 Green baseline | M0 developer agent | `4f591440` from `3e7cdac3` | ✅ | ✅ | ✅ | ✅ | QA `PASS WITH P2`; review `APPROVE WITH FOLLOW-UPS`; no P0/P1 |
| M1 Shared contracts | M1 contracts developer | `agent/m1-shared-contracts` from `4f591440` | 🟡 | ⬜ | ⬜ | ⬜ | Brief `tasks/M1_shared_contracts.md` |
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
| M1-001 | Freeze native/Python shared contracts | M1 contracts developer | `4f591440` | `src/core`, compatibility type header, contract tests | M0 merged | Active | None |

## Open findings

| Finding | Severity | Source | Owner | Target task | State |
|---|---|---|---|---|---|
| M0-QA-001 | P2 | QA + Review | M1/follow-up owner | Version identity | Open |

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

## Completed gate evidence

| Task | Developer | QA | Review | Integration |
|---|---|---|---|---|
| M0-001 | `4f591440`; UV/install/import, Release/Debug, CTest 3/3 PASS | `PASS WITH P2`; clean exact candidate, 20x CLI and CTest determinism | `APPROVE WITH FOLLOW-UPS`; no P0/P1 | Fast-forwarded to `hw4/backtest-engine-options` |
