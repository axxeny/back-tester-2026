# Project status board

Team Lead updates this file after every handoff, QA run, review, merge, or architecture decision.

## Current baseline

| Field | Value |
|---|---|
| Upstream branch | `main` |
| HW4 integration branch | `hw4/backtest-engine-options` |
| Reviewed starting commit | `c4f4c02916f5a9fb5f2636926fd93cd28af0f46d` |
| Current integration commit | `c51d82ee78b5f3647afd4997fb68081233d62c93` |
| Current milestone | M2B |
| Build status | M2A merged; clean Release build and CTest 6/6 passed through locked `uv run cmake` / `uv run ctest` |
| Python package status | Editable reinstall/import passed; native/Python version reports `0.0.1` |

## Milestone status

| Milestone | Owner | Branch/commit | Dev | QA | Review | Merge | Notes |
|---|---|---|---|---|---|---|---|
| M0 Green baseline | M0 developer agent | `4f591440` from `3e7cdac3` | ✅ | ✅ | ✅ | ✅ | QA `PASS WITH P2`; review `APPROVE WITH FOLLOW-UPS`; no P0/P1 |
| M1 Shared contracts | M1 contracts developer | `27e91ef0` | ✅ | ✅ | ✅ | ✅ | Scheduled payload P1 fixed before merge |
| M2A Core/market | M2A core/market developer | `c51d82ee` from `27e91ef` | ✅ | ✅ | ✅ | ✅ | Re-QA PASS 6/6 + adversarial harness; re-review APPROVE; no P0/P1 |
| M2B Scheduler | Scheduler developer | `tasks/M2B_scheduler_concurrency.md` from `c51d82ee` | 🟡 | ⬜ | ⬜ | ⬜ | Narrow implementation opened |
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
| M2B-001 | Deterministic scheduler, SPSC rings, ready barrier, lifecycle | Scheduler developer | `c51d82ee` | `src/scheduler`, scheduler tests/benchmark, minimal CMake registration | M1 + M2A merged | Active | None |

## Open findings

| Finding | Severity | Source | Owner | Target task | State |
|---|---|---|---|---|---|
| M0-QA-001 | P2 | QA + Review | M1/follow-up owner | Version identity | Open |
| M2A-REV-003 | P2 | Review | M3 owner | Validate positive `contract_multiplier` before accounting | Open |

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
| M1-001 | `27e91ef`; contract and compatibility tests PASS | PASS after scheduled-payload fix | APPROVE; no P0/P1 | Fast-forwarded |
| M2A-001 | `c51d82e`; Release, native 23/23, CTest 6/6, import PASS | PASS; P0/P1/P2=0; independent identity/decimal harness | APPROVE; no P0/P1 | Fast-forwarded |
