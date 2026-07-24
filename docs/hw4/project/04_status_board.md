# Project status board

Team Lead updates this file after every handoff, QA run, review, merge, or architecture decision.

## Current baseline

| Field | Value |
|---|---|
| Upstream branch | `main` |
| HW4 integration branch | `hw4/backtest-engine-options` |
| Reviewed starting commit | `c4f4c02916f5a9fb5f2636926fd93cd28af0f46d` |
| Current integration commit | `01f7d324e724422d3b04722bb011a62afea0ef35` |
| Current milestone | M3 |
| Build status | M2B merged; clean Release build and CTest 6/6 passed; native scheduler tests 37/37 |
| Python package status | Editable reinstall/import passed; native/Python version reports `0.0.1` |

## Milestone status

| Milestone | Owner | Branch/commit | Dev | QA | Review | Merge | Notes |
|---|---|---|---|---|---|---|---|
| M0 Green baseline | M0 developer agent | `4f591440` from `3e7cdac3` | ✅ | ✅ | ✅ | ✅ | QA `PASS WITH P2`; review `APPROVE WITH FOLLOW-UPS`; no P0/P1 |
| M1 Shared contracts | M1 contracts developer | `27e91ef0` | ✅ | ✅ | ✅ | ✅ | Scheduled payload P1 fixed before merge |
| M2A Core/market | M2A core/market developer | `c51d82ee` from `27e91ef` | ✅ | ✅ | ✅ | ✅ | Re-QA PASS 6/6 + adversarial harness; re-review APPROVE; no P0/P1 |
| M2B Scheduler | Scheduler developer | `01f7d324` from `50036d8` | ✅ | ✅ | ✅ | ✅ | QA PASS after DateRange P1 fix; review APPROVE, no P0/P1 |
| M2C Python stub | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M3 Trading/matching | Trading developer | `tasks/M3_trading_matching_state.md` from `01f7d324` | 🟡 | ⬜ | ⬜ | ⬜ | Native path only; no Python/results |
| M4A Results/PnL | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M4B Python integration | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M5 End-to-end | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |
| M6 Benchmarks/hardening | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |

Legend: ⬜ not started · 🟡 active · ✅ passed · ❌ failed · ⏸ blocked.

## Active tasks

| Task ID | Scope | Agent | Base | Owned files | Dependencies | State | Blocker |
|---|---|---|---|---|---|---|---|
| M3-001 | Native consumer, matching, order state, positions | Trading developer | `01f7d324` | `src/trading`, trading tests, minimal CMake registration | M2A + M2B merged | Active | None |

## Open findings

| Finding | Severity | Source | Owner | Target task | State |
|---|---|---|---|---|---|
| M0-QA-001 | P2 | QA + Review | M1/follow-up owner | Version identity | Open |
| M2A-REV-003 | P2 | Review | M3 owner | Validate positive `contract_multiplier` before accounting | Open |
| M2B-REV-001 | P2 | Review | M3 owner | Enforce unique monotonic producer command sequences | Open |
| M2B-REV-002 | P2 | Review | Later runtime hardening | Document or linearize general SPSC `close()` versus in-flight push | Deferred |

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
| M2B-001 | `01f7d324`; native 37/37, Release CTest 6/6, ASan/UBSan + TSan PASS | PASS after DateRange P1 fix; full adversarial matrix | APPROVE WITH P2; no P0/P1 | Fast-forwarded |
