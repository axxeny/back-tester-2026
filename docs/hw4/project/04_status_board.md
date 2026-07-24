# Project status board

Team Lead updates this file after every handoff, QA run, review, merge, or architecture decision.

## Current baseline

| Field | Value |
|---|---|
| Upstream branch | `main` |
| HW4 integration branch | `hw4/backtest-engine-options` |
| Reviewed starting commit | `c4f4c02916f5a9fb5f2636926fd93cd28af0f46d` |
| Current integration commit | `9c7967a0210e10009c39d1d50951efe8d4ec81da` |
| Current milestone | M5 |
| Build status | M4B merged; Release CTest 6/6, ASan/UBSan 6/6, TSan 1/1 |
| Python package status | Real runtime path merged; Python suite 18/18; native/Python version `0.0.1` |

## Milestone status

| Milestone | Owner | Branch/commit | Dev | QA | Review | Merge | Notes |
|---|---|---|---|---|---|---|---|
| M0 Green baseline | M0 developer agent | `4f591440` from `3e7cdac3` | ✅ | ✅ | ✅ | ✅ | QA `PASS WITH P2`; review `APPROVE WITH FOLLOW-UPS`; no P0/P1 |
| M1 Shared contracts | M1 contracts developer | `27e91ef0` | ✅ | ✅ | ✅ | ✅ | Scheduled payload P1 fixed before merge |
| M2A Core/market | M2A core/market developer | `c51d82ee` from `27e91ef` | ✅ | ✅ | ✅ | ✅ | Re-QA PASS 6/6 + adversarial harness; re-review APPROVE; no P0/P1 |
| M2B Scheduler | Scheduler developer | `01f7d324` from `50036d8` | ✅ | ✅ | ✅ | ✅ | QA PASS after DateRange P1 fix; review APPROVE, no P0/P1 |
| M2C Python stub | Team Lead disposition | superseded | — | — | — | — | Real M3 engine exists; bind directly in M4B instead of building throwaway stub |
| M3 Trading/matching | Trading developer | `2fa28d04` from `cb74071` | ✅ | ✅ | ✅ | ✅ | Re-QA PASS; re-review APPROVE; no P0/P1/P2 |
| M4A Results/PnL | Results developer | `1a097e56` from `0984521` | ✅ | ✅ | ✅ | ✅ | Performance P1 fixed; re-QA PASS; re-review APPROVE |
| M4B Python integration | Python/runtime developer | `9c7967a0` | ✅ | ✅ | ✅ | ✅ | Two P1 review findings fixed; final QA/review PASS; no P0/P1/P2 |
| M5 End-to-end | Integration developer | `tasks/M5_end_to_end.md` from `9c7967a0` | 🟡 | ⬜ | ⬜ | ⬜ | Two-instrument deterministic checkpoint |
| M6 Benchmarks/hardening | _assign_ | | ⬜ | ⬜ | ⬜ | ⬜ | |

Legend: ⬜ not started · 🟡 active · ✅ passed · ❌ failed · ⏸ blocked.

## Active tasks

| Task ID | Scope | Agent | Base | Owned files | Dependencies | State | Blocker |
|---|---|---|---|---|---|---|---|
| M5-001 | Deterministic two-instrument Python checkpoint | Integration developer | `9c7967a0` | examples, tiny fixture, E2E tests, README | M4B merged | Active | None |

## Open findings

| Finding | Severity | Source | Owner | Target task | State |
|---|---|---|---|---|---|
| M0-QA-001 | P2 | QA + Review | M4B owner | Version identity | Closed in M4B (`0.0.1`) |
| M2A-REV-003 | P2 | Review | M3 owner | Validate positive `contract_multiplier` before accounting | Closed in M3 |
| M2B-REV-001 | P2 | Review | M3 owner | Enforce unique monotonic producer command sequences | Closed in M3 |
| M2B-REV-002 | P2 | Review | Later runtime hardening | Document or linearize general SPSC `close()` versus in-flight push | Deferred |
| M3-M4-NOTE | P2 | Review | M4B owner | Python optional config must select a positive order-latency default | Closed in M4B |

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
| M3-001 | `2fa28d04`; native 49/49, Release CTest 6/6, ASan/UBSan + TSan PASS | PASS after checked-accounting/latency fixes; full external harness | APPROVE; no P0/P1/P2 | Fast-forwarded |
| M4A-001 | `1a097e56`; native 57/57, Release CTest 6/6, ASan/UBSan PASS | PASS; exact/lifetime/performance external harnesses | APPROVE after O(N²) ledger-copy fix; no P0/P1 | Fast-forwarded |
| M4B-001 | `9c7967a0`; Release CTest 6/6, Python 18/18, ASan/UBSan + TSan PASS | PASS; causality, callback recursion, GIL/context and reuse adversarial coverage | APPROVE; no P0/P1/P2 | Fast-forwarded |
