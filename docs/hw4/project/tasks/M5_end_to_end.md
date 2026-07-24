# Task M5-001 — end-to-end checkpoint

## Identity

- **Milestone:** M5 end-to-end
- **Base:** `hw4/backtest-engine-options` /
  `9c7967a0210e10009c39d1d50951efe8d4ec81da`
- **Prompt:** `docs/hw4/prompts/07_dev_integration_benchmarks.md`
- **Owner:** integration developer agent

## Scope

Add a small checked-in two-instrument JSONL fixture and a simple Python
mean-reversion example that exercises the already merged production path:

```text
JSONL → historical store → scheduler → trading engine → Python Strategy
→ orders/fills/positions → Result
```

Do not redesign public contracts, matching, accounting, scheduler priority, or
the Python API. M6 owns benchmark reporting and broad hardening; M5 may only
repair defects required to make the real checkpoint correct.

## Required behavior

1. Exact callback order and instrument IDs are asserted.
2. At least one order arrives after configured positive latency, rests, and
   fills on a later market change.
3. A cancel path reaches its documented terminal state.
4. Final positions, selected PnL/fill/order-log values, columns, and dtypes are
   exact.
5. Twenty runs produce identical normalized callbacks and results.
6. A callback exception propagates and a following run succeeds.
7. One documented command runs the example from a clean editable install.

## Ownership

- Own: `examples/**`, a new tiny fixture under `test/data/**` or
  `python/tests/data/**`, focused end-to-end tests, and the matching README
  example section.
- Minimal runtime fixes are allowed only when an end-to-end test exposes a
  real defect; document each such fix and add a focused regression.
- Do not add benchmark thresholds or duplicate integration adapters.

## Gates

- clean editable install/import;
- Release native CTest and full Python tests;
- example command and exact output;
- 20-run determinism;
- ASan/UBSan attempt for any native change;
- independent adversarial QA and exact-diff review with no P0/P1.

