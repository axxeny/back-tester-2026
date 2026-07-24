# Task M6-001 — benchmarks, hardening, and submission

## Identity

- **Milestone:** M6 final hardening
- **Base:** `hw4/backtest-engine-options` /
  `a04ff0f6e27f74d6fe5077e2fc394516ffeff02b`
- **Prompt:** `docs/hw4/prompts/07_dev_integration_benchmarks.md`
- **Owner:** hardening developer agent

## Scope

Turn the merged implementation into a reproducible HW4 submission. Finish and
run the two required Release benchmarks, close documentation/command gaps, and
fix only defects exposed by final clean-build, sanitizer, determinism, or
benchmark validation. Do not optimize without evidence or redesign contracts.

## Required deliverables

1. Ready-signal round-trip benchmark reports warm-up/measured iterations,
   capacity/waiting strategy, compiler/build/OS/CPU when available, and
   min/mean/p50/p95/p99 nanoseconds.
2. Python callback benchmark measures 1,000 no-op callbacks per sample for
   top-1 and top-15, excludes parsing/startup/DataFrame work, and reports native
   loop overhead separately when practical.
3. README gives exact clean sync/install/import, Release build/CTest, Python
   tests, example, and both benchmark commands.
4. Final limitations state the adopted optimistic fill model and remaining
   supported/out-of-scope behavior without hiding failures.
5. Requirements traceability points to the final tests/commands.

## Gates

- fresh environment/editable install and import;
- clean Release build, native CTest, full Python suite, and example;
- 20-run end-to-end determinism;
- ASan/UBSan and TSan where supported;
- both Release benchmarks execute and publish reproducible output;
- changed-file hooks/diff check;
- independent final QA and architecture/performance review, no P0/P1.

