# Architecture index

These files define the target implementation for Homework 4. They intentionally resolve ambiguities in the source PDF so that independent agents can build compatible components.

1. [`01_scope_and_decisions.md`](01_scope_and_decisions.md) — scope, non-goals, and frozen project decisions.
2. [`02_context_and_containers.md`](02_context_and_containers.md) — context, process/thread model, and high-level sequence.
3. [`03_components_and_boundaries.md`](03_components_and_boundaries.md) — module ownership, dependencies, and proposed repository layout.
4. [`04_event_time_and_concurrency.md`](04_event_time_and_concurrency.md) — event timeline, ordering, queues, ready barrier, shutdown, and error propagation.
5. [`05_order_matching_and_state.md`](05_order_matching_and_state.md) — fill model, private liquidity, cancellation, and order state machine.
6. [`06_python_api_and_results.md`](06_python_api_and_results.md) — callbacks, strategy context, pybind11/GIL rules, result schemas, and PnL.
7. [`07_performance.md`](07_performance.md) — hot-path constraints and benchmark methodology.
8. [`08_repository_gap_analysis.md`](08_repository_gap_analysis.md) — reviewed baseline and concrete gaps in the teammate's implementation.
9. [`09_testing_and_acceptance.md`](09_testing_and_acceptance.md) — test matrix and final HW4 definition of done.

## Status of decisions

The decisions are project-level defaults chosen to complete the assignment. When an instructor or team agreement provides a different answer, update the decision log first, then change code and tests together.
