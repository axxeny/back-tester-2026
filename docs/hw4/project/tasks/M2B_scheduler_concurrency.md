# Task M2B-001 — deterministic scheduler and ready barrier

## Identity

- **Milestone:** M2B Scheduler and synchronization
- **Base branch/commit:** `hw4/backtest-engine-options` /
  `c51d82ee78b5f3647afd4997fb68081233d62c93`
- **Assigned role prompt:** `docs/hw4/prompts/03_dev_scheduler_concurrency.md`
- **Owner:** scheduler developer agent

## Scope

Implement the native two-thread scheduling foundation without matching, Python,
or result behavior:

- bounded allocation-free SPSC rings for engine events and order commands;
- lexicographic scheduling by time, frozen priority, and stable sequence;
- `dispatch_seq` starting at 1 and an atomic `processed_seq` barrier;
- stop-aware backpressure and clean normal/failure shutdown;
- a scripted native consumer and a compilable Release benchmark harness.

The dispatcher may prefetch an event, but it must not let the consumer process
event `N+1` before event `N` has completed. Commands created while reacting to
an event enter the same deterministic future-event scheduler and are never
processed recursively.

## Ownership

- Own: new `src/scheduler/**`, `test/SchedulerTest.cpp`, and scheduler benchmark
  source.
- May edit only to register owned sources/targets: `src/CMakeLists.txt`,
  `test/CMakeLists.txt`, and the smallest relevant CMake file.
- Must not edit: `src/core/**`, `src/market/**`, legacy matching/trading code,
  Python/package files, public enums, event payloads, or result schemas.

## Required behavior

1. Equal scheduled timestamps order market, new order, cancel, then stable
   source/command sequence.
2. Market delivery time is the already typed `MarketDelivery.engine_ts_ns`;
   commands use `scheduled_arrival_ts_ns`; no wall clock enters ordering.
3. The dispatcher assigns strictly increasing nonzero dispatch sequences.
4. `processed_seq` is release-published only after the scripted consumer's
   complete reaction and command enqueue; dispatcher observes it with acquire.
5. Queue full/empty waits observe stop and never drop, reorder, or spin forever.
6. End-of-data joins both threads. An injected consumer failure stores the first
   exception, wakes ring/barrier waiters, joins, and rethrows to the caller.
7. The production API owns no Python objects and generates no fills.

## Acceptance

- ring empty/full/wrap-around/backpressure and stop-unblock tests;
- exact equal-time priority and stable tie-break tests;
- order-latency placement among market events;
- controlled proof that prefetched `N+1` is not processed before `N` ack;
- monotonic ready sequence and clean end-of-data;
- injected failure while dispatcher or either ring endpoint waits;
- 20 identical scripted runs;
- fresh Release build and full CTest pass;
- ASan/UBSan or TSan attempted when supported, with unsupported tooling reported;
- ready round-trip benchmark target compiles and runs outside normal CTest.

## Performance and design constraints

- Fixed capacity and no per-event allocation in ring push/pop.
- No mutex/condition-variable queue in the normal event path.
- Memory orders are documented next to the atomics; do not use blanket
  sequential consistency.
- No logging in event or benchmark loops.
- Keep the API small enough for M3 to attach the real market consumer.

## QA focus

Force capacity-one/tiny rings, sequence wrap boundaries where meaningful,
equal-time insertion in different orders, stop at every wait point, consumer
throw before acknowledgement, and repeated-run determinism.

## Done

Developer tests pass, independent QA finds no P0/P1, independent review finds no
P0/P1, and the candidate fast-forwards into the HW4 integration branch.
