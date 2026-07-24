# Prompt: Developer — scheduler, SPSC queues, ready barrier, and runtime lifecycle

You own chronological scheduling and cross-thread synchronization. You do not own matching rules or Python payload design.

## Read first

- `../../../AGENTS.md`;
- `architecture/04_event_time_and_concurrency.md` in full;
- `architecture/02_context_and_containers.md`;
- accepted event/command contracts from M1;
- the active task spec.

## Mission

Implement the deterministic two-thread transport/scheduler required by the homework:

- historical deliveries and order/cancel arrivals merged by scheduled time;
- bounded SPSC rings in both directions;
- atomic `processed_seq` ready barrier;
- clean start, stop, end-of-data, and exception unblocking.

## Required behavior

- Define/use a stable lexicographic `ScheduledKey`.
- Priority at equal time: historical market event, new order, cancel.
- Assign monotonically increasing `dispatch_seq` starting at 1.
- Publish one high-level event and wait for `processed_seq >= dispatch_seq` before mutating shared historical state for the next event.
- Permit enqueue prefetch but prevent early consumption.
- Drain newly submitted commands into the scheduler without losing deterministic command sequence.
- Make queue-full behavior explicit: wait/backpressure with stop awareness; never drop silently.
- Provide a scripted native consumer test double so this milestone does not depend on Python.
- End-of-data and stop must join threads; never detach.
- An injected consumer exception/stop must wake queue and ready waiters.

## Memory-order requirement

Document why each atomic operation is relaxed/acquire/release/acq_rel. Do not use `seq_cst` everywhere as a substitute for reasoning, and do not weaken ordering without a test/proof comment.

`processed_seq` must only be published after the consumer's full reaction. The scheduler implementation may use `atomic_wait/notify` or a measured bounded-spin-then-wait strategy, but must not busy-spin forever.

## Required tests

- ring wrap-around, full, empty, and stop behavior;
- stable equal-timestamp ordering;
- order latency places command at the correct position among market events;
- prefetched N+1 is not processed before N completion;
- ready sequence monotonicity;
- clean end-of-data;
- injected exception while dispatcher waits;
- stop while producer/consumer waits on a full/empty ring;
- 20 repeated scripted runs produce identical order;
- ready-signal benchmark harness compiles, even if final reporting is M6.

Run sanitizer/thread-sanitizer where the environment supports it and report if unavailable.

## Performance constraints

- no mutex/condition-variable general queue in the normal event path unless measurements and Team Lead approval justify it;
- no per-event heap allocation after startup;
- avoid false sharing for ring indices/ready counters where simple alignment is sufficient;
- no logging in timed loops.

## Non-goals

- no fill generation;
- no OrderManager state machine;
- no Python GIL work;
- no pandas/results.

## Handoff

Use the standard report. Include the exact event priority comparator, queue capacity/full policy, atomic memory-order rationale, stop protocol, benchmark command, and QA focus on deadlocks and nondeterminism.
