# Performance design and benchmarks

## 1. Performance posture

This is an HFT-school project, so the hot path should demonstrate correct low-latency engineering choices. It is not necessary to build a lock-free framework or hand-tune every cache line before correctness is established.

Optimization order:

1. remove avoidable algorithmic work and allocation;
2. use typed contiguous data;
3. establish ownership and deterministic synchronization;
4. measure Release builds;
5. optimize measured bottlenecks only.

## 2. Mandatory hot-path rules

- Parse ISO timestamps and decimal prices once at ingestion.
- Store timestamps as `int64` nanoseconds.
- Store price as integer ticks and quantity as integer.
- Use numeric IDs and typed enums.
- Do not call `std::stod`, parse JSON, format strings, or allocate Python objects in matching.
- Do not take a full-book snapshot to match one order.
- Extract only required top-N levels for callbacks.
- Use price-indexed structures to find marketable own orders.
- Pre-reserve fill, transition, PnL, and command buffers.
- Use bounded SPSC rings and an atomic ready sequence between the two mandatory threads.
- Avoid `std::function`, shared-pointer copies, and virtual dispatch inside per-level matching loops.
- Acquire the GIL only for the Python callback window.
- Do not log per event in benchmark or normal hot loops.

## 3. Practical data-structure guidance

The current `std::map`-based L3 book is acceptable for the first finished submission. Replacing it with a custom flat tree is not on the critical path.

Worth fixing now:

- full-book `snapshot(0)` on every order;
- strings and doubles in engine-facing event types;
- repeated hash/map lookups that can be cached during one match;
- unbounded vectors without reservation;
- unnecessary cross-thread locks under the strict barrier.

Not worth doing before integration:

- custom allocators everywhere;
- intrusive containers;
- manual SIMD;
- broad small-vector rewrites;
- spin loops without measured benefit.

## 4. Required ready-signal benchmark

Measure a warmed-up round trip:

```text
dispatcher publishes synthetic event
    → trading thread consumes and performs no-op reaction
    → trading thread stores processed_seq
    → dispatcher observes completion
```

Report:

- build type and compiler;
- CPU/OS summary when available;
- ring capacity and waiting strategy;
- warm-up iterations;
- measured iterations;
- p50, p95, p99, minimum, and mean nanoseconds.

Do not invent a universal pass threshold. The baseline is for regression detection and comparison of implementations on the same machine.

## 5. Required Python callback benchmark

Measure 1,000 invocations per sample for:

- no-op `on_book_update()` with top-of-book payload;
- no-op `on_book_update()` with top-15 payload;
- optionally no-op `on_fill()`.

Report native loop overhead separately if possible. Warm up Python and the binding path before timing. The benchmark must not include JSON parsing, DataFrame construction, console output, or process startup.

## 6. Regression policy

A change that degrades a relevant benchmark by more than 20% on the same machine/build requires an explanation or optimization follow-up, unless it fixes correctness. Correctness wins over benchmark numbers, but regressions must be visible.

## 7. Profiling checkpoint

Profile only after the first end-to-end run. Candidate counters:

- events per second;
- allocations per million market events;
- callback count after top-N filtering;
- number of full-book copies — target zero in matching;
- average own orders scanned per book update;
- result-buffer reallocations — target zero after warm-up/reserve estimate.
