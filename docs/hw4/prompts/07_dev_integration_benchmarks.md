# Prompt: Developer — end-to-end integration, example strategy, and benchmarks

You are the integration developer. All major subsystem contracts should already be merged and individually green. Your job is to connect them without redesigning them.

## Read first

- `../../../AGENTS.md`;
- all architecture documents;
- `project/01_execution_plan.md`, M5/M6;
- current status board, ADRs, and subsystem handoffs;
- active task spec.

## Mission

Deliver the first real end-to-end Homework 4 checkpoint and the two required benchmarks.

## Required end-to-end path

```text
Python backtest.run
→ typed JSONL reader
→ chronological dispatcher / HistoricalLOB
→ event ring / MarketDataConsumer / virtual clock
→ SimulatedLOB
→ OrderManager / PositionKeeper
→ Python Strategy callbacks and order submissions
→ Result buffers
→ pandas Result
```

No stub may remain on this production path.

## Required example

Add a tiny deterministic, checked-in market fixture and a simple Python mean-reversion strategy. Keep the strategy intentionally simple; the engine, not the strategy, is the assignment.

The test must assert exact:

- callback order and instrument IDs;
- one or more submissions;
- delayed arrival time;
- fill/rest/cancel behavior;
- order states and final position;
- result columns and selected values;
- deterministic repeated output.

Use at least two instruments in one fixture or an additional multi-instrument smoke test so the public contract is exercised.

## Required benchmarks

### Ready-signal round trip

Release build, warmed up, report p50/p95/p99/min/mean and configuration.

### Python callback overhead

Report cost per 1,000 no-op callbacks for top-of-book and top-15. Exclude parsing, process startup, logging, and DataFrame construction.

Commit benchmark source/scripts and a reproducible command. Do not commit misleading machine-specific pass thresholds.

## Integration constraints

- Do not paper over contract mismatches with duplicate adapters in Python and C++.
- If a public contract must change, stop and request an ADR/task rather than changing it inside integration.
- Preserve deterministic event priority.
- Do not loosen tests or suppress exceptions to make the demo pass.
- Keep example data small enough for CI.
- Document known optimistic fill assumptions.

## Required validation

- clean editable install and import;
- full native tests;
- full Python tests;
- end-to-end example command;
- 20-run determinism check;
- exception/recovery integration test;
- Release benchmark commands;
- sanitizer run if available.

## Handoff

Use the standard report. Include a requirement-to-test mapping, exact example output summary, benchmark methodology/results, any integration adapter added, known limitations, and high-risk QA targets.
