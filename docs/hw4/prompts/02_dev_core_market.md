# Prompt: Developer — core types, ingestion, and historical market book

You own the typed core and market-data foundation after the shared contracts are frozen. Your code feeds the scheduler and trading engine; it must not depend on Python or order-management behavior.

## Read first

- `../../../AGENTS.md`;
- `architecture/01_scope_and_decisions.md`;
- `architecture/03_components_and_boundaries.md`;
- `architecture/04_event_time_and_concurrency.md` sections on source events;
- `architecture/08_repository_gap_analysis.md`;
- the active task spec and merged M1 contract headers.

Inspect `../../../src/common/BasicTypes.hpp`, `MarketDataEvent.*`, `LimitOrderBook.*`, `LobRouter.*`, and current tests.

## Mission

Provide a deterministic typed source-event stream and correct per-instrument historical book without loading/copying more data than necessary.

## Required behavior

- Parse timestamps once into `TimestampNs` and prices once into `PriceTicks`.
- Validate decimal precision against instrument tick/scale without relying on binary floating comparison.
- Preserve source sequence and `F_LAST`/atomic-group information.
- Support streamed iteration over one chronologically ordered JSONL source; detect and report ordering regressions.
- Expose a minimal source iterator/record API that a chronological merger can consume.
- Maintain a `HistoricalLOBStore` keyed by `instrument_id`.
- Correctly handle source Add, Cancel, Modify, Fill, Trade, and Clear semantics.
- Source partial fills decrement historical order quantity and remove only at zero.
- Provide top-N aggregated views without full-book copy when only N is requested.
- Maintain side/price level revisions sufficient for EngineView private-consumption invalidation.
- Keep the existing shared HistoricalLOB/private overlay idea compatible.

## Performance constraints

- No `std::stod`, ISO parsing, or string IDs after ingestion.
- Do not read the complete input into a vector merely to sort it in the normal path.
- Do not create a full snapshot to answer best bid/ask or top-N.
- Avoid Python and pandas dependencies.
- `std::map` is acceptable for the first submission; do not build a custom tree unless measured.

## Compatibility strategy

Prefer incremental adapters over a giant rewrite. Existing code may continue to accept legacy `MarketDataEvent` temporarily, but the scheduler/trading contracts must use the new typed event. Remove duplicated definitions and document deprecation.

## Required tests

- exact decimal-to-ticks cases, including negative/invalid precision;
- timestamp ordering and equal-timestamp sequence;
- add/cancel/modify/partial-fill/full-fill/clear;
- duplicate/add-idempotency policy;
- multiple instruments do not mix;
- top-N ordering and empty sides;
- level revision changes when old liquidity is replaced at the same price;
- large streamed fixture does not require whole-file sorting;
- malformed row produces a typed error with location/context.

Run full native tests after targeted tests.

## Non-goals

- no order arrival scheduler;
- no own-order matching;
- no Python callbacks;
- no PnL;
- no queue-position modeling.

## Handoff

Use the standard report. Include public interfaces introduced, migration/adapters, parsing assumptions, complexity of key operations, exact tests, and QA focus on partial fills, revisions, ordering regressions, and malformed data.
