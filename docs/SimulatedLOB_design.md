# Typed SimulatedLOB and engine-private overlays

## Single fill authority

The production matcher is `cmf::trading::SimulatedLOB` in
`src/trading/SimulatedLOB.*`. It is the only component that decides synthetic
fill price and quantity. `TradingEngine` owns lifecycle transitions, positions,
recording, and callbacks, and applies the ordered `SyntheticFill` decisions
returned by `SimulatedLOB`.

The old `src/main` double/string matcher was removed from the build and source.
Native synthetic tests and Python end-to-end runs now exercise the same typed
implementation.

## State and ownership

`HistoricalLOBStore` owns the shared typed L3 replay. A `SimulatedLOB` owns one
typed `EngineView`, which contains:

- this engine's resting-order price/time indexes;
- this engine's private historical-liquidity consumption, keyed by instrument,
  historical order ID, side, and liquidity revision.

Synthetic fills never mutate the shared historical book. Separate
`EngineView`s therefore see independent private consumption.

## Matching

An accepted buy sweeps visible historical asks from best to worse while
`ask_price <= limit_price`; a sell similarly sweeps bids while
`bid_price >= limit_price`. Fills use the historical order price, support
partial quantities and multiple levels, and leave any remainder resting.
Resting own orders are reevaluated after each atomic market group in price/time
FIFO order. Own orders never self-match.

The matching loop is numeric and typed: integer ticks, quantities, IDs, and
revisions. It visits historical liquidity directly without a full-book
snapshot.

## Concurrency boundary

The scheduler's strict ready barrier keeps `HistoricalLOBStore` stable while
the Trading Engine calls `SimulatedLOB`. The dispatcher mutates the next market
group only after `processed_seq` acknowledges all matching, state updates, and
callbacks for the current event. The mandatory one-engine path therefore needs
no matcher-side book mutex.
