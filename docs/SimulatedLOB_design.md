# Simulated LOB with independent engine-private overlays

## Core design decision

The implementation uses **one shared `HistoricalLOB` plus per-engine diffs**, not N full LOB copies.

Why:

- The historical L3 book can be very large, so copying it for every strategy engine would multiply memory by N.
- The engine-specific state is usually much smaller: its own synthetic resting orders and the historical liquidity that this engine has already consumed.
- This gives the desired view: `SimulatedLOB = HistoricalLOB - EngineView.consumedHistoricalLiquidity + EngineView.ownSyntheticOrders`.

## Main classes

- `HistoricalLOB`: thread-safe wrapper around the existing reconstructed `LimitOrderBook`; this is the shared market basement.
- `EngineView`: private overlay for one trading engine. It stores only synthetic orders and private historical-liquidity consumption.
- `SimulatedLOB`: small facade binding one `HistoricalLOB` to one `EngineView`; this is what a strategy should read/trade against.

## Fill model implemented now

When an engine sends a limit order:

- Buy fills if `limitPrice >= private historical best ask`.
- Sell fills if `limitPrice <= private historical best bid`.
- Fill price is the historical touch price.
- Fill size is `min(order size, visible size at historical touch)`.
- Filled historical liquidity is removed only from this engine's private view.
- Remaining quantity rests as this engine's synthetic order.

This is intentionally the simplest touch-fill model. It does not yet sweep through multiple historical levels.

## Thread-safety

- `HistoricalLOB` uses `std::shared_mutex`: many engine views can snapshot/read concurrently; L3 replay updates take an exclusive lock.
- Each `EngineView` uses its own `std::mutex`, so different engines do not block each other except on short shared historical snapshots.
- Other engines never see another engine's synthetic orders or private consumed liquidity.
