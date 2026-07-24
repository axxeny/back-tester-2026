# Shared native contracts

## 1. Contract location and compatibility

The dependency-free native contract is declared once under `src/core/`.
`core/Contracts.hpp` is the convenience include for the complete contract.
`common/BasicTypes.hpp` remains a compatibility include for legacy targets; it
aliases old names to the core definitions and does not redeclare `Side`,
`Quantity`, `ClOrdId`, or the other public numeric types.

Legacy LOB structures continue to store source prices as `double` until their
owned migration tasks. New event-loop contracts use integer `PriceTicks`.

## 2. Numeric and enum encodings

Timestamps, price ticks, quantity, and instrument IDs are signed 64-bit
integers. Client order IDs, exchange order IDs, and deterministic sequences are
unsigned 64-bit integers.

All enum result columns store the enum's fixed-width underlying value:

| Enum | Underlying type | Stable encoding |
|---|---|---|
| `Side` | `int8` | `Sell=-1`, `None=0`, `Buy=1` |
| `OrderState` | `uint8` | `PendingNew=0`, `Open=1`, `PartiallyFilled=2`, `Filled=3`, `PendingCancel=4`, `Cancelled=5`, `Rejected=6` |
| `RejectReason` | `uint8` | `None=0`, `UnknownInstrument=1`, `InvalidSide=2`, `NonPositiveQuantity=3`, `InvalidPrice=4`, `TickMisalignment=5`, `DuplicateClientOrderId=6`, `UnsupportedOrderType=7`, `UnsupportedTimeInForce=8`, `UnknownOrder=9`, `AlreadyTerminal=10` |
| `EventPriority` | `uint8` | `MarketData=0`, `NewOrder=1`, `Cancel=2` |
| `CommandType` | `uint8` | `NewOrder=0`, `Cancel=1` |
| `OrderLogEventType` | `uint8` | `Submit=0`, `Accepted=1`, `Fill=2`, `CancelRequest=3`, `Cancelled=4`, `Reject=5` |
| `LiquiditySource` | `uint8` | `HistoricalDisplayed=0` |

These encodings are public serialization/result contracts. Additions or changes
require the decision-change process.

## 3. Callback contract

A book callback fires after the complete atomic historical group (for example,
Databento `F_LAST`) and only when the configured top-N view changed. The
default depth is 15; internal replay remains full L3.

For one high-level market event, callback and state order is:

1. match resting private orders against the stable historical book;
2. update order, position, and result state, then call `on_fill()`;
3. call `on_trade()` in stable source order;
4. call `on_book_update()` when the top-N view changed;
5. enqueue commands created by callbacks;
6. publish `processed_seq`.

`BookUpdateView` contains non-owning spans. They are valid only for the duration
of the callback and must be copied by a consumer that needs a longer lifetime.
Other callback and command payloads are numeric value types with no owning
strings or Python objects.

## 4. Range and terminal policies

`DateRange` includes historical source records whose exchange timestamp equals
either the start or end. A command arrival at the end timestamp may execute; an
arrival strictly after the end timestamp does not execute.

If a cancel arrives after its order has already reached terminal `Filled`
state, it produces a reject with `RejectReason::AlreadyTerminal`. It is not a
silent no-op.

## 5. Results boundary

`FillResultRow`, `OrderLogResultRow`, and `PnlPoint` declare the native values
and exact enum storage used by the result schemas. Buffer allocation,
ownership, NumPy/Arrow lifetime retention, and zero-copy conversion are M4
implementation concerns. M1 creates no Python objects and makes no zero-copy
claim.
