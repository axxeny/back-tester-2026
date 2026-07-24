# Native result ownership and PnL

`ResultRecorder` is a `trading::Recorder` implementation plus an explicit
`on_book_mark()` input. It consumes decisions made by the trading engine; it
does not match orders or create transitions.

Each public result field has one typed `std::vector`. All affected vectors are
reserved before a row is committed, so allocation failure cannot expose a
partial row. Position changes are calculated in a staged FIFO ledger and are
committed only after checked arithmetic and result-capacity preparation
succeed.

For a closed FIFO lot, realized PnL is:

```text
(exit_ticks - entry_ticks) * signed_closed_quantity
    * contract_multiplier / price_scale
```

An open lot marked at the exact midpoint uses:

```text
((bid_ticks + ask_ticks) / 2 - entry_ticks) * signed_open_quantity
    * contract_multiplier / price_scale
```

Every multiplication, addition, and rational normalization is checked before
state mutation. Missing-sided books retain the last valid mark. Fill samples
and changed marks at the same engine timestamp replace the last aggregate
sample.

During a run, PnL remains a reduced `int64 numerator / positive int64
denominator`. `freeze()` performs the only bulk conversion to `double`, marks
the storage immutable, and returns `FrozenResults`:

```text
ResultRecorder --shared ownership--> immutable native column storage
                                      ^
FrozenResults -----------------------|
```

The read-only spans remain valid while any copied `FrozenResults` handle is
alive, even after the recorder and engine are destroyed. M4A creates no Python,
NumPy, pandas, or Arrow objects and therefore makes no Python zero-copy claim.
