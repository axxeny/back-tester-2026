"""Deterministic options back-testing API backed by the native runtime."""

from ._backtester import (
    BacktestConfig,
    BookLevel,
    BookUpdate,
    DateRange,
    Fill,
    InstrumentMeta,
    OpenOrder,
    OrderState,
    Position,
    Reject,
    RejectReason,
    Result,
    Side,
    Strategy,
    Trade,
    run,
    version,
)


class _Backtest:
    """Namespace preserving the documented ``backtest.run(...)`` entry point."""

    run = staticmethod(run)


backtest = _Backtest()

__all__ = [
    "BacktestConfig",
    "BookLevel",
    "BookUpdate",
    "DateRange",
    "Fill",
    "InstrumentMeta",
    "OpenOrder",
    "OrderState",
    "Position",
    "Reject",
    "RejectReason",
    "Result",
    "Side",
    "Strategy",
    "Trade",
    "backtest",
    "run",
    "version",
]
