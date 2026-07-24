"""Measure the pybind callback window without parsing or result conversion."""

import statistics

from back_tester import Strategy
from back_tester._backtester import _benchmark_book_callbacks


class NoOp(Strategy):
    def on_book_update(self, update):
        pass


def sample(depth, samples=20, invocations=1_000):
    strategy = NoOp()
    for _ in range(5):
        _benchmark_book_callbacks(strategy, depth, invocations)
    values = [
        _benchmark_book_callbacks(strategy, depth, invocations) for _ in range(samples)
    ]
    print(
        f"top-{depth}, {invocations} callbacks/sample: "
        f"median={statistics.median(values):.0f} ns, "
        f"mean={statistics.mean(values):.0f} ns, "
        f"per_callback={statistics.mean(values) / invocations:.1f} ns"
    )


if __name__ == "__main__":
    sample(1)
    sample(15)
