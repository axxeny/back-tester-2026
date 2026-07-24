# Prompt: Developer — pybind11 Strategy API and GIL discipline

You own the Python Strategy boundary. You may begin against a stub context after M1, then integrate with the real TradingEngine/OrderManager in a later narrowly scoped task.

## Read first

- `../../../AGENTS.md`;
- `architecture/06_python_api_and_results.md`;
- `architecture/01_scope_and_decisions.md` callback decisions;
- merged M1 contract headers;
- current pyproject/CMake/package skeleton;
- active task spec.

## Mission

Expose a minimal, stable Python API in which a Python subclass drives the native engine through callbacks and per-instrument order/state queries, with correct GIL and exception behavior.

## Required API

Bind at least:

- `Side`, order states, reject reasons, and relevant typed IDs/views;
- `BacktestConfig` and `DateRange` or equivalent;
- Strategy base/trampoline callbacks:
  - `on_book_update(update)`;
  - `on_trade(trade)`;
  - `on_fill(fill)`;
  - `on_reject(reject)`;
- strategy context methods:
  - `submit_limit(instrument_id, side, price, quantity)`;
  - `cancel_order(client_order_id)`;
  - `position(instrument_id)`;
  - `open_orders(instrument_id)`;
  - current engine time.

Every callback payload carries instrument, exchange time, engine time, and sequence. Book depth payload is callback-scoped; do not expose a dangling span.

## GIL requirements

- `backtest.run()` releases the GIL during native work.
- Reacquire only around Python override invocation or required Python object access.
- Do not hold GIL while waiting on queues, ready sequences, or joins.
- Capture Python exceptions and route them through the engine's stop/unblock protocol.
- After failure, a second backtest must be able to run in the same process.

## Stub phase

When the real engine is not merged, implement a narrow `IStrategyContext` stub that exercises callbacks and API shape. Do not duplicate engine state or invent different method signatures. Replace the stub through dependency injection at the boundary, not by changing public Python methods later.

## Required tests

- Python subclass receives each callback with correct fields;
- top-1 and top-15 book payloads preserve order and lifetime during callback;
- multi-instrument position/open-order query signatures;
- submission/cancel call through the context stub/real implementation;
- Python exception from each callback propagates and leaves no blocked thread;
- object lifetime: Strategy is held during run but released afterward;
- no callback occurs without GIL;
- no GIL held during a measurable native wait;
- repeated run in one interpreter;
- import surface and type repr/error messages are usable.

## Non-goals

- do not implement matching in bindings;
- do not create pandas rows per callback;
- do not expose mutable references to native containers;
- do not add notebooks/UI/frameworks.

## Handoff

Use the standard report. Include Python API examples, C++ interface/trampoline shape, GIL windows, exception path, payload lifetime rules, exact tests, and QA focus on reentrancy, second-run behavior, and dangling views.
