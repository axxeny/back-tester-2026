# 7. Тесты и benchmarks

## 7.1. Уровни проверки

Проект проверяется на нескольких уровнях:

| Уровень | Что ловит |
|---|---|
| Native unit | Ошибки структур данных и C++ логики |
| Native integration | Scheduler + TradingEngine + runtime |
| Python integration | Pybind, GIL, callbacks, pandas |
| End-to-end | Полный путь от fixture до Result |
| Determinism | Случайный порядок и race-dependent output |
| Sanitizers | Memory errors, undefined behavior, data races |
| Benchmarks | Стоимость synchronization и Python boundary |

## 7.2. Native build и CTest

```bash
uv run cmake -S . -B build-release \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON
uv run cmake --build build-release -j
uv run ctest --test-dir build-release --output-on-failure
```

CTest содержит шесть registered tests. Первый запускает большой native test
executable с тестами:

- parsing и numeric boundaries;
- HistoricalLOB;
- SPSC ring и ReadyBarrier;
- chronological ordering;
- matching и private consumption;
- state machine;
- positions и PnL;
- runtime и exception handling.

Остальные проверяют CLI и ingestion failures.

Предупреждения macOS linker про пустую static library table of contents или
duplicate library не являются test failure. Exit code сборки и результаты
CTest являются определяющими.

## 7.3. Python tests

```bash
uv run pytest -q python/tests
```

Проверяются:

- callbacks и payloads;
- submit/cancel;
- context только внутри callback;
- GIL и Strategy lifetime;
- callback exceptions;
- reuse после ошибки;
- concurrent/nested Strategy protection;
- Result schema, dtype и lifetime;
- end-to-end scenario;
- benchmark helper.

## 7.4. Проверка детерминированности

```bash
uv run pytest -q \
  python/tests/test_end_to_end.py::test_twenty_runs_are_identical
```

Каждый запуск нормализует и сравнивает:

- callback tuples;
- final positions;
- все fill rows;
- все order-log rows;
- PnL index и values.

Двадцать одинаковых результатов показывают, что итог не зависит от случайного
планирования OS threads.

## 7.5. Что измеряет ready-signal benchmark

Запуск:

```bash
build-release/bin/test/back-tester-scheduler-benchmark
```

Код:

[`test/SchedulerBenchmark.cpp`](../../test/SchedulerBenchmark.cpp)

Benchmark использует реальные:

- `SpscRing<ScheduledEvent>`;
- `ReadyBarrier`;
- producer/main thread;
- consumer thread.

Измеряемая граница:

```text
start timer
  producer push_wait(event)
  consumer pop_wait(event)
  consumer publish_processed(sequence)
  producer wait_until(sequence)
stop timer
```

Не входят:

- создание thread;
- выделение samples vector;
- warmup;
- сортировка;
- печать.

Конфигурация:

```text
ring capacity = 1
warmup = 10,000
measured = 100,000
```

Это benchmark synchronization boundary, а не полного backtest.

## 7.6. Percentiles

Если:

```text
p50 = 416 ns
```

то половина samples не медленнее 416 ns.

```text
p95 = 584 ns
```

означает, что 95% samples не медленнее 584 ns.

```text
p99 = 709 ns
```

означает, что 99% samples не медленнее 709 ns.

Значения зависят от CPU, ОС, температуры, фоновой нагрузки и сборки. Это не
универсальный pass threshold.

## 7.7. Как Python benchmark вызывает C++

Запуск:

```bash
uv run python python/benchmarks/callback_overhead.py
```

Python driver:

[`python/benchmarks/callback_overhead.py`](../../python/benchmarks/callback_overhead.py)

Native helper:

[`src/python/bindings.cpp`](../../src/python/bindings.cpp)
`_benchmark_book_callbacks`.

Цепочка:

```text
Python script
  ▼
import back_tester._backtester
  ▼
C++ _benchmark_book_callbacks(strategy, depth, 1000)
  ▼
C++ создаёт payload до timer
  ▼
C++ 1000 раз:
    acquire GIL
    pybind dispatch
    Python on_book_update(payload)
    return to C++
  ▼
C++ возвращает elapsed nanoseconds
  ▼
Python печатает statistics
```

## 7.8. Что входит в callback time

Входит:

- C++ loop;
- GIL window;
- pybind11 C++ → Python dispatch;
- поиск и вызов Python method;
- передача prebuilt payload;
- пустой callback;
- возврат в C++.

Не входит:

- process startup;
- JSON parsing;
- HistoricalLOB;
- matching;
- DataFrame;
- logging;
- создание payload.

Поэтому это цена callback boundary, а не цена полного market event.

## 7.9. Top-1 и top-15

Top-1 payload содержит:

```text
1 bid + 1 ask
```

Top-15:

```text
15 bids + 15 asks
```

Payload создаётся до timer. Пустой Python callback не перебирает уровни, поэтому
главная стоимость — GIL и pybind dispatch. Из-за этого top-1 и top-15 могут
быть близки.

Если стратегия начнёт обходить все уровни, её собственное время top-15
увеличится.

## 7.10. Почему native empty-loop baseline не вычитается

Side-effect-free пустой C++ loop Release compiler может полностью удалить.
Тогда получится ложный baseline `0 ns`.

Поэтому benchmark честно пишет:

```text
native_baseline=not_reported
callback totals are unadjusted
```

Никакая сомнительная величина не вычитается из Python callback time.

## 7.11. Как интерпретировать полученные значения

Пример:

```text
top-1 mean_per_callback=380.4 ns
top-15 mean_per_callback=382.5 ns
```

Это означает:

> Пустой переход C++ → Python → C++ через этот binding занимает примерно
> 0.38 микросекунды на данной машине и в данной сборке.

Реальная стратегия будет стоить:

```text
boundary cost
+ время Python logic
+ работа с данными стратегии
```

