# 1. Общая картина и устройство проекта

## 1.1. Какую задачу решает backtester

Backtester воспроизводит записанный рынок и позволяет проверить стратегию так,
как будто она работала в прошлом.

На входе находится JSONL-файл:

```text
добавили bid
добавили ask
изменили заявку
отменили заявку
произошла сделка
```

На выходе находятся:

- порядок callbacks стратегии;
- журнал состояний собственных заявок;
- synthetic fills;
- позиция по каждому инструменту;
- временной ряд PnL.

Слово `synthetic` важно: backtester не отправляет заявку на настоящую биржу.
Он вычисляет, что могло бы произойти по принятой модели исполнения.

## 1.2. Простая аналогия

Можно представить систему как поездку на автомобиле:

| Компонент | Аналогия |
|---|---|
| JSONL | Запись дороги и движения |
| Market reader | Устройство чтения записи |
| Scheduler | Часы и светофоры |
| HistoricalLOB | Окружающий рынок |
| TradingEngine | Автомобиль |
| Python Strategy | Водитель |
| pybind11 | Руль и педали |
| ResultRecorder | Бортовой самописец |
| pandas Result | Отчёт после поездки |

Стратегия решает, что делать. Движок определяет, когда команда дошла до рынка,
могла ли она исполниться и что стало с заявкой и позицией.

## 1.3. Главный поток данных

```text
JSONL
  │
  ▼
JsonlReader
  │ typed MarketDataEvent
  ▼
JsonlScheduledSource
  │ scheduled market event
  ▼
SchedulerRuntime
  │ единая временная линия
  ▼
TradingEngine
  ├── matching
  ├── order state
  ├── position
  └── result recording
  │
  ▼
PythonStrategyAdapter
  │
  ▼
Python callbacks
```

Обратный путь команды:

```text
Python callback
  │ self.submit_limit() / self.cancel_order()
  ▼
PythonStrategyHandle
  ▼
TradingEngine
  │ NewOrderCommand / CancelCommand
  ▼
SchedulerRuntime
  │ применяет order latency
  ▼
TradingEngine
  │ arrival, matching, fill/cancel
  ▼
Python on_fill() / on_reject()
```

## 1.4. Почему Python не делает всё сам

Горячий цикл backtest может обработать миллионы событий. Поэтому внутри него
нежелательно:

- заново разбирать строки;
- создавать Python dict для каждого события;
- добавлять строки в pandas по одной;
- копировать всю книгу ради одной заявки;
- зависеть от порядка, в котором ОС запустила потоки.

C++-часть хранит:

- timestamps как `int64` наносекунды;
- цены как целые fixed-point значения;
- quantities и IDs как числа;
- стороны, действия и состояния как enum;
- результаты как колоночные массивы.

Python вызывается только на публичной границе стратегии.

## 1.5. Основные каталоги

| Путь | Назначение |
|---|---|
| [`src/core`](../../src/core) | Общие типы, события и конфигурация |
| [`src/market`](../../src/market) | JSONL reader и историческая L3-книга |
| [`src/scheduler`](../../src/scheduler) | Очереди, порядок событий, barrier |
| [`src/trading`](../../src/trading) | Заявки, matching, позиции |
| [`src/results`](../../src/results) | Fill/order/PnL buffers |
| [`src/runtime`](../../src/runtime) | Сборка компонентов в один backtest |
| [`src/python`](../../src/python) | Pybind11 bindings |
| [`python/back_tester`](../../python/back_tester) | Публичный Python package |
| [`examples`](../../examples) | Запускаемые стратегии |
| [`test`](../../test) | Native tests, fixtures и benchmark |
| [`python/tests`](../../python/tests) | Python integration tests |

## 1.6. HistoricalLOB, EngineView и SimulatedLOB

`HistoricalLOB` — книга, восстановленная из исторического файла. Она отвечает
на вопрос:

> Какие заявки были видны на записанном рынке?

`EngineView` — приватное состояние нашей симуляции:

- собственные заявки;
- их порядок;
- уже виртуально использованная историческая ликвидность.

Ментальная формула:

```text
SimulatedLOB = HistoricalLOB + private EngineView
```

Synthetic fill не удаляет ликвидность из общей исторической книги. Вместо этого
движок отдельно запоминает, сколько конкретной исторической заявки уже было
виртуально использовано этим EngineView. Благодаря этому несколько независимых
симуляций не переписывают исходную историю друг для друга.

## 1.7. Что проект моделирует и чего не моделирует

Поддерживается:

- несколько инструментов;
- limit GTC заявки;
- cancel;
- положительная фиксированная order latency;
- фиксированная market-data latency;
- partial fills;
- sweep нескольких уровней до limit price;
- resting order, который исполняется позже;
- позиции и multiplier-aware PnL;
- Python callbacks;
- детерминированный результат.

Не моделируется:

- настоящая очередь перед нашей заявкой;
- market impact;
- случайный slippage и latency jitter;
- IOC, FOK, stop, peg, post-only;
- multi-leg options orders;
- exercise, assignment и expiration settlement;
- Greeks и полноценный risk engine.

Fill-модель оптимистична: если на допустимой цене видна историческая
ликвидность, наша заявка может её виртуально забрать.

