# 8. Словарь, вопросы и упражнения

## 8.1. Словарь

### Ask

Заявка продавца. Лучший ask — минимальная цена, по которой сейчас готовы
продать.

### Bid

Заявка покупателя. Лучший bid — максимальная цена, по которой сейчас готовы
купить.

### Callback

Метод стратегии, который движок вызывает в ответ на событие:

```python
on_book_update
on_trade
on_fill
on_reject
```

### Consumer

Поток, который получает scheduled event и передаёт его TradingEngine.

### Dispatcher

Поток, который объединяет market events и strategy commands в одну временную
линию.

### Engine time

Виртуальное время, на котором событие видит торговый движок.

### Exchange time

Время исходного исторического события на бирже.

### Fill

Исполнение части или всей собственной заявки.

### Fixed-point price

Цена, сохранённая целым числом:

```text
101.25 × 1,000,000,000 = 101,250,000,000
```

### GIL

Блокировка Python interpreter. Нужна для исполнения Python callbacks и работы с
Python objects.

### HistoricalLOB

Историческая L3-книга, восстановленная из входного файла.

### L3

Представление книги на уровне отдельных exchange orders, а не только суммы на
цене.

### Latency

Задержка доставки market data или собственной команды.

### Limit order

Заявка с ограничением цены. Buy не должен исполняться дороже limit, sell — ниже
limit.

### Look-ahead

Ошибка, при которой событие или стратегия видит будущую рыночную информацию.

### Mark

Цена, по которой оценивается открытая позиция. В проекте используется midpoint
двусторонней книги.

### Matching

Поиск исторической ликвидности, совместимой с ценой собственной заявки.

### PnL

Profit and Loss — прибыль или убыток.

### Pybind11

Библиотека для экспорта C++ classes/functions в Python и вызова Python из C++.

### Ready barrier

Механизм, не позволяющий dispatcher выдать следующее событие до полной
обработки текущего.

### Resting order

Открытая, но пока не исполнившаяся заявка.

### SPSC

Single Producer, Single Consumer — очередь с одним producer и одним consumer.

### Synthetic fill

Виртуальное исполнение в backtest, а не настоящая биржевая сделка.

### Top-N

Первые N агрегированных уровней bid и ask, видимых стратегии.

### Zero-copy

Передача Python view на native memory без поэлементного копирования.

## 8.2. Контрольные вопросы

1. Почему Python strategy не читает JSON напрямую?
2. Почему `submit_limit()` разрешён только внутри callback?
3. Чем submit time отличается от arrival time?
4. Почему future market group нельзя применять во время prefetch?
5. Зачем dispatcher ждёт `processed_seq`?
6. Почему synthetic fill не удаляет order из shared HistoricalLOB?
7. Что происходит до вызова `on_fill()`?
8. Почему Result хранится колонками?
9. Что удерживает native Result memory после удаления Python `Result`?
10. Почему callback benchmark не является benchmark полного backtest?

Ответы можно найти в главах 2–7.

## 8.3. Упражнение: изменить latency

В [`examples/mean_reversion.py`](../../examples/mean_reversion.py) временно
замените:

```python
order_latency_ns=5
```

на:

```python
order_latency_ns=20
```

Перед запуском предположите:

- когда первая заявка станет Open;
- успеет ли cancel instrument 2 прибыть до конца fixture;
- какие timestamps изменятся в order log.

После эксперимента верните исходное значение.

## 8.4. Упражнение: посмотреть Result

Добавьте во временную копию example:

```python
print(result.fills_df)
print(result.order_log_df)
print(result.pnl_series)
```

Найдите:

- Submit и Accepted первой заявки;
- CancelRequest и Cancelled второй;
- единственный fill;
- timestamp финального PnL.

## 8.5. Упражнение: partial fill

Скопируйте fixture во временный файл и уменьшите ask quantity на времени 200:

```text
было 4
стало 1
```

Предположите:

- fill quantity;
- remaining quantity;
- state заявки;
- финальную позицию.

Не коммитьте изменённую test fixture поверх golden scenario.

## 8.6. Упражнение: измерить собственную стратегию

В callback benchmark пустой callback ничего не делает. Создайте временный
вариант:

```python
class ReadsLevels(Strategy):
    def on_book_update(self, update):
        total = sum(level.quantity for level in update.bids)
        total += sum(level.quantity for level in update.asks)
```

Сравните top-1 и top-15. Теперь разница должна отражать Python-обход уровней.

## 8.7. Упражнение: проследить один submit по коду

Откройте по порядку:

1. [`examples/mean_reversion.py`](../../examples/mean_reversion.py)
2. [`src/python/bindings.cpp`](../../src/python/bindings.cpp)
3. [`src/trading/TradingEngine.cpp`](../../src/trading/TradingEngine.cpp)
4. [`src/scheduler/SchedulerRuntime.hpp`](../../src/scheduler/SchedulerRuntime.hpp)
5. снова [`src/trading/TradingEngine.cpp`](../../src/trading/TradingEngine.cpp)

Найдите:

```text
Python submit_limit
→ PythonStrategyHandle::submit_limit
→ TradingEngine::submit_limit
→ CommandSink::push
→ SchedulerRuntime::drain_commands
→ TradingEngine::process_new
```

Это полный круг заявки от Python до delayed native arrival.

## 8.8. Куда идти дальше

После учебника полезно читать:

- [`docs/hw4/architecture`](../hw4/architecture) — формальные решения;
- [`docs/hw4/project/07_requirements_traceability.md`](../hw4/project/07_requirements_traceability.md)
  — связь задания с реализацией;
- [`src/results/DESIGN.md`](../../src/results/DESIGN.md) — ownership и PnL;
- [`docs/SimulatedLOB_design.md`](../SimulatedLOB_design.md) — исходный дизайн
  симулированной книги.

