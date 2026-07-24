# 3. Market data, виртуальное время и scheduler

## 3.1. Что находится в JSONL

Fixture [`test/data/m5_two_instrument.jsonl`](../../test/data/m5_two_instrument.jsonl)
содержит по одному JSON-объекту на строку.

Упрощённый объект:

```json
{
  "ts_event": "...100Z",
  "instrument_id": 1,
  "action": "A",
  "side": "B",
  "price": "99",
  "size": 8,
  "sequence": 1,
  "order_id": "101"
}
```

Значение:

```text
на времени 100
для инструмента 1
добавить bid order 101
по цене 99
размером 8
```

Основные действия:

| Код | Смысл |
|---|---|
| `A` | Add |
| `M` | Modify |
| `C` | Cancel |
| `F` | Fill исторической заявки |
| `T` | Trade print |
| `R` | Clear/reset |

## 3.2. Парсинг выполняется один раз

[`JsonlReader`](../../src/market/JsonlReader.hpp) преобразует вход в typed
`MarketDataEvent`.

Например:

```text
"101"       → int64 exchange order ID
"99"        → 99_000_000_000 price ticks
timestamp   → int64 nanoseconds
"B"         → enum Side
"A"         → enum MarketAction
```

После ingestion matching больше не разбирает JSON и не вызывает `stod`.

Reader работает потоково:

```text
прочитать группу
обработать группу
прочитать следующую
```

Он не загружает и не сортирует весь файл. Битый JSON, неправильный timestamp,
регрессия sequence или незавершённая atomic group завершают run ошибкой.

## 3.3. Atomic market group

Несколько строк могут описывать одно логическое изменение книги на одном
instrument и timestamp. Флаг `F_LAST` завершает группу.

Стратегия не должна увидеть промежуточную полукнигу:

```text
применить строку 1
применить строку 2
...
применить F_LAST
только теперь создать callback
```

Book callback формируется, только если видимый top-N действительно изменился.
Trade-only group на пустой неизменившейся книге создаёт trade callback, но не
пустой book callback.

## 3.4. Историческая L3-книга

[`HistoricalLOBStore`](../../src/market/HistoricalLOBStore.hpp) хранит
отдельный `LimitOrderBook` для каждого `instrument_id`.

L3 означает, что книга знает отдельные orders, а не только сумму на цене:

```text
ask 101:
  order 5001 quantity 3
  order 5002 quantity 7
```

Это позволяет:

- корректно применять modify/cancel/fill;
- сохранять price-time traversal;
- различать старую и новую ликвидность на той же цене;
- отслеживать revision исторического order.

## 3.5. Виртуальное время

Wall-clock время компьютера не определяет результат backtest.

Market delivery time:

```text
engine_time = exchange_time + market_data_latency
```

New order arrival:

```text
arrival = submit_engine_time + order_latency
```

Cancel arrival:

```text
arrival = cancel_submit_time + order_latency
```

Scheduler объединяет эти timestamps в одну виртуальную линию.

## 3.6. Приоритет одинаковых timestamps

Если несколько событий имеют одинаковое scheduled time:

1. market event;
2. new order arrival;
3. cancel arrival.

Внутри одного класса используется стабильный sequence.

Это часть контракта. Без неё результат зависел бы от случайного порядка потоков.

## 3.7. Два потока

[`SchedulerRuntime`](../../src/scheduler/SchedulerRuntime.hpp) использует:

- dispatcher thread;
- consumer/trading thread.

Dispatcher:

1. получает следующий market key;
2. забирает команды стратегии;
3. выбирает самое раннее событие;
4. присваивает `dispatch_seq`;
5. отправляет событие в `SpscRing`;
6. ждёт подтверждение обработки.

Consumer:

1. получает событие;
2. передаёт его TradingEngine;
3. выполняет callbacks;
4. отправляет созданные callback-команды;
5. публикует `processed_seq`.

## 3.8. SPSC ring

SPSC означает:

```text
Single Producer, Single Consumer
```

У event ring:

- один producer — dispatcher;
- один consumer — trading thread.

У command ring направление обратное:

- producer — trading/callback side;
- consumer — dispatcher.

Это bounded queues: у них заранее ограничена capacity. Горячий путь не требует
общей mutex-очереди с неограниченным ростом.

## 3.9. Ready barrier

После публикации события `N` dispatcher ждёт:

```text
processed_seq >= N
```

Consumer публикует processed sequence только после:

- matching;
- обновления order state;
- обновления position;
- записи результата;
- callback;
- enqueue callback-команд.

Поэтому событие `N+1` не может быть обработано раньше завершения реакции на
`N`.

## 3.10. Защита от look-ahead

Scheduler может заранее прочитать key будущего события, но не должен заранее
изменить книгу.

Источник разделён на две стадии:

```text
next()
  читает и staging-ит группу
  не изменяет HistoricalLOB

prepare_for_dispatch()
  вызывается только после победы market event в chronological selection
  применяет группу к книге
  строит trades и top-N
```

Пример запрещённого поведения:

```text
t=105 должна прийти наша заявка
t=200 исторический ask станет 101

ошибка: prefetch уже изменил ask на 101
       заявка ложно исполнилась на t=105
```

Правильное поведение:

```text
t=105 заявка видит старый ask 102 и отдыхает
t=200 market event применяется
t=200 заявка исполняется по ask 101
```

