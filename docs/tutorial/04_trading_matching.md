# 4. Заявки, matching, fills и позиции

## 4.1. TradingEngine

Главная реализация находится в
[`src/trading/TradingEngine.cpp`](../../src/trading/TradingEngine.cpp).

TradingEngine одновременно:

- принимает scheduled events;
- ведёт виртуальное `now_ns`;
- реализует `StrategyContext`;
- хранит собственные orders;
- ведёт resting indexes;
- выполняет matching;
- обновляет позиции;
- сообщает события в `Recorder`;
- вызывает Strategy callbacks.

## 4.2. Состояния заявки

```text
PendingNew
    │ arrival
    ▼
   Open
    ├── partial fill ──► PartiallyFilled
    ├── full fill ─────► Filled
    └── cancel request ► PendingCancel ─► Cancelled

invalid submit ────────► Rejected
```

Terminal states:

- `Filled`;
- `Cancelled`;
- `Rejected`.

Terminal order больше не возвращается из `open_orders()`.

## 4.3. Submit внутри callback

В Python:

```python
order_id = self.submit_limit(
    instrument_id=1,
    side=bt.Side.BUY,
    price_ticks=101_000_000_000,
    quantity=2,
)
```

C++ выполняет:

1. проверяет, что вызов сделан внутри callback;
2. создаёт новый `client_order_id`;
3. создаёт order в состоянии `PendingNew`;
4. пишет `Submit` в order log;
5. проверяет instrument, side, price, tick alignment и quantity;
6. рассчитывает delayed arrival;
7. отправляет `NewOrderCommand` в scheduler.

Matching не выполняется в том же стеке callback.

## 4.4. Почему команда не обрабатывается рекурсивно

Плохая модель:

```text
on_book_update
  submit_limit
    process_new
      match
        on_fill
          submit_limit
            ...
```

Она создаёт рекурсивное дерево callbacks и делает порядок трудно
предсказуемым.

В проекте:

```text
on_book_update
  submit_limit
  enqueue command
callback завершился
scheduler позже выбирает command
TradingEngine process_new
```

Даже rejects, созданные во время callback, откладываются FIFO до выхода из
инициирующего callback. Максимальная callback depth остаётся равной 1.

## 4.5. Arrival заявки

На `NewOrderCommand` движок:

1. находит `PendingNew` order;
2. переводит его в `Open`;
3. записывает `Accepted`;
4. добавляет в resting index;
5. проверяет, стала ли заявка marketable.

Для buy:

```text
limit price >= historical ask
```

Для sell:

```text
limit price <= historical bid
```

## 4.6. Fill-at-touch и sweep

Допустим, historical asks:

```text
101 × 2
102 × 3
103 × 10
```

Наша заявка:

```text
BUY limit 102 quantity 4
```

Она может получить:

```text
2 @ 101
2 @ 102
```

И не может пойти на 103, потому что limit равен 102.

Это multi-level sweep с limit protection.

## 4.7. Partial fill

Если доступно меньше:

```text
BUY 5 @ 101
historical ask 101 × 2
```

результат:

```text
fill quantity = 2
filled_quantity = 2
remaining_quantity = 3
state = PartiallyFilled
```

Остаток снова вставляется в resting index и может исполниться позже.

## 4.8. Собственный FIFO

Если две наши заявки находятся на одной цене, сначала рассматривается order,
который раньше прибыл на exchange timeline.

Ключ порядка:

```text
price
arrival_sequence
client_order_id
```

Для buys лучшая цена выше. Для sells лучшая цена ниже.

## 4.9. Private consumption

Synthetic fill не меняет shared HistoricalLOB.

Движок хранит ключ:

```text
instrument
historical side
historical exchange order ID
liquidity revision
```

и количество уже виртуально использованной ликвидности.

Revision нужен для такого случая:

```text
на ask 101 была старая quantity 2
мы её виртуально использовали
затем исторический order обновился новой liquidity
```

Новая revision не должна считаться уже использованной.

## 4.10. Порядок обновления при fill

Перед `on_fill()` C++:

1. уменьшает remaining quantity;
2. увеличивает filled quantity;
3. меняет state;
4. обновляет position;
5. удаляет terminal order из open index;
6. пишет order event;
7. пишет fill row;
8. только после этого вызывает Python.

Поэтому внутри:

```python
def on_fill(self, fill):
    position = self.position(fill.instrument_id)
    orders = self.open_orders(fill.instrument_id)
```

видно уже новое состояние.

## 4.11. Cancel

В callback:

```python
self.cancel_order(order_id)
```

Сразу записывается:

```text
state = PendingCancel
event = CancelRequest
```

Но actual cancel arrival происходит после `order_latency`.

Когда scheduler доставляет `CancelCommand`:

- order удаляется из resting index;
- state становится `Cancelled`;
- order удаляется из open-order index;
- записывается terminal transition.

При fill/cancel race результат определяется общей временной линией и
same-timestamp priority.

## 4.12. Reject

Примеры причин:

- unknown instrument;
- invalid side;
- quantity ≤ 0;
- price ≤ 0;
- price не кратна tick size;
- unknown order при cancel;
- order уже terminal.

Reject записывается в Result до `on_reject()`. Если reject появился внутри
другого callback, уведомление стратегии помещается в FIFO deferred queue.

