# 6. Пошаговый разбор end-to-end примера

## 6.1. Запуск

```bash
uv run python examples/mean_reversion.py
```

Код стратегии:

[`examples/mean_reversion.py`](../../examples/mean_reversion.py)

Рыночная fixture:

[`test/data/m5_two_instrument.jsonl`](../../test/data/m5_two_instrument.jsonl)

Конфигурация:

```python
bt.BacktestConfig(
    order_latency_ns=5,
    book_depth=1,
)
```

Metadata:

```python
instrument 1 multiplier = 10
instrument 2 multiplier = 5
```

## 6.2. Полная временная линия

| Время | Instrument | Событие |
|---:|---:|---|
| 100 | 1 | Исторические bid 99 и ask 102 |
| 100 | 1 | Python получает book callback |
| 100 | 1 | Python submit BUY 101 × 2 |
| 105 | 1 | Заявка прибывает и становится Open |
| 110 | 2 | Исторические bid 49 и ask 51 |
| 110 | 2 | Python submit BUY 49 × 1 |
| 115 | 2 | Заявка прибывает и становится Open |
| 150 | 2 | Исторический bid меняется на 48 |
| 150 | 2 | Python делает cancel request |
| 155 | 2 | Cancel прибывает, order становится Cancelled |
| 200 | 1 | Historical ask меняется с 102 на 101 |
| 200 | 1 | Resting BUY 101 × 2 исполняется |
| 200 | 1 | Python получает fill, trade, book |

## 6.3. Instrument 1: rest, затем fill

На времени 100:

```text
bid = 99
ask = 102
```

Стратегия отправляет:

```text
BUY limit 101 quantity 2
```

Order latency равна 5:

```text
submit = 100
arrival = 105
```

На 105:

```text
limit 101 < ask 102
```

Заявка не marketable и остаётся Open.

На 200 historical ask меняется:

```text
ask 102 → 101
```

Теперь:

```text
limit 101 >= ask 101
```

Создаётся:

```text
fill price = 101
fill quantity = 2
remaining = 0
state = Filled
position = +2
```

## 6.4. Instrument 2: cancel

На 110:

```text
bid = 49
ask = 51
```

Стратегия отправляет:

```text
BUY 49 × 1
```

На 115 заявка становится Open.

На 150 стратегия вызывает cancel:

```text
state = PendingCancel
```

Cancel latency тоже равна 5:

```text
cancel arrival = 155
```

На 155:

```text
state = Cancelled
position instrument 2 = 0
```

Instrument 1 и 2 не смешивают:

- orders;
- positions;
- historical books;
- private consumption.

## 6.5. Callback order

Фактический вывод:

```text
callbacks=[
  ('book', 1, 2, 100),
  ('book', 2, 4, 110),
  ('book', 2, 5, 150),
  ('fill', 1, 1, 200),
  ('trade', 1, 7, 200),
  ('book', 1, 7, 200)
]
```

На времени 200 порядок:

```text
fill → trade → book
```

Сначала движок reevaluate-ит resting orders против уже применённой книги.
Потом публикует source trades. Затем сообщает итоговый top-N.

## 6.6. Order log instrument 1

```text
t=100 Submit    PendingNew      remaining=2
t=105 Accepted  Open            remaining=2
t=200 Fill      Filled          remaining=0
```

## 6.7. Order log instrument 2

```text
t=110 Submit         PendingNew      remaining=1
t=115 Accepted       Open            remaining=1
t=150 CancelRequest  PendingCancel   remaining=1
t=155 Cancelled      Cancelled       remaining=1
```

У cancelled order remaining остаётся 1, потому что quantity не исполнялась.
Cancel прекращает активность заявки, но не превращает остаток в fill.

## 6.8. Финальные значения

```text
final_positions={1: 2, 2: 0}
terminal_states={1: Filled, 2: Cancelled}
fills=1
final_pnl=-20.0
```

Числовые enum encodings в выводе:

```text
Filled    = 3
Cancelled = 5
```

## 6.9. Что доказывает этот пример

Один маленький сценарий проходит весь production path:

```text
JSONL
→ streaming reader
→ HistoricalLOB
→ chronological scheduler
→ event ring
→ TradingEngine
→ Python callback
→ submit/cancel commands
→ delayed arrival
→ matching/state/position
→ ResultRecorder
→ NumPy/pandas
```

Это не mock и не stub. Python example использует тот же native `run()`, который
доступен пользователю package.

