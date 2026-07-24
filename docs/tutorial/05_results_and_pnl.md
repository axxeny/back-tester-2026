# 5. Result, pandas и расчёт PnL

## 5.1. Почему результат хранится колонками

Медленный вариант:

```python
for fill in fills:
    dataframe.loc[len(dataframe)] = {...}
```

Он создаёт Python objects и расширяет DataFrame на каждом событии.

В проекте C++ хранит отдельный массив для каждой колонки:

```text
fills.exchange_ts_ns[]
fills.instrument_id[]
fills.price_ticks[]
fills.quantity[]

orders.engine_ts_ns[]
orders.state[]
orders.remaining_quantity[]

pnl.engine_ts_ns[]
pnl.total_pnl[]
```

Это называется columnar storage.

Реализация находится в
[`src/results/ResultRecorder.cpp`](../../src/results/ResultRecorder.cpp).

## 5.2. Fill result

`fills_df` содержит:

| Колонка | Смысл |
|---|---|
| `exchange_ts_ns` | Время исторической ликвидности |
| `engine_ts_ns` | Виртуальное время движка |
| `instrument_id` | Инструмент |
| `client_order_id` | Наша заявка |
| `side` | Buy/Sell encoding |
| `price_ticks` | Цена fill |
| `quantity` | Количество этого fill |
| `remaining_quantity` | Остаток заявки |
| `liquidity_source` | Источник ликвидности |

## 5.3. Order log

`order_log_df` — не одна строка на order, а журнал переходов.

Пример:

```text
t=100 Submit         PendingNew
t=105 Accepted       Open
t=200 Fill           Filled
```

Для cancel:

```text
t=110 Submit         PendingNew
t=115 Accepted       Open
t=150 CancelRequest  PendingCancel
t=155 Cancelled      Cancelled
```

Это позволяет проверить latency и state machine, а не только финальный state.

## 5.4. Position

Для buy fill:

```text
signed quantity = +quantity
```

Для sell:

```text
signed quantity = -quantity
```

Net position:

```text
position = сумма signed fills
```

PositionKeeper также ведёт average open price и realized PnL для закрывающих
сделок.

## 5.5. Contract multiplier и price scale

Цена хранится как fixed-point integer:

```text
101.0 → 101_000_000_000
price_scale = 1_000_000_000
```

Multiplier переводит ценовое движение в денежный результат:

```text
PnL =
    price difference
  × quantity
  × contract multiplier
  ÷ price scale
```

Внутри промежуточные вычисления используют checked integer/rational arithmetic,
чтобы не накапливать случайную ошибку `double`.

`double` создаётся один раз при финальном `freeze()`, потому что pandas Series
удобно представлять как `float64`.

## 5.6. Mark-to-market

Если книга двусторонняя:

```text
midpoint = (best_bid + best_ask) / 2
```

Unrealized PnL оценивает открытую позицию по midpoint.

Пример:

```text
buy price = 101
best bid = 99
best ask = 101
midpoint = 100
quantity = 2
multiplier = 10
```

```text
PnL = (100 - 101) × 2 × 10 = -20
```

## 5.7. Freeze

После завершения run:

```cpp
return recorder.freeze();
```

`FrozenResults` содержит shared ownership неизменяемого native storage.

После freeze:

- recorder больше не добавляет строки;
- spans остаются стабильными;
- Python может безопасно получить NumPy views.

## 5.8. Native memory и pandas

В [`src/python/bindings.cpp`](../../src/python/bindings.cpp) создаётся
`py::array`, указывающий на C++ memory:

```cpp
py::array(dtype, shape, stride, values.data(), owner_capsule)
```

`owner_capsule` хранит копию:

```cpp
std::shared_ptr<FrozenResults>
```

Пока жив хотя бы один NumPy array, DataFrame или Series, capsule удерживает
native storage.

Поэтому допустимо:

```python
fills = result.fills_df
del result
print(fills)
```

Данные не исчезнут.

## 5.9. Почему написано `copy=False`

Pandas создаётся так:

```python
pandas.DataFrame(values, copy=False)
```

Цель — избежать копирования по одной строке и по возможности сохранить
NumPy/native backing storage.

Pandas всё равно может сделать собственную bulk-копию в зависимости от dtype и
внутренних правил, но в проекте отсутствует per-row Python conversion path.

## 5.10. Dtypes являются частью контракта

Примеры:

```text
timestamps       int64
instrument IDs   int64
client order IDs uint64
side             int8
state            uint8
quantity         int64
PnL              float64
```

Тесты проверяют не только значения, но также порядок колонок и dtype.

