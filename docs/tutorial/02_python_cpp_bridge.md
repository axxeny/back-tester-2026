# 2. Сборка и мост между Python и C++

## 2.1. C++ работает внутри Python-процесса

В этом проекте Python не запускает C++ как отдельную программу и не общается с
ним через сеть. C++ собирается в динамическую библиотеку:

```text
back_tester/_backtester.cpython-312-darwin.so
```

Python загружает эту библиотеку внутрь собственного процесса. Поэтому вызов
нативной функции выглядит как обычный Python-вызов.

## 2.2. Что делает установка

Команда:

```bash
uv run pip install -e .
```

запускает цепочку:

```text
pip
  ▼
scikit-build-core
  ▼
CMake
  ▼
AppleClang / Clang / GCC
  ▼
back-tester-lib + _backtester.so
  ▼
editable Python package
```

Настройка Python build backend находится в
[`pyproject.toml`](../../pyproject.toml):

```toml
[build-system]
build-backend = "scikit_build_core.build"

[tool.scikit-build]
cmake.define.BUILD_PYTHON_MODULE = true
wheel.packages = ["python/back_tester"]
```

CMake при включённом `BUILD_PYTHON_MODULE` добавляет
[`src/python`](../../src/python):

```cmake
pybind11_add_module(_backtester MODULE bindings.cpp)
target_link_libraries(_backtester PRIVATE back-tester-lib)
```

Итоговый `_backtester` содержит bindings и связан с основной C++-библиотекой
движка.

## 2.3. Роль pybind11

Pybind11 — библиотека-адаптер между типами Python и C++.

Точка объявления модуля находится в
[`src/python/bindings.cpp`](../../src/python/bindings.cpp):

```cpp
PYBIND11_MODULE(_backtester, module) {
    module.def("version", ...);
    // enum, classes, run(), benchmark helper
}
```

Через неё экспортируются:

- `Side`, `OrderState`, `RejectReason`;
- `BacktestConfig`, `DateRange`, `InstrumentMeta`;
- payload classes `BookUpdate`, `Trade`, `Fill`, `Reject`;
- `Strategy`;
- `Result`;
- `run()`.

Например, native enum становится Python enum:

```python
bt.Side.BUY
bt.Side.SELL
```

## 2.4. Python façade

Файл [`python/back_tester/__init__.py`](../../python/back_tester/__init__.py)
импортирует сущности из native `_backtester`:

```python
from ._backtester import Strategy, Result, Side, run
```

И создаёт удобное пространство имён:

```python
bt.backtest.run(...)
```

Важно:

```python
bt.backtest.run
```

выглядит как Python API, но реализация `run()` находится в C++.

## 2.5. Python Strategy является наследником native класса

Пользователь пишет:

```python
class MyStrategy(bt.Strategy):
    def on_book_update(self, update):
        self.submit_limit(...)
```

`bt.Strategy` — класс, созданный pybind11. Методы:

```python
submit_limit
cancel_order
position
open_orders
now_ns
```

на самом деле делегируют вызов в C++ `StrategyContext`.

Путь заявки:

```text
Python self.submit_limit()
  ▼
PythonStrategyHandle::submit_limit()
  ▼
TradingEngine::submit_limit()
```

## 2.6. Как C++ вызывает Python callback

`PythonStrategyAdapter` реализует C++ interface `Strategy`.

Когда TradingEngine вызывает:

```cpp
strategy_.on_fill(fill, context);
```

адаптер выполняет:

```cpp
strategy_.attr("on_fill")(fill);
```

То есть на Python-стороне вызывается:

```python
strategy.on_fill(fill)
```

Для `BookUpdate` C++ сначала копирует callback-scoped уровни в
`OwnedBookUpdate`. Это не даёт Python сохранить ссылку на память, которая будет
переиспользована следующим market event.

## 2.7. Что такое GIL

GIL — блокировка Python-интерпретатора. Для выполнения Python bytecode поток
должен владеть GIL.

Долгий native run запускается так:

```cpp
{
    py::gil_scoped_release release;
    frozen = run_backtest(...);
}
```

На время чтения рынка, matching и ожидания потоков GIL освобождён.

Перед callback:

```cpp
py::gil_scoped_acquire gil;
ActiveContext active(handle, context);
strategy.attr(method)(payload);
```

Порядок:

```text
C++ работает               GIL свободен
C++ хочет вызвать Python   захватывает GIL
Python callback            GIL удерживается
callback завершился        context отключается
C++ продолжает             GIL освобождается
```

## 2.8. Почему context доступен только внутри callback

Этот код разрешён:

```python
def on_book_update(self, update):
    print(self.now_ns)
    self.submit_limit(...)
```

Этот код запрещён:

```python
strategy.submit_limit(...)  # вне callback
```

Причина: submit должен быть привязан к конкретному engine event и
`CommandSink`. Вне callback непонятно, какое сейчас виртуальное время и куда
детерминированно поставить команду.

`ActiveContext` подключает C++ context только на время callback и проверяет
поток. После callback он отключается.

## 2.9. Ошибки Python не оставляют зависшие потоки

Если Python callback бросает исключение:

1. pybind11 сохраняет Python exception;
2. consumer thread сообщает scheduler об ошибке;
3. закрываются event и command rings;
4. ready barrier разблокируется;
5. dispatcher и consumer threads завершаются;
6. оба потока соединяются через `join()`;
7. исходное исключение возвращается вызывающему `backtest.run()`.

Следующий backtest после ошибки может быть запущен снова.

Один и тот же объект Strategy нельзя одновременно использовать в двух runs.
`StrategyRunGuard` выдаст:

```text
strategy is already running
```

Это защищает callback context от смешивания между двумя движками.

