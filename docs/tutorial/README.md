# Учебник по Back-tester HW4

Этот учебник объясняет проект с нуля: зачем в нём одновременно Python и C++,
как исторические события превращаются в callbacks стратегии, каким образом
Python-заявка возвращается в C++, как рассчитываются fills, позиции и PnL и что
на самом деле измеряют benchmarks.

Материал рассчитан на читателя, который умеет читать простой Python, но может
не знать pybind11, GIL, CMake, биржевой LOB и многопоточность.

## Как читать

Главы расположены в порядке прохождения данных через систему:

1. [Общая картина и устройство проекта](01_overview.md)
2. [Сборка и мост между Python и C++](02_python_cpp_bridge.md)
3. [Market data, виртуальное время и scheduler](03_market_data_scheduler.md)
4. [Заявки, matching, fills и позиции](04_trading_matching.md)
5. [Result, pandas и расчёт PnL](05_results_and_pnl.md)
6. [Пошаговый разбор end-to-end примера](06_end_to_end_walkthrough.md)
7. [Тесты и benchmarks](07_tests_and_benchmarks.md)
8. [Словарь, вопросы и упражнения](08_glossary_and_exercises.md)

Для первого знакомства достаточно прочитать главы 1, 2 и 6. Главы 3–5
подробно объясняют внутреннее устройство. Глава 7 нужна, чтобы понимать
проверки и цифры производительности.

## Быстрый запуск

Из корня репозитория:

```bash
uv sync --locked
uv run pip install -e .
uv run python examples/mean_reversion.py
```

Полная проверка:

```bash
uv run cmake -S . -B build-release \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON
uv run cmake --build build-release -j
uv run ctest --test-dir build-release --output-on-failure
uv run pytest -q python/tests
```

## Главная мысль в одном абзаце

Python — это удобный язык стратегии. C++ — это сам симулятор: он читает
исторический рынок, поддерживает книгу заявок, объединяет события в одну
временную линию, применяет latency, исполняет заявки, обновляет позиции и
накапливает результат. Pybind11 соединяет эти два мира внутри одного процесса:
Python вызывает C++ `run()`, а C++ в нужные моменты вызывает Python callbacks.

