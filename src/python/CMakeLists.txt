find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(_backtester MODULE bindings.cpp)
target_compile_definitions(_backtester PRIVATE BACK_TESTER_VERSION="${PROJECT_VERSION}")

install(TARGETS _backtester DESTINATION back_tester)
