#include <pybind11/pybind11.h>

#ifndef BACK_TESTER_VERSION
#define BACK_TESTER_VERSION "unknown"
#endif

PYBIND11_MODULE(_backtester, module) {
  module.doc() = "Minimal native baseline for back_tester";
  module.def("version", []() { return BACK_TESTER_VERSION; });
}
