set(BACK_TESTER_FEATHER_MODE "AUTO" CACHE STRING
    "Feather ingest support: AUTO, ON, or OFF")
set_property(CACHE BACK_TESTER_FEATHER_MODE PROPERTY STRINGS AUTO ON OFF)

set(BACK_TESTER_HAVE_FEATHER OFF)
set(BACK_TESTER_PYARROW_INCLUDE_DIR "")
set(BACK_TESTER_PYARROW_LIB_DIRS "")
set(BACK_TESTER_PYARROW_LIBRARIES "")

if(NOT BACK_TESTER_FEATHER_MODE STREQUAL "OFF")
    if(NOT DEFINED Python3_EXECUTABLE)
        if(EXISTS "${CMAKE_SOURCE_DIR}/.venv/bin/python3")
            set(Python3_EXECUTABLE "${CMAKE_SOURCE_DIR}/.venv/bin/python3" CACHE FILEPATH
                "Python interpreter for pyarrow-backed Feather support" FORCE)
        elseif(EXISTS "${CMAKE_SOURCE_DIR}/.venv/bin/python")
            set(Python3_EXECUTABLE "${CMAKE_SOURCE_DIR}/.venv/bin/python" CACHE FILEPATH
                "Python interpreter for pyarrow-backed Feather support" FORCE)
        endif()
    endif()

    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND)
        execute_process(
            COMMAND ${Python3_EXECUTABLE} -c
                    "import glob, os, pyarrow as pa; lib_dir = pa.get_library_dirs()[0]; libs = sorted(glob.glob(os.path.join(lib_dir, 'libarrow.[0-9]*.dylib'))); print(pa.get_include()); print(';'.join(pa.get_library_dirs())); print(';'.join(libs))"
            RESULT_VARIABLE PYARROW_RESULT
            OUTPUT_VARIABLE PYARROW_OUTPUT
            ERROR_VARIABLE PYARROW_ERROR
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(PYARROW_RESULT EQUAL 0)
            string(REPLACE "\n" ";" PYARROW_LINES "${PYARROW_OUTPUT}")
            list(GET PYARROW_LINES 0 BACK_TESTER_PYARROW_INCLUDE_DIR)
            list(GET PYARROW_LINES 1 BACK_TESTER_PYARROW_LIB_DIRS)
            list(GET PYARROW_LINES 2 BACK_TESTER_PYARROW_LIBRARIES)
            set(BACK_TESTER_HAVE_FEATHER ON)
            list(APPEND CMAKE_BUILD_RPATH ${BACK_TESTER_PYARROW_LIB_DIRS})
            print_message("Feather ingest:     enabled via ${Python3_EXECUTABLE}")
        elseif(BACK_TESTER_FEATHER_MODE STREQUAL "ON")
            message(FATAL_ERROR
                "BACK_TESTER_FEATHER_MODE=ON but pyarrow probe failed for ${Python3_EXECUTABLE}: ${PYARROW_ERROR}")
        else()
            print_message("Feather ingest:     disabled (run `uv sync --group feather --no-dev`)")
        endif()
    elseif(BACK_TESTER_FEATHER_MODE STREQUAL "ON")
        message(FATAL_ERROR
            "BACK_TESTER_FEATHER_MODE=ON but no Python interpreter was found")
    else()
        print_message("Feather ingest:     disabled (no Python interpreter found)")
    endif()
endif()
