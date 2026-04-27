set(BACK_TESTER_FEATHER_MODE "AUTO" CACHE STRING
    "Feather ingest support: AUTO, ON, or OFF")
set_property(CACHE BACK_TESTER_FEATHER_MODE PROPERTY STRINGS AUTO ON OFF)

set(BACK_TESTER_HAVE_FEATHER OFF)
set(BACK_TESTER_ARROW_TARGET "")

if(NOT BACK_TESTER_FEATHER_MODE STREQUAL "OFF")
    find_package(Arrow QUIET CONFIG)

    if(TARGET arrow::arrow)
        set(BACK_TESTER_HAVE_FEATHER ON)
        set(BACK_TESTER_ARROW_TARGET arrow::arrow)
        print_message("Feather ingest:     enabled via arrow::arrow")
    elseif(TARGET Arrow::arrow_shared)
        set(BACK_TESTER_HAVE_FEATHER ON)
        set(BACK_TESTER_ARROW_TARGET Arrow::arrow_shared)
        print_message("Feather ingest:     enabled via Arrow::arrow_shared")
    elseif(BACK_TESTER_FEATHER_MODE STREQUAL "ON")
        message(FATAL_ERROR
            "BACK_TESTER_FEATHER_MODE=ON but Arrow was not found. Run `conan install . --build=missing -s build_type=${CMAKE_BUILD_TYPE}` and reconfigure CMake.")
    else()
        print_message("Feather ingest:     disabled (run `conan install . --build=missing -s build_type=${CMAKE_BUILD_TYPE}`)")
    endif()
endif()
