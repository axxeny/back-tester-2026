# Third-party libraries.
# Use the checked-in Catch2 amalgamated source instead of downloading/building the
# full Catch2 project.  This keeps zip-based/offline builds deterministic.

if(EXISTS "${CMAKE_SOURCE_DIR}/3rdparty/Catch2/extras/catch_amalgamated.cpp")
    set(TGT Catch2-static-lib)
    add_library(${TGT} STATIC
        "${CMAKE_SOURCE_DIR}/3rdparty/Catch2/extras/catch_amalgamated.cpp"
    )
    target_include_directories(${TGT} SYSTEM PUBLIC
        "${CMAKE_SOURCE_DIR}/3rdparty/Catch2/extras"
    )
else()
    message(FATAL_ERROR "3rdparty/Catch2/extras/catch_amalgamated.cpp is missing; restore 3rdparty/Catch2 from the project zip.")
endif()
