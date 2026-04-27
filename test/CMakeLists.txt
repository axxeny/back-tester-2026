include(CTest)

include(Catch)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/test)

file(GLOB TEST_SRC CONFIGURE_DEPENDS *.cpp)

# executable
set(TARGET back-tester-tests)
add_executable(${TARGET} ${TEST_SRC})

# Link against the necessary libraries
target_link_libraries(${TARGET} PRIVATE
    Catch2::Catch2WithMain
    back-tester-ingest
    back-tester-lob
)

# Register the tests with CTest
catch_discover_tests(${TARGET})
