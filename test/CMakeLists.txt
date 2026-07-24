include(CTest)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/test)

set(TARGET back-tester-tests)
add_executable(${TARGET}
    BasicTypesTest.cpp
    CoreContractsTest.cpp
    SimulatedLOBTest.cpp
    TestMain.cpp
)

target_link_libraries(${TARGET} PRIVATE
    back-tester-common
    back-tester-lib
)

add_test(NAME native-unit-tests COMMAND ${TARGET})

add_test(
    NAME cli-missing-path
    COMMAND ${CMAKE_COMMAND}
        -DCLI_PATH=$<TARGET_FILE:back-tester>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/CheckCliUsage.cmake
)

add_test(
    NAME cli-tiny-fixture
    COMMAND $<TARGET_FILE:back-tester> ${CMAKE_CURRENT_SOURCE_DIR}/data/tiny_mbo.jsonl
)
