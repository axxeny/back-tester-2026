set(TARGET back-tester-ingest)
file(GLOB SRC CONFIGURE_DEPENDS *.cpp)

add_library(${TARGET} STATIC ${SRC})
target_link_libraries(${TARGET} PUBLIC
    back-tester-common
    simdjson-static-lib
)

if(BACK_TESTER_HAVE_FEATHER)
    target_compile_definitions(${TARGET} PUBLIC BACK_TESTER_HAVE_FEATHER=1)
    target_include_directories(${TARGET} SYSTEM PUBLIC ${BACK_TESTER_PYARROW_INCLUDE_DIR})
    target_link_directories(${TARGET} PUBLIC ${BACK_TESTER_PYARROW_LIB_DIRS})
    target_link_libraries(${TARGET} PUBLIC ${BACK_TESTER_PYARROW_LIBRARIES})
endif()
