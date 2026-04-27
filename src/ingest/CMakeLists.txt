set(TARGET back-tester-ingest)
file(GLOB SRC CONFIGURE_DEPENDS *.cpp)

add_library(${TARGET} STATIC ${SRC})
target_link_libraries(${TARGET} PUBLIC
    back-tester-common
    simdjson::simdjson
)

if(BACK_TESTER_HAVE_FEATHER)
    target_compile_definitions(${TARGET} PUBLIC BACK_TESTER_HAVE_FEATHER=1)
    target_link_libraries(${TARGET} PUBLIC ${BACK_TESTER_ARROW_TARGET})
endif()
