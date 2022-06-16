set(MAIN_PATH ${CMAKE_CURRENT_LIST_DIR})
configure_file(scan_test.h.in ${CMAKE_CURRENT_LIST_DIR}/scan_test.h)

aux_source_directory(${CMAKE_CURRENT_LIST_DIR} SCAN_TESTS_LIST)
set(SCAN_TEST_TARGET tablescantest)
add_library(${SCAN_TEST_TARGET} STATIC ${SCAN_TESTS_LIST})
target_compile_options(${SCAN_TEST_TARGET} PUBLIC )

target_include_directories(${SCAN_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${SCAN_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${SCAN_TEST_TARGET} PUBLIC /opt/lib/include)


