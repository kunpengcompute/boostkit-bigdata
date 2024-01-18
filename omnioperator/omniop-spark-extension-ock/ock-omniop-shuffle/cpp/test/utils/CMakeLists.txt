## ---------------- test utils for ock shuffle --------------------
file(GLOB OCK_UTILS_TESTS_LIST  ock_test_utils.*)
set(OCK_UTILS_TEST_TARGET ock_utils_test)
add_library(${OCK_UTILS_TEST_TARGET} ${OCK_UTILS_TESTS_LIST})

# dependent libraries
target_link_libraries(${OCK_UTILS_TEST_TARGET} ock_columnar_shuffle)
target_compile_options(${OCK_UTILS_TEST_TARGET} PUBLIC)
target_include_directories(${OCK_UTILS_TEST_TARGET} PUBLIC ../../src/3rdparty/omni/include)
target_include_directories(${OCK_UTILS_TEST_TARGET} PUBLIC ${CMAKE_BINARY_DIR}/src)
target_include_directories(${OCK_UTILS_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${OCK_UTILS_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)