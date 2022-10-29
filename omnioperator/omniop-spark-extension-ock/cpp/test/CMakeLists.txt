add_subdirectory(shuffle)
add_subdirectory(utils)

# configure
set(TP_TEST_TARGET tptest)
set(MY_LINK
        ock_utils_test
        ock_shuffle_test
        )

# find gtest package
find_package(GTest REQUIRED)

set (UT_FILES
        tptest.cpp
        shuffle/ock_shuffle_test.cpp
        )

message("compile test")
# compile a executable file
add_executable(${TP_TEST_TARGET} ${UT_FILES})
# dependent libraries
target_link_libraries(${TP_TEST_TARGET}
        -Wl,--start-group gcov
        ${GTEST_BOTH_LIBRARIES}
        ${MY_LINK}
        gtest
        pthread
        stdc++
        dl
        boostkit-omniop-vector-1.1.0-aarch64
        securec
        ock_columnar_shuffle)

target_compile_options(${TP_TEST_TARGET} PUBLIC -g -O0 -fPIC)

if (${CMAKE_BUILD_TYPE} MATCHES "Debug")
    target_compile_options(${TP_TEST_TARGET} PUBLIC -g -O0 -fPIC)
else ()
    target_compile_options(${TP_TEST_TARGET} PUBLIC -g -O2 -fPIC)
endif ()
# dependent include
target_include_directories(${TP_TEST_TARGET} PRIVATE ${GTEST_INCLUDE_DIRS})

# discover tests
gtest_discover_tests(${TP_TEST_TARGET})