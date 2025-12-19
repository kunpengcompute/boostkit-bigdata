aux_source_directory(${CMAKE_CURRENT_LIST_DIR} TEST_ROOT_SRCS)

add_subdirectory(tablescan)
add_subdirectory(filesystem)
add_subdirectory(io/arrowadapter)
add_subdirectory(io/orcfile)

# configure
set(TP_TEST_TARGET tptest)
set(MY_LINK
        tablescantest
        filesystemtest
        arrowadaptertest
        orcfiletest
        )

# find gtest package
find_package(GTest REQUIRED)

# compile a executable file
add_executable(${TP_TEST_TARGET} ${ROOT_SRCS} ${TEST_ROOT_SRCS})

# dependent libraries
target_link_libraries(${TP_TEST_TARGET}
        ${GTEST_BOTH_LIBRARIES}
        ${SOURCE_LINK}
        -Wl,--whole-archive
        ${MY_LINK}
        -Wl,--no-whole-archive
        gtest
        pthread
        stdc++
        dl
        boostkit-omniop-vector-2.0.0-aarch64
        securec
        native_reader)

target_compile_options(${TP_TEST_TARGET} PUBLIC -g -O2 -fPIC)

# dependent include
target_include_directories(${TP_TEST_TARGET} PRIVATE ${GTEST_INCLUDE_DIRS})

# discover tests
gtest_discover_tests(${TP_TEST_TARGET})
