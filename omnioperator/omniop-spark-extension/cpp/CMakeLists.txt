# project name
project(spark-thestral-plugin)

# required cmake version
cmake_minimum_required(VERSION 3.10)

# configure cmake
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_COMPILER "g++")

set(root_directory ${PROJECT_BINARY_DIR})

# configure file
configure_file(
    "${PROJECT_SOURCE_DIR}/config.h.in"
    "${PROJECT_SOURCE_DIR}/config.h"
)

aux_source_directory(${CMAKE_CURRENT_LIST_DIR} ROOT_SRCS)
# for header searching
include_directories(SYSTEM src)

# compile library
add_subdirectory(src)

message(STATUS "Build by ${CMAKE_BUILD_TYPE}")

option(BUILD_CPP_TESTS "test" ON)
message(STATUS "Option BUILD_CPP_TESTS: ${BUILD_CPP_TESTS}")
if(${BUILD_CPP_TESTS})
    enable_testing()
    add_subdirectory(test)
endif ()

# options
option(DEBUG_RUNTIME "Debug" OFF)
message(STATUS "Option DEBUG: ${DEBUG_RUNTIME}")

option(TRACE_RUNTIME "Trace" OFF)
message(STATUS "Option TRACE: ${TRACE_RUNTIME}")
