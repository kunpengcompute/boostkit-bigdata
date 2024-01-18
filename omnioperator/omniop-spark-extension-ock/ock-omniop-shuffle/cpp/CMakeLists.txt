# project name
project(ock-omniop-shuffle)

set(CMAKE_VERBOSE_MAKEFILE ON)

# required cmake version
cmake_minimum_required(VERSION 3.10)

# configure cmake
set(CMAKE_CXX_STANDARD 17)

set(root_directory ${PROJECT_BINARY_DIR})

# for header searching
include_directories(SYSTEM src)
include_directories(SYSTEM "src/3rdparty/omni/include")
include_directories(SYSTEM "src/3rdparty/datakit/include")
include_directories(SYSTEM "src/3rdparty/json/include")
include_directories(SYSTEM "src/3rdparty/")
link_directories(SYSTEM "src/3rdparty/omni/lib")
link_directories(SYSTEM "src/3rdparty/datakit/lib")

# compile library
add_subdirectory(src)

message(STATUS "Build by ${CMAKE_BUILD_TYPE}")

option(BUILD_CPP_TESTS "test" OFF)
message(STATUS "Option BUILD_CPP_TESTS: ${BUILD_CPP_TESTS}")
if (${BUILD_CPP_TESTS})
  enable_testing()
  add_subdirectory(test)
endif ()

# options
option(DEBUG_RUNTIME "Debug" OFF)
message(STATUS "Option DEBUG: ${DEBUG_RUNTIME}")

option(TRACE_RUNTIME "Trace" OFF)
message(STATUS "Option TRACE: ${TRACE_RUNTIME}")