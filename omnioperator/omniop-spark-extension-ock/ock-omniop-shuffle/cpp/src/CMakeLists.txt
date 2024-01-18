set (PROJ_TARGET ock_columnar_shuffle)

set (SOURCE_FILES
        shuffle/ock_splitter.cpp
        shuffle/ock_hash_write_buffer.cpp
        shuffle/ock_merge_reader.cpp
        jni/OckShuffleJniWriter.cpp
        jni/OckShuffleJniReader.cpp
        )

# Find required protobuf package
find_package(Protobuf REQUIRED)
if(PROTOBUF_FOUND)
    message(STATUS "protobuf library found")
else()
    message(FATAL_ERROR "protobuf library is needed but cant be found")
endif()
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate_cpp(PROTO_SRCS_VB PROTO_HDRS_VB proto/vec_data.proto)
set(CMAKE_SKIP_RPATH TRUE)
add_library (${PROJ_TARGET} SHARED ${SOURCE_FILES} ${PROTO_SRCS} ${PROTO_HDRS} ${PROTO_SRCS_VB} ${PROTO_HDRS_VB})

# will setenv JAVA_HOME so use clion could compile cpp code in windows
if (NOT DEFINED ENV{JAVA_HOME})
    set(JAVA_HOME /usr/local/java/) # java may use ln -s to real java package
else ()
    set(JAVA_HOME $ENV{JAVA_HOME})
endif ()

# JNI
target_include_directories(${PROJ_TARGET} PUBLIC ${JAVA_HOME}/include)
target_include_directories(${PROJ_TARGET} PUBLIC ${JAVA_HOME}/include/linux)
target_include_directories(${PROJ_TARGET} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(${PROJ_TARGET} PUBLIC 3rdparty/omni/include)
target_include_directories(${PROJ_TARGET} PUBLIC /opt/lib/include)

target_link_libraries (${PROJ_TARGET} PUBLIC
        protobuf.a
        z
        boostkit-omniop-vector-1.3.0-aarch64
        ock_shuffle
        gcov
        )

set_target_properties(${PROJ_TARGET} PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

message("-- Build mode :${CMAKE_BUILD_TYPE}")
if (${CMAKE_BUILD_TYPE} MATCHES "Debug")
    target_compile_options(${PROJ_TARGET} PUBLIC -g -O0 -fPIC
            -ftest-coverage
            -fprofile-arcs
            -fdump-rtl-expand)
else ()
    target_compile_options(${PROJ_TARGET} PUBLIC
            -O2
            -fPIC
            -fstack-protector-strong)
    target_link_options(${PROJ_TARGET} PUBLIC
            -Wl,-z,relro,-z,now,-z,noexecstack
            -s)
endif ()
install(TARGETS ${PROJ_TARGET}
        DESTINATION lib
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ)