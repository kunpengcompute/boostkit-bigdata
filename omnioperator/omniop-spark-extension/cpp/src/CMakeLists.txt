include_directories(SYSTEM "/user/local/include")

set (PROJ_TARGET spark_columnar_plugin)


set (SOURCE_FILES
        io/ColumnWriter.cc
        io/Compression.cc
        io/MemoryPool.cc
        io/OrcObsFile.cc
        io/OutputStream.cc
        io/SparkFile.cc
        io/WriterOptions.cc
        io/orcfile/OrcFileRewrite.cc
        io/orcfile/OrcHdfsFileRewrite.cc
        shuffle/splitter.cpp
        common/common.cpp
        jni/SparkJniWrapper.cpp
        jni/OrcColumnarBatchJniReader.cpp
        jni/jni_common.cpp
        )

#Find required protobuf package
find_package(Protobuf REQUIRED)
if(PROTOBUF_FOUND)
    message(STATUS "protobuf library found")
else()
    message(FATAL_ERROR "protobuf library is needed but cant be found")
endif()

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate_cpp(PROTO_SRCS_VB PROTO_HDRS_VB proto/vec_data.proto)
add_library (${PROJ_TARGET} SHARED ${SOURCE_FILES} ${PROTO_SRCS} ${PROTO_HDRS} ${PROTO_SRCS_VB} ${PROTO_HDRS_VB})

#JNI
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${PROJ_TARGET} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries (${PROJ_TARGET} PUBLIC
        orc
        crypto
        sasl2
        protobuf
        z
        snappy
        lz4
        zstd
        eSDKOBS
        boostkit-omniop-vector-1.3.0-aarch64
        )

set_target_properties(${PROJ_TARGET} PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

install(TARGETS ${PROJ_TARGET} DESTINATION lib)
