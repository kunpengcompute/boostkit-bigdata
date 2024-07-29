include_directories(SYSTEM "/user/local/include")

set (PROJ_TARGET native_reader)


set (SOURCE_FILES
        jni/OrcColumnarBatchJniReader.cpp
        jni/jni_common.cpp
        jni/ParquetColumnarBatchJniReader.cpp
        parquet/ParquetReader.cpp
        parquet/ParquetColumnReader.cpp
        parquet/ParquetTypedRecordReader.cpp
        parquet/ParquetDecoder.cpp
        orcfile/OrcFileOverride.cc
        orcfile/OrcHdfsFileOverride.cc
        orcfile/OmniRowReaderImpl.cc
        orcfile/OmniRLEv2.cc
        orcfile/OmniColReader.cc
        orcfile/OmniByteRLE.cc
        common/UriInfo.cc
        filesystem/hdfs_file.cpp
        filesystem/hdfs_filesystem.cpp
        filesystem/io_exception.cpp
        filesystem/status.cpp
        arrowadapter/FileSystemAdapter.cc
        arrowadapter/UtilInternal.cc
        arrowadapter/HdfsAdapter.cc
        arrowadapter/LocalfsAdapter.cc
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
add_library (${PROJ_TARGET} SHARED ${SOURCE_FILES} ${PROTO_SRCS} ${PROTO_HDRS} ${PROTO_SRCS_VB} ${PROTO_HDRS_VB})

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

#JNI
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${PROJ_TARGET} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries (${PROJ_TARGET} PUBLIC
        Arrow::arrow_shared
        Parquet::parquet_shared
        orc
        boostkit-omniop-vector-1.5.0-aarch64
        hdfs
        )

set_target_properties(${PROJ_TARGET} PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

install(TARGETS ${PROJ_TARGET} DESTINATION lib)
