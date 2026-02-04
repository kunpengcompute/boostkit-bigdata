include_directories(SYSTEM "/user/local/include")

set(PROJ_TARGET native_reader)


set(SOURCE_FILES
        jni/OrcColumnarBatchJniWriter.cpp
        jni/OrcColumnarBatchJniReader.cpp
        jni/jni_common.cpp
        jni/ParquetColumnarBatchJniReader.cpp
        jni/ParquetColumnarBatchJniWriter.cpp
        parquet/ParquetReader.cpp
        parquet/ParquetColumnReader.cpp
        parquet/ParquetTypedRecordReader.cpp
        parquet/ParquetDecoder.cpp
        parquet/ParquetExpression.cpp
        parquet/ParquetWriter.cpp
        common/UriInfo.cc
        orcfile/OrcFileOverride.cc
        orcfile/OrcHdfsFileOverride.cc
        orcfile/OmniRowReaderImpl.cc
        orcfile/OmniRLEv2.cc
        orcfile/OmniRLE.cc
        orcfile/OmniColReader.cc
        orcfile/OmniByteRLE.cc
        orcfile/OrcUtil.cpp
        orcfile/OmniColumnWriter.cc
        orcfile/OmniWriter.cc
        common/UriInfo.cc
        common/RebaseDate.cpp
        filesystem/hdfs_file.cpp
        filesystem/hdfs_filesystem.cpp
        filesystem/io_exception.cpp
        filesystem/status.cpp
        arrowadapter/FileSystemAdapter.cc
        arrowadapter/UtilInternal.cc
        arrowadapter/HdfsAdapter.cc
        arrowadapter/LocalfsAdapter.cc
        common/JulianGregorianRebase.cpp
        common/TimeRebaseInfo.cpp
        common/PredicateUtil.cpp
        common/Directories.cpp
        common/Directories.h)

#Find required protobuf package
find_package(Protobuf REQUIRED)
if (PROTOBUF_FOUND)
    message(STATUS "protobuf library found")
else ()
    message(FATAL_ERROR "protobuf library is needed but cant be found")
endif ()

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_library(${PROJ_TARGET} SHARED ${SOURCE_FILES} ${PROTO_SRCS} ${PROTO_HDRS} ${PROTO_SRCS_VB} ${PROTO_HDRS_VB})

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(nlohmann_json 3.7.3 REQUIRED)

#JNI
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${PROJ_TARGET} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(${PROJ_TARGET} PUBLIC
        Arrow::arrow_shared
        ArrowDataset::arrow_dataset_shared
        Parquet::parquet_shared
        nlohmann_json::nlohmann_json
        orc
        boostkit-omniop-vector-2.0.0-aarch64
        hdfs
)

set_target_properties(${PROJ_TARGET} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

install(TARGETS ${PROJ_TARGET} DESTINATION lib)
