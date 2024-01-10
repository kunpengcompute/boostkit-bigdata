aux_source_directory(${CMAKE_CURRENT_LIST_DIR} ORC_FILE_TESTS_LIST)
set(MAIN_PATH ${CMAKE_CURRENT_SOURCE_DIR})

configure_file(orcfile_test.h.in ${CMAKE_CURRENT_SOURCE_DIR}/orcfile_test.h)
set(ORC_FILE_TARGET orcfiletest)

add_library(${ORC_FILE_TARGET} STATIC ${ORC_FILE_TESTS_LIST})
target_compile_options(${ORC_FILE_TARGET} PUBLIC )
target_include_directories(${ORC_FILE_TARGET} PUBLIC ${CMAKE_BINARY_DIR}/src)
target_include_directories(${ORC_FILE_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${ORC_FILE_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
