# mysql - use pre-built headers from old/ directory
# MySQL source headers have too many cross-references to build-generated files
# (mysql_version.h, binary_log_types.h, etc.), so we use the complete
# pre-installed header set from the old build system.
set(MYSQL_OLD_HEADERS "${TP_SOURCE_DIR}/../old/include/mysql")
set(MYSQL_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/mysql_headers/include/mysql)
file(MAKE_DIRECTORY ${MYSQL_NESTED_DIR})
if(EXISTS "${MYSQL_OLD_HEADERS}")
    file(GLOB _MYSQL_ALL_HEADERS "${MYSQL_OLD_HEADERS}/*")
    file(COPY ${_MYSQL_ALL_HEADERS} DESTINATION ${MYSQL_NESTED_DIR})
endif()
add_library(mysql_headers INTERFACE)
target_include_directories(mysql_headers SYSTEM INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/mysql_headers/include)
