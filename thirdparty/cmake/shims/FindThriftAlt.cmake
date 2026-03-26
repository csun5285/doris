# Find module shim for ThriftAlt – exposes Doris's source-built thrift to Arrow
# Arrow's own FindThriftAlt.cmake checks ThriftAlt_FOUND and returns early,
# so we just need to set the right variables before it runs.

if(TARGET thrift_static)
    # Use the target directly instead of a file path — avoids generate-time file existence checks
    get_target_property(_thrift_binary_dir thrift_static BINARY_DIR)
    set(ThriftAlt_LIB "${_thrift_binary_dir}/lib/libthrift.a" CACHE INTERNAL "")
    set(ThriftAlt_INCLUDE_DIR "${TP_SOURCE_DIR}/thrift-0.16.0/lib/cpp/src" CACHE INTERNAL "")
    set(ThriftAlt_VERSION "0.16.0" CACHE INTERNAL "")
    set(ThriftAlt_FOUND TRUE CACHE INTERNAL "")
    set(Thrift_FOUND TRUE CACHE INTERNAL "")
    set(Thrift_VERSION "0.16.0" CACHE INTERNAL "")
    set(THRIFT_VERSION "0.16.0" CACHE INTERNAL "")

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(ThriftAlt
        REQUIRED_VARS ThriftAlt_LIB ThriftAlt_INCLUDE_DIR
        VERSION_VAR ThriftAlt_VERSION)

    if(NOT TARGET thrift::thrift)
        # Use INTERFACE IMPORTED with target_link_libraries to avoid
        # file-existence checks that STATIC IMPORTED + IMPORTED_LOCATION triggers
        add_library(thrift::thrift INTERFACE IMPORTED GLOBAL)
        target_link_libraries(thrift::thrift INTERFACE thrift_static)
        target_include_directories(thrift::thrift INTERFACE "${ThriftAlt_INCLUDE_DIR}")
    endif()
endif()
