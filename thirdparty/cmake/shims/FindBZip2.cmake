# Find module shim for BZip2
if(TARGET libbz2)
    get_target_property(_bz2_src_dir libbz2 SOURCE_DIR)
    set(BZIP2_INCLUDE_DIR "${_bz2_src_dir}")
    set(BZIP2_LIBRARIES libbz2)
    set(BZIP2_FOUND TRUE)
    set(BZip2_FOUND TRUE)
    
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(BZip2 DEFAULT_MSG BZIP2_LIBRARIES BZIP2_INCLUDE_DIR)

    if(NOT TARGET BZip2::BZip2)
        add_library(BZip2::BZip2 INTERFACE IMPORTED GLOBAL)
        target_link_libraries(BZip2::BZip2 INTERFACE libbz2)
    endif()
endif()
