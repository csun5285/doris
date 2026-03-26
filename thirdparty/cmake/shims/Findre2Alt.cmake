# Find module shim for re2Alt
if(TARGET re2)
    get_target_property(_re2_src re2 SOURCE_DIR)
    set(RE2_INCLUDE_DIR "${_re2_src}")
    set(RE2_LIB re2)
    set(re2Alt_FOUND TRUE)
    set(re2_FOUND TRUE)
    
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(re2Alt DEFAULT_MSG RE2_LIB RE2_INCLUDE_DIR)

    if(NOT TARGET re2::re2)
        add_library(re2::re2 INTERFACE IMPORTED GLOBAL)
        target_link_libraries(re2::re2 INTERFACE re2)
    endif()
endif()
