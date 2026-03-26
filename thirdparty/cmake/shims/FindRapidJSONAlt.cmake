# Find module shim for RapidJSONAlt — header-only library
# Arrow's FindRapidJSONAlt.cmake checks RapidJSONAlt_FOUND at line 18-20
if(TARGET rapidjson)
    get_target_property(_rj_inc rapidjson INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _rj_inc 0 RAPIDJSON_INCLUDE_DIR)
    set(RAPIDJSON_VERSION "1.1.0")
    set(RapidJSONAlt_FOUND TRUE)
    set(RapidJSON_FOUND TRUE)
    
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(RapidJSONAlt
        REQUIRED_VARS RAPIDJSON_INCLUDE_DIR
        VERSION_VAR RAPIDJSON_VERSION)

    if(NOT TARGET RapidJSON)
        add_library(RapidJSON INTERFACE IMPORTED)
        target_include_directories(RapidJSON INTERFACE "${RAPIDJSON_INCLUDE_DIR}")
    endif()
endif()
