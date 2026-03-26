# xsimd config shim – provides xsimd target for Arrow
# Compute the xsimd include dir relative to this config file's location
# This file is at thirdparty/cmake/shims/xsimd/xsimdConfig.cmake
# xsimd source is at thirdparty/src/xsimd-13.0.0/include
get_filename_component(_xsimd_shim_dir "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)
get_filename_component(_tp_cmake_shims "${_xsimd_shim_dir}/.." ABSOLUTE)
get_filename_component(_tp_cmake "${_tp_cmake_shims}/.." ABSOLUTE)
get_filename_component(_tp_root "${_tp_cmake}/.." ABSOLUTE)
set(_xsimd_inc "${_tp_root}/src/xsimd-13.0.0/include")

if(NOT TARGET xsimd)
    add_library(xsimd INTERFACE IMPORTED)
    target_include_directories(xsimd INTERFACE "${_xsimd_inc}")
endif()
set(xsimd_FOUND TRUE)
set(${CMAKE_FIND_PACKAGE_NAME}_FOUND TRUE)
set(xsimd_VERSION "13.0.0")
