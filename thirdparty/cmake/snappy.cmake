# snappy
set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(SNAPPY_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/snappy-1.1.10 ${CMAKE_CURRENT_BINARY_DIR}/snappy EXCLUDE_FROM_ALL)
# Doris needs RTTI for SnappySlicesSource inheritance
if(TARGET snappy)
    target_compile_options(snappy PRIVATE -frtti)
    set_target_properties(snappy PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/../bin"
    )
endif()

# Create nested snappy/ include directory so #include <snappy/snappy.h> works
# (the source dir has snappy.h at top level, but Doris code uses snappy/snappy.h)
set(SNAPPY_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/snappy_headers/include/snappy)
file(MAKE_DIRECTORY ${SNAPPY_NESTED_DIR})
file(GLOB _SNAPPY_HEADERS "${TP_SOURCE_DIR}/snappy-1.1.10/snappy*.h")
file(COPY ${_SNAPPY_HEADERS} DESTINATION ${SNAPPY_NESTED_DIR})
# Also copy generated snappy-stubs-public.h
if(EXISTS "${CMAKE_CURRENT_BINARY_DIR}/snappy/snappy-stubs-public.h")
    file(COPY "${CMAKE_CURRENT_BINARY_DIR}/snappy/snappy-stubs-public.h" DESTINATION ${SNAPPY_NESTED_DIR})
endif()
add_library(snappy_headers INTERFACE)
target_include_directories(snappy_headers SYSTEM INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/snappy_headers/include)

# Create Snappy::snappy IMPORTED target so Arrow/Parquet can find it via find_package(Snappy)
# Without this, Arrow passes bare "snappy" (the CMake target name) to the linker instead of the .a path
set(_SNAPPY_LIB "${CMAKE_CURRENT_BINARY_DIR}/../bin/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX}")
if(NOT TARGET Snappy::snappy)
    add_library(Snappy::snappy STATIC IMPORTED GLOBAL)
    set_target_properties(Snappy::snappy PROPERTIES
        IMPORTED_LOCATION "${_SNAPPY_LIB}"
        INTERFACE_INCLUDE_DIRECTORIES "${TP_SOURCE_DIR}/snappy-1.1.10;${CMAKE_CURRENT_BINARY_DIR}/snappy"
    )
    add_dependencies(Snappy::snappy snappy)
endif()
# Also create Snappy::snappy-static so Arrow's FindSnappyAlt.cmake (line 36)
# finds it immediately instead of falling through to a buggy code path that
# creates a non-GLOBAL imported target (causing bare "snappy" in the link line).
if(NOT TARGET Snappy::snappy-static)
    add_library(Snappy::snappy-static STATIC IMPORTED GLOBAL)
    set_target_properties(Snappy::snappy-static PROPERTIES
        IMPORTED_LOCATION "${_SNAPPY_LIB}"
        INTERFACE_INCLUDE_DIRECTORIES "${TP_SOURCE_DIR}/snappy-1.1.10;${CMAKE_CURRENT_BINARY_DIR}/snappy"
    )
    add_dependencies(Snappy::snappy-static snappy)
endif()
# Set cache variables for Arrow's find_package(Snappy)
set(SNAPPY_LIBRARY "${_SNAPPY_LIB}" CACHE STRING "" FORCE)
set(SNAPPY_INCLUDE_DIR "${TP_SOURCE_DIR}/snappy-1.1.10" CACHE STRING "" FORCE)
set(Snappy_FOUND TRUE CACHE BOOL "" FORCE)
set(SNAPPY_FOUND TRUE CACHE BOOL "" FORCE)
# Pre-set SnappyAlt_FOUND so Arrow's FindSnappyAlt.cmake returns at line 18-20
# without executing any find logic (avoids the variable-name bug on line 45).
set(SnappyAlt_FOUND TRUE CACHE BOOL "" FORCE)
# Arrow uses ${Snappy_TARGET} internally (ThirdpartyToolchain.cmake line 1416 etc.)
# Must point to a valid IMPORTED target with correct IMPORTED_LOCATION.
set(Snappy_TARGET "Snappy::snappy-static" CACHE STRING "" FORCE)
