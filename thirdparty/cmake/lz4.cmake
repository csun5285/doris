# lz4
set(LZ4_BUILD_CLI OFF CACHE BOOL "" FORCE)
set(LZ4_BUILD_LEGACY_LZ4C OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/lz4-1.9.4/build/cmake ${CMAKE_CURRENT_BINARY_DIR}/lz4 EXCLUDE_FROM_ALL)
if(TARGET lz4_static)
    add_library(lz4 ALIAS lz4_static)
endif()

# Create nested lz4/ include directory so #include <lz4/lz4.h> works
set(LZ4_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/lz4_headers/include/lz4)
file(MAKE_DIRECTORY ${LZ4_NESTED_DIR})
file(GLOB _LZ4_HEADERS "${TP_SOURCE_DIR}/lz4-1.9.4/lib/lz4*.h")
file(COPY ${_LZ4_HEADERS} DESTINATION ${LZ4_NESTED_DIR})
add_library(lz4_headers INTERFACE)
target_include_directories(lz4_headers SYSTEM INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/lz4_headers/include)
