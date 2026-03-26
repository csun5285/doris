# hyperscan (vectorscan) - has CMake, build from source
# Requires Boost headers and ragel
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_UNIT OFF CACHE BOOL "" FORCE)
set(FAT_RUNTIME OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/hyperscan-5.4.2 ${CMAKE_CURRENT_BINARY_DIR}/hyperscan EXCLUDE_FROM_ALL)

# Create hyperscan include structure to mimic "include/hs/hs.h"
set(HS_INC_DIR ${CMAKE_CURRENT_BINARY_DIR}/hyperscan_headers/include)
file(GLOB HS_SRC_HEADERS "${TP_SOURCE_DIR}/hyperscan-5.4.2/src/hs*.h")
file(COPY ${HS_SRC_HEADERS} DESTINATION "${HS_INC_DIR}/hs")

if(TARGET hs)
    target_include_directories(hs INTERFACE ${HS_INC_DIR})
    add_library(hyperscan ALIAS hs)
endif()
