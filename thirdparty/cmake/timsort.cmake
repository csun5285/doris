# timsort - single header file
add_library(timsort INTERFACE)
set(TIMSORT_INC_DIR ${CMAKE_CURRENT_BINARY_DIR}/timsort_headers/include)
file(COPY "${TP_SOURCE_DIR}/timsort.hpp" DESTINATION "${TIMSORT_INC_DIR}/gfx")
target_include_directories(timsort SYSTEM INTERFACE ${TIMSORT_INC_DIR})
