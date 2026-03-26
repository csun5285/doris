# libunwind_headers: copy libunwind headers from old/include
set(UNWIND_OLD_DIR "${TP_SOURCE_DIR}/../old/include")
set(UNWIND_NESTED_DIR "${CMAKE_CURRENT_BINARY_DIR}/libunwind_headers/include")
file(MAKE_DIRECTORY ${UNWIND_NESTED_DIR})

# Copy libunwind.h and related headers
foreach(_h libunwind.h libunwind-common.h libunwind-dynamic.h libunwind-x86_64.h libunwind-coredump.h unwind.h libunwind-ptrace.h)
    if(EXISTS "${UNWIND_OLD_DIR}/${_h}")
        file(COPY "${UNWIND_OLD_DIR}/${_h}" DESTINATION ${UNWIND_NESTED_DIR})
    endif()
endforeach()

add_library(libunwind_headers INTERFACE)
target_include_directories(libunwind_headers SYSTEM INTERFACE ${UNWIND_NESTED_DIR})
