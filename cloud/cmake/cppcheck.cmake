find_program(CPPCHECK NAMES cppcheck)
if (NOT CPPCHECK)
    message(STATUS "The check program cppcheck is not found")
else()
    message(STATUS "Find program: ${CPPCHECK}, enable CMAKE_CXX_CPPCHECK")
    set(CMAKE_CXX_CPPCHECK ${CPPCHECK})
    list(APPEND CMAKE_CXX_CPPCHECK "--enable=warning")
endif()

