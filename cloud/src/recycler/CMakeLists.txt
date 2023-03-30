
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/recycler")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/recycler")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")

add_library(Recycler
    checker.cpp
    recycler_service.cpp
    recycler.cpp
    s3_accessor.cpp
)
