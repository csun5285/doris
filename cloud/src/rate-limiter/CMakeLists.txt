
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/rate-limiter")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/rate-limiter")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")

add_library(RateLimiter
    rate_limiter.cpp
    s3_rate_limiter.cpp
)

# vim: et tw=120 ts=4 sw=4 cc=80:
