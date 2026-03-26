# librdkafka
set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_SSL ON CACHE BOOL "" FORCE)
set(WITH_SASL ON CACHE BOOL "" FORCE)
set(WITH_ZLIB ON CACHE BOOL "" FORCE)
set(WITH_ZSTD ON CACHE BOOL "" FORCE)
set(WITH_LIBDL ON CACHE BOOL "" FORCE)
set(WITH_PLUGINS OFF CACHE BOOL "" FORCE)
set(OPENSSL_ROOT_DIR "${CMAKE_CURRENT_BINARY_DIR}/openssl" CACHE PATH "" FORCE)

# Create OpenSSL::SSL and OpenSSL::Crypto IMPORTED targets if they don't exist
# Use source-built OpenSSL from thirdparty build directory
if(NOT TARGET OpenSSL::SSL)
    add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::SSL PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libssl.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/openssl/include"
    )
endif()
if(NOT TARGET OpenSSL::Crypto)
    add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::Crypto PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libcrypto.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/openssl/include"
    )
endif()

# Pre-set dependency vars so rdkafka cmake can find zstd/lz4/sasl
# Create IMPORTED targets for ZSTD and LZ4 so rdkafka find_package succeeds
# and rdkafka gets correct link dependencies through cmake targets.

# ZSTD — create ZSTD::ZSTD imported target
if(NOT TARGET ZSTD::ZSTD)
    add_library(ZSTD::ZSTD ALIAS libzstd_static)
endif()
set(ZSTD_FOUND TRUE CACHE BOOL "" FORCE)
set(ZSTD_INCLUDE_DIRS "${TP_SOURCE_DIR}/zstd-1.5.7/lib" CACHE PATH "" FORCE)
set(ZSTD_INCLUDE_DIR "${TP_SOURCE_DIR}/zstd-1.5.7/lib" CACHE PATH "" FORCE)

# LZ4 — create LZ4::LZ4 imported target
if(NOT TARGET LZ4::LZ4)
    add_library(LZ4::LZ4 ALIAS lz4_static)
endif()
set(LZ4_FOUND TRUE CACHE BOOL "" FORCE)
set(LZ4_INCLUDE_DIRS "${TP_SOURCE_DIR}/lz4-1.9.4/lib" CACHE PATH "" FORCE)
set(LZ4_INCLUDE_DIR "${TP_SOURCE_DIR}/lz4-1.9.4/lib" CACHE PATH "" FORCE)

# SASL - sasl.h is installed to cyrus-sasl build dir
set(SASL_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/cyrus-sasl/include" CACHE PATH "" FORCE)

add_subdirectory(${TP_SOURCE_DIR}/librdkafka-2.11.0 ${CMAKE_CURRENT_BINARY_DIR}/rdkafka EXCLUDE_FROM_ALL)
if(TARGET rdkafka)
    add_library(rdkafka_cpp ALIAS rdkafka++)
    # Link rdkafka against actual zstd/lz4 targets so the libraries are found
    target_link_libraries(rdkafka PRIVATE libzstd_static lz4_static cyrus-sasl)
    # Add include dirs as PRIVATE to avoid polluting global scope
    target_include_directories(rdkafka PRIVATE
        ${TP_SOURCE_DIR}/zstd-1.5.7/lib
        ${TP_SOURCE_DIR}/lz4-1.9.4/lib
        ${CMAKE_CURRENT_BINARY_DIR}/cyrus-sasl/include
    )
endif()

# Create nested librdkafka/ directory so #include <librdkafka/rdkafkacpp.h> works
set(RDKAFKA_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/rdkafka_headers/include/librdkafka)
file(MAKE_DIRECTORY ${RDKAFKA_NESTED_DIR})
# Copy public headers from src/ and src-cpp/
file(GLOB _RDKAFKA_PUB_H "${TP_SOURCE_DIR}/librdkafka-2.11.0/src/rdkafka.h")
file(GLOB _RDKAFKA_CPP_H "${TP_SOURCE_DIR}/librdkafka-2.11.0/src-cpp/rdkafkacpp.h")
if(_RDKAFKA_PUB_H)
    file(COPY ${_RDKAFKA_PUB_H} DESTINATION ${RDKAFKA_NESTED_DIR})
endif()
if(_RDKAFKA_CPP_H)
    file(COPY ${_RDKAFKA_CPP_H} DESTINATION ${RDKAFKA_NESTED_DIR})
endif()
# Also copy generated rdkafka config header
if(EXISTS "${CMAKE_CURRENT_BINARY_DIR}/rdkafka/src/rdkafka.h")
    file(COPY "${CMAKE_CURRENT_BINARY_DIR}/rdkafka/src/rdkafka.h" DESTINATION ${RDKAFKA_NESTED_DIR})
endif()
add_library(rdkafka_headers INTERFACE)
target_include_directories(rdkafka_headers SYSTEM INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/rdkafka_headers/include)
