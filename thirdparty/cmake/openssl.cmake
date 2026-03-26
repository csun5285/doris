# openssl — pure CMake build (no autoconf/Configure)
# Build libssl.a + libcrypto.a from source for Linux x86_64.
# OpenSSL has ~285 .c in crypto/ and ~44 in ssl/.
# Uses GLOB to collect source files. Platform-specific config via generated headers.

set(SSL_SRC ${TP_SOURCE_DIR}/openssl-OpenSSL_1_1_1s)
set(SSL_CONFIG_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl_config)
file(MAKE_DIRECTORY ${SSL_CONFIG_DIR}/openssl)
file(MAKE_DIRECTORY ${SSL_CONFIG_DIR}/crypto)

# --- Step 1: Run Configure once to generate required headers during build phase ---
# OpenSSL's Configure generates buildinf.h, opensslconf.h, bn_conf.h, dso_conf.h
set(SSL_CONFIGDATA "${SSL_CONFIG_DIR}/configdata.pm")
set(SSL_BUILDINF "${SSL_CONFIG_DIR}/crypto/buildinf.h")
add_custom_command(
    OUTPUT ${SSL_CONFIGDATA} ${SSL_BUILDINF}
    COMMAND perl ${SSL_SRC}/Configure --prefix=${SSL_CONFIG_DIR} --openssldir=${SSL_CONFIG_DIR} no-shared no-tests no-asm -fPIC linux-x86_64
    COMMAND make -j1 build_generated crypto/buildinf.h
    WORKING_DIRECTORY ${SSL_CONFIG_DIR}
    COMMENT "Running OpenSSL Configure to generate headers..."
)
add_custom_target(openssl_config_headers DEPENDS ${SSL_CONFIGDATA} ${SSL_BUILDINF})

# --- Step 2: Collect source files ---
file(GLOB_RECURSE SSL_CRYPTO_SRCS "${SSL_SRC}/crypto/*.c")
file(GLOB_RECURSE SSL_SSL_SRCS "${SSL_SRC}/ssl/*.c")

# Remove platform-specific files we don't need
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "LPdir_unix|LPdir_win|LPdir_wince|LPdir_vms|LPdir_nyi")
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "armcap\\.c|ppccap\\.c|s390xcap\\.c|sparcv9cap\\.c")
# Remove test programs
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "test")
# Remove aes_x86core.c (conflicts with aes_core.c)
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "aes_x86core\\.c")
# Remove algorithm directories disabled by default / configuration
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "crypto/md2/")
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "crypto/rc5/")

# Remove engine/eng_devcrypto.c (needs /dev/crypto support not always present)
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "eng_devcrypto")
# Remove crypto/ec/ecp_nistz256_table.c (causes compilation issues with some compilers/flags)
list(FILTER SSL_CRYPTO_SRCS EXCLUDE REGEX "ecp_nistz256_table\\.c")

# libcrypto
add_library(_crypto STATIC ${SSL_CRYPTO_SRCS})
add_dependencies(_crypto openssl_config_headers)
target_include_directories(_crypto
    PRIVATE ${SSL_CONFIG_DIR}           # generated opensslconf.h, buildinf.h
    PRIVATE ${SSL_CONFIG_DIR}/include   # generated include/openssl/opensslconf.h
    PRIVATE ${SSL_CONFIG_DIR}/crypto    # generated bn_conf.h, dso_conf.h
    PRIVATE ${SSL_SRC}                  # e_os.h
    PRIVATE ${SSL_SRC}/crypto           # LPdir_unix.c etc.
    PRIVATE ${SSL_SRC}/crypto/ec/curve448/arch_32
    PRIVATE ${SSL_SRC}/crypto/ec/curve448
    PRIVATE ${SSL_SRC}/crypto/modes
    PUBLIC  ${SSL_SRC}/include          # openssl/*.h
    PUBLIC  ${SSL_CONFIG_DIR}/include   # openssl/opensslconf.h (generated)
)
target_compile_definitions(_crypto PRIVATE
    OPENSSL_NO_ASM
    OPENSSL_NO_STATIC_ENGINE
    OPENSSLDIR="\\\"/etc/pki/tls\\\""
    ENGINESDIR="\\\"/dev/null\\\""
    _GNU_SOURCE
    NDEBUG
)
target_compile_options(_crypto PRIVATE -fPIC -w -pthread)
target_link_libraries(_crypto PRIVATE pthread dl)
set_target_properties(_crypto PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

# libssl
add_library(_ssl STATIC ${SSL_SSL_SRCS})
target_include_directories(_ssl
    PRIVATE ${SSL_CONFIG_DIR}
    PRIVATE ${SSL_CONFIG_DIR}/include
    PRIVATE ${SSL_SRC}
    PUBLIC  ${SSL_SRC}/include
    PUBLIC  ${SSL_CONFIG_DIR}/include
)
target_compile_definitions(_ssl PRIVATE
    OPENSSL_NO_ASM
    OPENSSL_NO_STATIC_ENGINE
    OPENSSLDIR="\\\"/etc/pki/tls\\\""
    ENGINESDIR="\\\"/dev/null\\\""
    _GNU_SOURCE
    NDEBUG
)
target_compile_options(_ssl PRIVATE -fPIC -w -pthread)
target_link_libraries(_ssl PRIVATE _crypto pthread)
set_target_properties(_ssl PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

# Create IMPORTED targets for compatibility with other projects (e.g. s2n try_compile)
# We avoid ALIAS targets because try_compile does not support aliasing a normal target.

add_library(crypto STATIC IMPORTED GLOBAL)
set_target_properties(crypto PROPERTIES
    IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/lib/lib_crypto.a"
    INTERFACE_INCLUDE_DIRECTORIES "${SSL_SRC}/include;${SSL_CONFIG_DIR}/include"
)
add_dependencies(crypto _crypto)

add_library(openssl STATIC IMPORTED GLOBAL)
set_target_properties(openssl PROPERTIES
    IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/lib/lib_ssl.a"
    INTERFACE_INCLUDE_DIRECTORIES "${SSL_SRC}/include;${SSL_CONFIG_DIR}/include"
)
add_dependencies(openssl _ssl)

# Create OpenSSL::SSL / OpenSSL::Crypto IMPORTED targets for find_package(OpenSSL) compat
if(NOT TARGET OpenSSL::SSL)
    add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::SSL PROPERTIES
        IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/lib/lib_ssl.a"
        INTERFACE_INCLUDE_DIRECTORIES "${SSL_SRC}/include;${SSL_CONFIG_DIR}/include"
    )
    add_dependencies(OpenSSL::SSL _ssl)
endif()
if(NOT TARGET OpenSSL::Crypto)
    add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::Crypto PROPERTIES
        IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/lib/lib_crypto.a"
        INTERFACE_INCLUDE_DIRECTORIES "${SSL_SRC}/include;${SSL_CONFIG_DIR}/include"
    )
    add_dependencies(OpenSSL::Crypto _crypto)
endif()

set(OPENSSL_ROOT_DIR "${SSL_SRC}" CACHE PATH "" FORCE)
set(OPENSSL_INCLUDE_DIR "${SSL_SRC}/include;${SSL_CONFIG_DIR}/include" CACHE STRING "" FORCE)
set(OPENSSL_SSL_LIBRARY "${CMAKE_BINARY_DIR}/lib/lib_ssl.a" CACHE FILEPATH "" FORCE)
set(OPENSSL_CRYPTO_LIBRARY "${CMAKE_BINARY_DIR}/lib/lib_crypto.a" CACHE FILEPATH "" FORCE)
set(OPENSSL_LIBRARIES "${CMAKE_BINARY_DIR}/lib/lib_ssl.a;${CMAKE_BINARY_DIR}/lib/lib_crypto.a" CACHE STRING "" FORCE)
set(OPENSSL_FOUND TRUE CACHE BOOL "" FORCE)
set(OPENSSL_VERSION "1.1.1s" CACHE STRING "" FORCE)
