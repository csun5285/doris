# unixODBC — pure CMake build (no autoconf)
# Build ODBC driver manager only (DriverManager/) for static linking.

set(ODBC_SRC ${TP_SOURCE_DIR}/unixODBC-2.3.7)
set(ODBC_CONFIG_DIR ${CMAKE_CURRENT_BINARY_DIR}/odbc_config)
file(MAKE_DIRECTORY ${ODBC_CONFIG_DIR})

set(ODBC_CONFIG_H "${ODBC_SRC}/config.h")
add_custom_command(
    OUTPUT ${ODBC_CONFIG_H}
    COMMAND env CFLAGS=-fPIC CXXFLAGS=-fPIC ./configure --with-pic --disable-shared --disable-readline
    COMMAND bash -c "cd libltdl && env CFLAGS=-fPIC CXXFLAGS=-fPIC ./configure --with-pic --disable-shared"
    WORKING_DIRECTORY ${ODBC_SRC}
    COMMENT "Configuring unixODBC..."
)
add_custom_target(odbc_config_headers DEPENDS ${ODBC_CONFIG_H})

# Collect DriverManager .c files via GLOB (122 files)
file(GLOB ODBC_DM_SRCS "${ODBC_SRC}/DriverManager/*.c")

# Also need log/ and ini/ (configuration parser)
file(GLOB ODBC_LOG_SRCS "${ODBC_SRC}/log/*.c")
file(GLOB ODBC_INI_SRCS "${ODBC_SRC}/ini/*.c")
file(GLOB ODBC_LST_SRCS "${ODBC_SRC}/lst/*.c")
set(ODBC_LTDL_SRCS
    ${ODBC_SRC}/libltdl/lt__alloc.c
    ${ODBC_SRC}/libltdl/lt_dlloader.c
    ${ODBC_SRC}/libltdl/lt_error.c
    ${ODBC_SRC}/libltdl/ltdl.c
    ${ODBC_SRC}/libltdl/slist.c
    ${ODBC_SRC}/libltdl/loaders/dlopen.c
)

add_library(_odbc STATIC
    ${ODBC_DM_SRCS}
    ${ODBC_LOG_SRCS}
    ${ODBC_INI_SRCS}
    ${ODBC_LST_SRCS}
    ${ODBC_LTDL_SRCS}
)
add_dependencies(_odbc odbc_config_headers)

target_include_directories(_odbc
    PRIVATE ${ODBC_SRC}
    PRIVATE ${ODBC_SRC}/DriverManager
    PRIVATE ${ODBC_SRC}/log
    PRIVATE ${ODBC_SRC}/ini
    PRIVATE ${ODBC_SRC}/lst
    PRIVATE ${ODBC_SRC}/libltdl
    PRIVATE ${ODBC_SRC}/libltdl/libltdl
    PUBLIC  ${ODBC_SRC}/include
)

target_compile_definitions(_odbc PRIVATE
    HAVE_CONFIG_H
    _GNU_SOURCE
    UNIXODBC
    LT_SCOPE=extern
    LTDL=1
)

target_compile_options(_odbc PRIVATE
    -fPIC -w
    -Wno-int-conversion -std=gnu89
    -Wno-implicit-function-declaration
)

target_link_libraries(_odbc PRIVATE pthread dl)

add_library(odbc ALIAS _odbc)
