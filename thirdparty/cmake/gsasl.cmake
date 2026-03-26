# libgsasl — pure CMake build (no autoconf)
# Replaces execute_process(./configure && make) with direct add_library.
# Build libgsasl with all standard SASL mechanisms.

set(GSASL_SRC ${TP_SOURCE_DIR}/libgsasl-1.8.0)
set(GSASL_CONFIG_DIR ${CMAKE_CURRENT_BINARY_DIR}/gsasl_config)
file(MAKE_DIRECTORY ${GSASL_CONFIG_DIR})

# Run libgsasl configure to generate config.h and other gnulib headers
if(NOT EXISTS ${GSASL_SRC}/config.h)
    execute_process(
        COMMAND env CFLAGS=-fPIC CXXFLAGS=-fPIC ./configure --with-pic --disable-shared --with-gssapi-impl=no --without-libgcrypt --without-stringprep --without-idn
        WORKING_DIRECTORY ${GSASL_SRC}
        RESULT_VARIABLE GSASL_CONFIG_RES
    )
    if(NOT GSASL_CONFIG_RES EQUAL 0)
        message(FATAL_ERROR "gsasl configure failed")
    endif()
endif()

# --- Core library sources (src/) ---
set(GSASL_CORE_SRCS
    ${GSASL_SRC}/src/base64.c
    ${GSASL_SRC}/src/callback.c
    ${GSASL_SRC}/src/crypto.c
    ${GSASL_SRC}/src/done.c
    ${GSASL_SRC}/src/error.c
    ${GSASL_SRC}/src/free.c
    ${GSASL_SRC}/src/init.c
    ${GSASL_SRC}/src/listmech.c
    ${GSASL_SRC}/src/md5pwd.c
    ${GSASL_SRC}/src/mechname.c
    ${GSASL_SRC}/src/mechtools.c
    ${GSASL_SRC}/src/obsolete.c
    ${GSASL_SRC}/src/property.c
    ${GSASL_SRC}/src/register.c
    ${GSASL_SRC}/src/saslprep.c
    ${GSASL_SRC}/src/suggest.c
    ${GSASL_SRC}/src/supportp.c
    ${GSASL_SRC}/src/version.c
    ${GSASL_SRC}/src/xcode.c
    ${GSASL_SRC}/src/xfinish.c
    ${GSASL_SRC}/src/xstart.c
    ${GSASL_SRC}/src/xstep.c
    # Skip doxygen.c — documentation only
)

# --- Mechanism sources ---
set(GSASL_MECH_SRCS
    # ANONYMOUS
    ${GSASL_SRC}/anonymous/client.c
    ${GSASL_SRC}/anonymous/mechinfo.c
    ${GSASL_SRC}/anonymous/server.c
    # EXTERNAL
    ${GSASL_SRC}/external/client.c
    ${GSASL_SRC}/external/mechinfo.c
    ${GSASL_SRC}/external/server.c
    # LOGIN
    ${GSASL_SRC}/login/client.c
    ${GSASL_SRC}/login/mechinfo.c
    ${GSASL_SRC}/login/server.c
    # PLAIN
    ${GSASL_SRC}/plain/client.c
    ${GSASL_SRC}/plain/mechinfo.c
    ${GSASL_SRC}/plain/server.c
    # SECURID
    ${GSASL_SRC}/securid/client.c
    ${GSASL_SRC}/securid/mechinfo.c
    ${GSASL_SRC}/securid/server.c
    # CRAM-MD5
    ${GSASL_SRC}/cram-md5/challenge.c
    ${GSASL_SRC}/cram-md5/client.c
    ${GSASL_SRC}/cram-md5/digest.c
    ${GSASL_SRC}/cram-md5/mechinfo.c
    ${GSASL_SRC}/cram-md5/server.c
    # DIGEST-MD5
    ${GSASL_SRC}/digest-md5/client.c
    ${GSASL_SRC}/digest-md5/digesthmac.c
    ${GSASL_SRC}/digest-md5/free.c
    ${GSASL_SRC}/digest-md5/getsubopt.c
    ${GSASL_SRC}/digest-md5/mechinfo.c
    ${GSASL_SRC}/digest-md5/nonascii.c
    ${GSASL_SRC}/digest-md5/parser.c
    ${GSASL_SRC}/digest-md5/printer.c
    ${GSASL_SRC}/digest-md5/qop.c
    ${GSASL_SRC}/digest-md5/server.c
    ${GSASL_SRC}/digest-md5/session.c
    ${GSASL_SRC}/digest-md5/validate.c
    # SCRAM-SHA1
    ${GSASL_SRC}/scram/client.c
    ${GSASL_SRC}/scram/mechinfo.c
    ${GSASL_SRC}/scram/parser.c
    ${GSASL_SRC}/scram/printer.c
    ${GSASL_SRC}/scram/server.c
    ${GSASL_SRC}/scram/tokens.c
    ${GSASL_SRC}/scram/validate.c
)

# --- gnulib support sources (gl/) ---
# Use gnulib crypto (gc-gnulib.c) instead of libgcrypt (gc-libgcrypt.c)
set(GSASL_GL_SRCS
    ${GSASL_SRC}/gl/base64.c
    ${GSASL_SRC}/gl/c-ctype.c
    ${GSASL_SRC}/gl/gc-gnulib.c
    ${GSASL_SRC}/gl/gc-pbkdf2-sha1.c
    ${GSASL_SRC}/gl/hmac-md5.c
    ${GSASL_SRC}/gl/hmac-sha1.c
    ${GSASL_SRC}/gl/md5.c
    ${GSASL_SRC}/gl/memxor.c
    ${GSASL_SRC}/gl/sha1.c
)

add_library(_gsasl STATIC
    ${GSASL_CORE_SRCS}
    ${GSASL_MECH_SRCS}
    ${GSASL_GL_SRCS}
)

target_include_directories(_gsasl
    PRIVATE ${GSASL_SRC}               # config.h, mechanism subdirs
    PRIVATE ${GSASL_SRC}/src           # internal.h, gsasl-mech.h, etc.
    PRIVATE ${GSASL_SRC}/gl            # gnulib headers (gc.h, etc.)
    PRIVATE ${GSASL_SRC}/digest-md5    # qop.h, etc.
    PRIVATE ${GSASL_SRC}/cram-md5
    PRIVATE ${GSASL_SRC}/scram
    PRIVATE ${GSASL_SRC}/anonymous
    PRIVATE ${GSASL_SRC}/plain
    PRIVATE ${GSASL_SRC}/login
    PRIVATE ${GSASL_SRC}/external
    PUBLIC  ${GSASL_SRC}/src           # gsasl.h (public API)
)

target_compile_definitions(_gsasl PRIVATE
    HAVE_CONFIG_H
    _GNU_SOURCE
    "LOCALEDIR=\"/usr/share/locale\""
)

target_compile_options(_gsasl PRIVATE -fPIC -w)

add_library(gsasl ALIAS _gsasl)
