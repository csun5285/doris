# cyrus-sasl - autoconf based, build from source
# Reference: build-thirdparty.sh build_cyrus_sasl()
set(CYRUS_SASL_SRC ${TP_SOURCE_DIR}/cyrus-sasl-2.1.27)
set(CYRUS_SASL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/cyrus-sasl)
set(KRB5_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/krb5)
set(OPENSSL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl)

set(CYRUS_SASL_A "${CYRUS_SASL_BUILD_DIR}/lib/.libs/libsasl2.a")

include(ProcessorCount)
ProcessorCount(NPROC)

add_custom_command(
    OUTPUT ${CYRUS_SASL_A}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CYRUS_SASL_BUILD_DIR}
    # CFLAGS/LIBS from build-thirdparty.sh
    COMMAND ${CMAKE_COMMAND} -E env
        "CFLAGS=-fPIC -std=gnu89 -Wno-implicit-function-declaration"
        "CPPFLAGS=-I${OPENSSL_BUILD_DIR}/include -I${KRB5_BUILD_DIR}/include"
        "LDFLAGS=-L${OPENSSL_BUILD_DIR} -L${KRB5_BUILD_DIR}/lib"
        "LIBS=-lcrypto"
        ${CYRUS_SASL_SRC}/configure
        --prefix=${CYRUS_SASL_BUILD_DIR}
        --enable-static --enable-shared=no
        --with-openssl=${OPENSSL_BUILD_DIR}
        --with-pic
        --enable-gssapi=${KRB5_BUILD_DIR}
        --with-gss_impl=mit
        --with-dblib=none
    COMMAND make -j${NPROC}
    COMMAND bash -c "mkdir -p ${CYRUS_SASL_BUILD_DIR}/include/sasl && cp -p ${CYRUS_SASL_SRC}/include/*.h ${CYRUS_SASL_BUILD_DIR}/include/sasl/ && cp -p ${CYRUS_SASL_BUILD_DIR}/include/*.h ${CYRUS_SASL_BUILD_DIR}/include/sasl/ || true"
    WORKING_DIRECTORY ${CYRUS_SASL_BUILD_DIR}
    COMMENT "Building cyrus-sasl from source..."
)
add_custom_target(cyrus_sasl_builder DEPENDS ${CYRUS_SASL_A})
add_dependencies(cyrus_sasl_builder openssl_config_headers krb5_builder)

add_library(cyrus-sasl STATIC IMPORTED GLOBAL)
# libsasl2.a is in lib/.libs/ after make (autoconf libtool pattern)
if(EXISTS "${CYRUS_SASL_BUILD_DIR}/lib/libsasl2.a")
    set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION "${CYRUS_SASL_BUILD_DIR}/lib/libsasl2.a")
elseif(EXISTS "${CYRUS_SASL_BUILD_DIR}/lib/.libs/libsasl2.a")
    set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION "${CYRUS_SASL_BUILD_DIR}/lib/.libs/libsasl2.a")
else()
    message(WARNING "[contrib] cyrus-sasl library not found, using placeholder path")
    set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION "${CYRUS_SASL_BUILD_DIR}/lib/libsasl2.a")
endif()
target_include_directories(cyrus-sasl INTERFACE ${CYRUS_SASL_BUILD_DIR}/include)
