# krb5 - autoconf based, build from source
# Reference: build-thirdparty.sh build_krb5()
set(KRB5_SRC ${TP_SOURCE_DIR}/krb5-1.19)
set(KRB5_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/krb5)

set(KRB5_LIBS
    "${KRB5_BUILD_DIR}/lib/libkrb5support.a"
    "${KRB5_BUILD_DIR}/lib/libkrb5.a"
    "${KRB5_BUILD_DIR}/lib/libcom_err.a"
    "${KRB5_BUILD_DIR}/lib/libgssapi_krb5.a"
    "${KRB5_BUILD_DIR}/lib/libk5crypto.a")

include(ProcessorCount)
ProcessorCount(NPROC)

add_custom_command(
    OUTPUT ${KRB5_LIBS}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${KRB5_BUILD_DIR}
    COMMAND ${CMAKE_COMMAND} -E env
        "CFLAGS=-fcommon -fPIC -std=gnu89"
        "CXXFLAGS=-fPIC"
        ${KRB5_SRC}/src/configure
        --prefix=${KRB5_BUILD_DIR}
        --enable-static
        --disable-shared
        --without-keyutils
    COMMAND make -j${NPROC}
    WORKING_DIRECTORY ${KRB5_BUILD_DIR}
    COMMENT "Building krb5 from source..."
)
add_custom_target(krb5_builder DEPENDS ${KRB5_LIBS})

foreach(_lib krb5support krb5 com_err gssapi_krb5 k5crypto)
    add_library(${_lib} STATIC IMPORTED GLOBAL)
    set_target_properties(${_lib} PROPERTIES IMPORTED_LOCATION "${KRB5_BUILD_DIR}/lib/lib${_lib}.a")
    target_include_directories(${_lib} INTERFACE ${KRB5_BUILD_DIR}/include)
    add_dependencies(${_lib} krb5_builder)
endforeach()
