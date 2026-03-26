# krb5_headers: copy krb5 headers from old/include to a build-dir location
set(KRB5_OLD_DIR "${TP_SOURCE_DIR}/../old/include")
set(KRB5_NESTED_DIR "${CMAKE_CURRENT_BINARY_DIR}/krb5_headers/include")
file(MAKE_DIRECTORY ${KRB5_NESTED_DIR})

# Copy individual krb5 headers
foreach(_h krb5.h com_err.h profile.h gssapi.h)
    if(EXISTS "${KRB5_OLD_DIR}/${_h}")
        file(COPY "${KRB5_OLD_DIR}/${_h}" DESTINATION ${KRB5_NESTED_DIR})
    endif()
endforeach()
# Copy krb5/ and gssapi/ subdirectories
foreach(_d krb5 gssapi)
    if(EXISTS "${KRB5_OLD_DIR}/${_d}")
        file(COPY "${KRB5_OLD_DIR}/${_d}" DESTINATION ${KRB5_NESTED_DIR})
    endif()
endforeach()

add_library(krb5_headers INTERFACE)
target_include_directories(krb5_headers SYSTEM INTERFACE ${KRB5_NESTED_DIR})
