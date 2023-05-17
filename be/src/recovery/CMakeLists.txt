# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/recovery")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/recovery")

add_executable(recovery_tool
    recovery_tool.cpp
)

add_library(cos_sdk STATIC IMPORTED)
set_target_properties(cos_sdk PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libcossdk.a)

add_library(poco-crypto STATIC IMPORTED)
set_target_properties(poco-crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoCrypto.a)

add_library(poco-encodings STATIC IMPORTED)
set_target_properties(poco-encodings PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoEncodings.a)

add_library(poco-MongoDB STATIC IMPORTED)
set_target_properties(poco-MongoDB PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoMongoDB.a)

add_library(poco-redis STATIC IMPORTED)
set_target_properties(poco-redis PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoRedis.a)

add_library(poco-zip STATIC IMPORTED)
set_target_properties(poco-zip PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoZip.a)

add_library(poco-data STATIC IMPORTED)
set_target_properties(poco-data PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoData.a)

add_library(poco-foundation STATIC IMPORTED)
set_target_properties(poco-foundation PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoFoundation.a)

add_library(poco-net STATIC IMPORTED)
set_target_properties(poco-net PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoNet.a)

add_library(poco-util STATIC IMPORTED)
set_target_properties(poco-util PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoUtil.a)

add_library(poco-datasqlite STATIC IMPORTED)
set_target_properties(poco-datasqlite PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoDataSQLite.a)

add_library(poco-json STATIC IMPORTED)
set_target_properties(poco-json PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoJSON.a)

add_library(poco-netssl STATIC IMPORTED)
set_target_properties(poco-netssl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoNetSSL.a)

add_library(poco-xml STATIC IMPORTED)
set_target_properties(poco-xml PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/poco/libPocoXML.a)

set(COS_SDK_LIBS
    cos_sdk
    poco-crypto
    poco-encodings
    poco-MongoDB
    poco-redis
    poco-zip
    poco-data
    poco-foundation
    poco-net
    poco-util
    poco-datasqlite
    poco-json
    poco-netssl
    poco-xml)

set(THIRDPARTY
    glog
    curl
    fmt
    gflags
    protobuf
    openssl
    crypto
    zstd
    ${AWS_LIBS}
    ${COS_SDK_LIBS}
    )

set(RECOVERY_TOOL_LINK_LIBS
    ${RECOVERY_TOOL_LINK_LIBS}
    ${THIRDPARTY}
    -lstdc++fs
    -lresolv
)

# Add sanitize static link flags
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(RECOVERY_TOOL_LINK_LIBS ${RECOVERY_TOOL_LINK_LIBS} ${MALLOCLIB})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    set(RECOVERY_TOOL_LINK_LIBS ${RECOVERY_TOOL_LINK_LIBS} ${ASAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(RECOVERY_TOOL_LINK_LIBS ${RECOVERY_TOOL_LINK_LIBS} ${LSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    set(RECOVERY_TOOL_LINK_LIBS ${RECOVERY_TOOL_LINK_LIBS} ${UBSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(RECOVERY_TOOL_LINK_LIBS ${RECOVERY_TOOL_LINK_LIBS} ${TSAN_LIBS})
    add_definitions("-DTHREAD_SANITIZER")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()


if (NOT OS_MACOSX)
    set(RECOVERY_TOOL_LINK_LIBS ${RECOVERY_TOOL_LINK_LIBS}
        -lrt -l:libbfd.a -liberty -lc -lm -ldl -pthread
    )
else()
    set(RECOVERY_TOOL_LINK_LIBS ${RECOVERY_TOOL_LINK_LIBS}
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreText"
        "-framework Foundation"
        "-framework SystemConfiguration"
        "-framework Security"
    )
    if (USE_JEMALLOC OR (NOT CMAKE_BUILD_TYPE STREQUAL "DEBUG" AND NOT CMAKE_BUILD_TYPE STREQUAL "RELEASE"))
        set(RECOVERY_TOOL_LINK_LIBS
            ${RECOVERY_TOOL_LINK_LIBS}
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        )
    endif()
endif()

target_link_libraries(recovery_tool ${RECOVERY_TOOL_LINK_LIBS})

install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
install(TARGETS recovery_tool DESTINATION ${OUTPUT_DIR}/lib/)