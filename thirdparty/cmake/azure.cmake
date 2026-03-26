# Azure SDK for C++ - has CMake, build from source
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_PERFORMANCE_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
set(MSVC OFF) # ensure non-MSVC path
# Disable auto vcpkg fetching (Azure checks ENV, not CMake variable!)
set(ENV{AZURE_SDK_DISABLE_AUTO_VCPKG} "1")
set(AZURE_SDK_DISABLE_AUTO_VCPKG ON CACHE BOOL "" FORCE)
# Use the OpenSSL and LibXml2 cache variables already set globally by Doris thirdparty
# Disable opentelemetry tracing (not available)
set(BUILD_AZURE_CORE_OPENTELEMETRY OFF CACHE BOOL "" FORCE)
set(DISABLE_AZURE_CORE_OPENTELEMETRY ON CACHE BOOL "" FORCE)

# Remove opentelemetry subdirectory from the build
set(_AZURE_SRC "${TP_SOURCE_DIR}/azure-sdk-for-cpp-azure-core_1.16.0")
set(_AZURE_OTEL_DIR "${_AZURE_SRC}/sdk/core/azure-core-tracing-opentelemetry")
if(EXISTS "${_AZURE_OTEL_DIR}/CMakeLists.txt")
    # Patch out the opentelemetry dependency
    file(READ "${_AZURE_OTEL_DIR}/CMakeLists.txt" _OTEL_CONTENT)
    if(_OTEL_CONTENT MATCHES "find_package.*opentelemetry")
        file(WRITE "${_AZURE_OTEL_DIR}/CMakeLists.txt" "# Disabled - opentelemetry not available\nreturn()\n")
    endif()
endif()

add_subdirectory(${_AZURE_SRC} ${CMAKE_CURRENT_BINARY_DIR}/azure EXCLUDE_FROM_ALL)
