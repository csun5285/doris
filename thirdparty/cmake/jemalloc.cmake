# jemalloc — pure CMake build (no autoconf)
# Build jemalloc with Doris-specific prefix (je) and install suffix (_doris).
# Key: generate jemalloc_internal_defs.h from template.

set(JE_SRC ${TP_SOURCE_DIR}/jemalloc-5.3.0)
set(JE_CONFIG_DIR ${CMAKE_CURRENT_BINARY_DIR}/jemalloc_config)
file(MAKE_DIRECTORY ${JE_CONFIG_DIR}/jemalloc)
file(MAKE_DIRECTORY ${JE_CONFIG_DIR}/jemalloc/internal)

# Detect page size at configure time (needed for jemalloc)
execute_process(COMMAND getconf PAGESIZE
    OUTPUT_VARIABLE JE_PAGE_SIZE OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT JE_PAGE_SIZE)
    set(JE_PAGE_SIZE 4096)
endif()
# LG_PAGE = log2(PAGE_SIZE)
math(EXPR JE_LG_PAGE "0")
set(_ps ${JE_PAGE_SIZE})
while(_ps GREATER 1)
    math(EXPR _ps "${_ps} / 2")
    math(EXPR JE_LG_PAGE "${JE_LG_PAGE} + 1")
endwhile()

# Run jemalloc configure to generate necessary configured headers during build phase
set(JE_PREAMBLE "\${JE_SRC}/include/jemalloc/internal/jemalloc_preamble.h")
add_custom_command(
    OUTPUT \${JE_PREAMBLE}
    COMMAND ./configure --with-jemalloc-prefix=je_ --with-lg-page=\${JE_LG_PAGE} --disable-shared
    WORKING_DIRECTORY \${JE_SRC}
    COMMENT "Running jemalloc configure to generate headers..."
)
add_custom_target(jemalloc_config_headers DEPENDS \${JE_PREAMBLE})

# Collect jemalloc source files
file(GLOB JE_SRCS "${JE_SRC}/src/*.c")

# Remove OS-specific files
list(FILTER JE_SRCS EXCLUDE REGEX "zone\\.c")

add_library(_jemalloc STATIC ${JE_SRCS})

target_include_directories(_jemalloc
    PRIVATE ${JE_SRC}/include
    PRIVATE ${JE_SRC}/include/jemalloc/internal
    PUBLIC  ${JE_SRC}/include
)

add_dependencies(_jemalloc jemalloc_config_headers)

target_compile_definitions(_jemalloc PRIVATE
    JEMALLOC_NO_PRIVATE_NAMESPACE
    _GNU_SOURCE
    _REENTRANT
)

target_compile_options(_jemalloc PRIVATE -fPIC -w -funroll-loops)
target_link_libraries(_jemalloc PRIVATE pthread dl m)

add_library(jemalloc ALIAS _jemalloc)
