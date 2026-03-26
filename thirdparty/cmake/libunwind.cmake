# libunwind — pure CMake build (no autoconf)
# Build for Linux x86_64 only. The L* files are "local" variants, G* are "generic".
# Both are needed: libunwind.a = local unwind, libunwind-x86_64.a = remote.

set(UNW_SRC ${TP_SOURCE_DIR}/libunwind-1.6.2)
set(UNW_CONFIG_H "${UNW_SRC}/include/config.h")
add_custom_command(
    OUTPUT ${UNW_CONFIG_H}
    COMMAND env CFLAGS=-fPIC CXXFLAGS=-fPIC ./configure --with-pic --disable-shared
    WORKING_DIRECTORY ${UNW_SRC}
    COMMENT "Configuring libunwind..."
)
add_custom_target(libunwind_config_headers DEPENDS ${UNW_CONFIG_H})

# --- Source files for Linux x86_64 ---
# mi/ (machine-independent)
set(UNW_MI_SRCS
    ${UNW_SRC}/src/mi/init.c
    ${UNW_SRC}/src/mi/flush_cache.c
    ${UNW_SRC}/src/mi/mempool.c
    ${UNW_SRC}/src/mi/strerror.c
    ${UNW_SRC}/src/mi/backtrace.c
    ${UNW_SRC}/src/mi/dyn-cancel.c
    ${UNW_SRC}/src/mi/dyn-info-list.c
    ${UNW_SRC}/src/mi/dyn-register.c
    ${UNW_SRC}/src/mi/_ReadSLEB.c
    ${UNW_SRC}/src/mi/_ReadULEB.c
    ${UNW_SRC}/src/mi/Ldestroy_addr_space.c
    ${UNW_SRC}/src/mi/Lfind_dynamic_proc_info.c
    ${UNW_SRC}/src/mi/Lget_accessors.c
    ${UNW_SRC}/src/mi/Lget_fpreg.c
    ${UNW_SRC}/src/mi/Lget_proc_info_by_ip.c
    ${UNW_SRC}/src/mi/Lget_proc_name.c
    ${UNW_SRC}/src/mi/Lget_reg.c
    ${UNW_SRC}/src/mi/Lput_dynamic_unwind_info.c
    ${UNW_SRC}/src/mi/Lset_cache_size.c
    ${UNW_SRC}/src/mi/Lset_caching_policy.c
    ${UNW_SRC}/src/mi/Lset_fpreg.c
    ${UNW_SRC}/src/mi/Lset_reg.c
    ${UNW_SRC}/src/mi/Ldyn-extract.c
    ${UNW_SRC}/src/mi/Gdestroy_addr_space.c
    ${UNW_SRC}/src/mi/Gdyn-extract.c
    ${UNW_SRC}/src/mi/Gdyn-remote.c
    ${UNW_SRC}/src/mi/Gfind_dynamic_proc_info.c
    ${UNW_SRC}/src/mi/Gget_accessors.c
    ${UNW_SRC}/src/mi/Gget_fpreg.c
    ${UNW_SRC}/src/mi/Gget_proc_info_by_ip.c
    ${UNW_SRC}/src/mi/Gget_proc_name.c
    ${UNW_SRC}/src/mi/Gget_reg.c
    ${UNW_SRC}/src/mi/Gput_dynamic_unwind_info.c
    ${UNW_SRC}/src/mi/Gset_cache_size.c
    ${UNW_SRC}/src/mi/Gset_caching_policy.c
    ${UNW_SRC}/src/mi/Gset_fpreg.c
    ${UNW_SRC}/src/mi/Gset_reg.c
)

# dwarf/
set(UNW_DWARF_SRCS
    ${UNW_SRC}/src/dwarf/global.c
    ${UNW_SRC}/src/dwarf/Lfde.c
    ${UNW_SRC}/src/dwarf/Lexpr.c
    ${UNW_SRC}/src/dwarf/Lfind_proc_info-lsb.c
    ${UNW_SRC}/src/dwarf/Lfind_unwind_table.c
    ${UNW_SRC}/src/dwarf/Lparser.c
    ${UNW_SRC}/src/dwarf/Lpe.c
    ${UNW_SRC}/src/dwarf/Gfde.c
    ${UNW_SRC}/src/dwarf/Gexpr.c
    ${UNW_SRC}/src/dwarf/Gfind_proc_info-lsb.c
    ${UNW_SRC}/src/dwarf/Gfind_unwind_table.c
    ${UNW_SRC}/src/dwarf/Gparser.c
    ${UNW_SRC}/src/dwarf/Gpe.c
)

# x86_64/ (platform-specific)
set(UNW_X86_64_SRCS
    ${UNW_SRC}/src/x86_64/is_fpreg.c
    ${UNW_SRC}/src/x86_64/regname.c
    ${UNW_SRC}/src/x86_64/getcontext.S
    ${UNW_SRC}/src/x86_64/setcontext.S
    ${UNW_SRC}/src/x86_64/longjmp.S
    ${UNW_SRC}/src/x86_64/siglongjmp.S
    ${UNW_SRC}/src/x86_64/Lcreate_addr_space.c
    ${UNW_SRC}/src/x86_64/Lget_save_loc.c
    ${UNW_SRC}/src/x86_64/Lglobal.c
    ${UNW_SRC}/src/x86_64/Linit.c
    ${UNW_SRC}/src/x86_64/Linit_local.c
    ${UNW_SRC}/src/x86_64/Linit_remote.c
    ${UNW_SRC}/src/x86_64/Lget_proc_info.c
    ${UNW_SRC}/src/x86_64/Lregs.c
    ${UNW_SRC}/src/x86_64/Lresume.c
    ${UNW_SRC}/src/x86_64/Lstash_frame.c
    ${UNW_SRC}/src/x86_64/Lstep.c
    ${UNW_SRC}/src/x86_64/Ltrace.c
    ${UNW_SRC}/src/x86_64/Lapply_reg_state.c
    ${UNW_SRC}/src/x86_64/Lreg_states_iterate.c
    ${UNW_SRC}/src/x86_64/Los-linux.c
    ${UNW_SRC}/src/x86_64/Gcreate_addr_space.c
    ${UNW_SRC}/src/x86_64/Gget_save_loc.c
    ${UNW_SRC}/src/x86_64/Gglobal.c
    ${UNW_SRC}/src/x86_64/Ginit.c
    ${UNW_SRC}/src/x86_64/Ginit_local.c
    ${UNW_SRC}/src/x86_64/Ginit_remote.c
    ${UNW_SRC}/src/x86_64/Gget_proc_info.c
    ${UNW_SRC}/src/x86_64/Gregs.c
    ${UNW_SRC}/src/x86_64/Gresume.c
    ${UNW_SRC}/src/x86_64/Gstash_frame.c
    ${UNW_SRC}/src/x86_64/Gstep.c
    ${UNW_SRC}/src/x86_64/Gtrace.c
    ${UNW_SRC}/src/x86_64/Gapply_reg_state.c
    ${UNW_SRC}/src/x86_64/Greg_states_iterate.c
    ${UNW_SRC}/src/x86_64/Gos-linux.c
)

# os-linux + elf64
set(UNW_OS_SRCS
    ${UNW_SRC}/src/os-linux.c
    ${UNW_SRC}/src/elf64.c
    ${UNW_SRC}/src/dl-iterate-phdr.c
)

# Force CMake to compile .S files using the C compiler to avoid ASM language setup errors
set_source_files_properties(${UNW_X86_64_SRCS} PROPERTIES LANGUAGE C)

add_library(_libunwind STATIC
    ${UNW_MI_SRCS}
    ${UNW_DWARF_SRCS}
    ${UNW_X86_64_SRCS}
    ${UNW_OS_SRCS}
)

target_include_directories(_libunwind
    PUBLIC  ${UNW_SRC}/include
    PRIVATE ${UNW_SRC}/include/tdep-x86_64
    PRIVATE ${UNW_SRC}/src
    PRIVATE ${UNW_SRC}/src/x86_64
)

target_compile_definitions(_libunwind PRIVATE
    HAVE_CONFIG_H
    _GNU_SOURCE
)

target_compile_options(_libunwind PRIVATE -fPIC -w)
target_link_libraries(_libunwind PRIVATE pthread dl)
add_dependencies(_libunwind libunwind_config_headers)
add_library(libunwind ALIAS _libunwind)
