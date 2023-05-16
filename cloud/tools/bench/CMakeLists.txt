add_executable(fdb_bench fdb_bench.cpp)
target_link_libraries(fdb_bench ${DORIS_LINK_LIBS})

add_executable(cloud_bench cloud_bench.cpp)
target_link_libraries(cloud_bench ${DORIS_LINK_LIBS})

install(TARGETS fdb_bench DESTINATION ${OUTPUT_DIR}/lib)
install(TARGETS cloud_bench DESTINATION ${OUTPUT_DIR}/lib)
