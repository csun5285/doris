# hadoop_hdfs_headers: copy hadoop_hdfs_3_4 headers from old/include
set(HDFS_OLD_DIR "${TP_SOURCE_DIR}/../old/include")
set(HDFS_NESTED_DIR "${CMAKE_CURRENT_BINARY_DIR}/hadoop_hdfs_headers/include")
file(MAKE_DIRECTORY ${HDFS_NESTED_DIR})

# Copy the hadoop_hdfs_3_4 directory
if(EXISTS "${HDFS_OLD_DIR}/hadoop_hdfs_3_4")
    file(COPY "${HDFS_OLD_DIR}/hadoop_hdfs_3_4" DESTINATION ${HDFS_NESTED_DIR})
endif()
# Copy the hadoop_hdfs directory (without version suffix)
if(EXISTS "${HDFS_OLD_DIR}/hadoop_hdfs")
    file(COPY "${HDFS_OLD_DIR}/hadoop_hdfs" DESTINATION ${HDFS_NESTED_DIR})
endif()

add_library(hadoop_hdfs_headers INTERFACE)
target_include_directories(hadoop_hdfs_headers SYSTEM INTERFACE ${HDFS_NESTED_DIR})
