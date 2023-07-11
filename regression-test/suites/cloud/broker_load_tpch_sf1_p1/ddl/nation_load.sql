LOAD LABEL ${loadLabel} (
    DATA INFILE("${hdfsFs}/broker/nation.tbl")
    INTO TABLE nation
    COLUMNS TERMINATED BY "|"
    (n_nationkey, n_name, n_regionkey, n_comment, temp)
) 
with HDFS (
    "fs.defaultFS"="${hdfsFs}",
    "hadoop.username"="${hdfsUser}"
)
PROPERTIES
(
    "timeout"="1200",
    "max_filter_ratio"="0.1"
)
