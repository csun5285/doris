LOAD LABEL ${loadLabel} (
    DATA INFILE("${hdfsFs}/broker/supplier.tbl")
    INTO TABLE supplier
    COLUMNS TERMINATED BY "|"
    (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, temp)
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
