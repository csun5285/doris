LOAD LABEL ${loadLabel} (
    DATA INFILE("${hdfsFs}/broker/customer.tbl")
    INTO TABLE customer
    COLUMNS TERMINATED BY "|"
    (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
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
