LOAD LABEL ${loadLabel} (
    DATA INFILE("${hdfsFs}/broker/partsupp.tbl.*")
    INTO TABLE partsupp
    COLUMNS TERMINATED BY "|"
    (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, temp)
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
