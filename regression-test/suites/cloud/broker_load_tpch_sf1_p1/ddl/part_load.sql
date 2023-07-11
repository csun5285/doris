LOAD LABEL ${loadLabel} (
    DATA INFILE("${hdfsFs}/broker/part.tbl")
    INTO TABLE part
    COLUMNS TERMINATED BY "|"
    (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp)
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
