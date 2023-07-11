LOAD LABEL ${loadLabel} (
    DATA INFILE("${hdfsFs}/broker/region.tbl")
    INTO TABLE region
    COLUMNS TERMINATED BY "|"
    (r_regionkey, r_name, r_comment, temp)
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
