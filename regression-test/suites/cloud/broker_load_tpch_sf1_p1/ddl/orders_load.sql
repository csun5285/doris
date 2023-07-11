LOAD LABEL ${loadLabel} (
    DATA INFILE("${hdfsFs}/broker/orders.tbl.*")
    INTO TABLE orders
    COLUMNS TERMINATED BY "|"
    (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp)
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
