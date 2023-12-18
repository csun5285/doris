DROP TABLE IF EXISTS customer_ttl;

CREATE TABLE IF NOT EXISTS customer_ttl (
  C_CUSTKEY     INTEGER NOT NULL,
  C_NAME        VARCHAR(25) NOT NULL,
  C_ADDRESS     VARCHAR(40) NOT NULL,
  C_NATIONKEY   INTEGER NOT NULL,
  C_PHONE       CHAR(15) NOT NULL,
  C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
  C_MKTSEGMENT  CHAR(10) NOT NULL,
  C_COMMENT     VARCHAR(117) NOT NULL
)
DUPLICATE KEY(C_CUSTKEY, C_NAME)
DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32
PROPERTIES(
    "file_cache_ttl_seconds"="300"
);

show create table customer_ttl;

alter table customer_ttl set ("file_cache_ttl_seconds"="0");

show create table customer_ttl;
