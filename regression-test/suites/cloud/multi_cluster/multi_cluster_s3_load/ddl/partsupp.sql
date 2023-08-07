CREATE TABLE IF NOT EXISTS partsupp (
  PS_PARTKEY     INTEGER NOT NULL,
  PS_SUPPKEY     INTEGER NOT NULL,
  PS_AVAILQTY    INTEGER NOT NULL,
  PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
  PS_COMMENT     VARCHAR(199) NOT NULL 
)
DUPLICATE KEY(PS_PARTKEY, PS_SUPPKEY)
DISTRIBUTED BY HASH(PS_PARTKEY) BUCKETS 32


