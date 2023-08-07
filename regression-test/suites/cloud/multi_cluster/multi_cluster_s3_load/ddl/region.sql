CREATE TABLE IF NOT EXISTS region (
  R_REGIONKEY  INTEGER NOT NULL,
  R_NAME       CHAR(25) NOT NULL,
  R_COMMENT    VARCHAR(152)
)
DUPLICATE KEY(R_REGIONKEY, R_NAME)
DISTRIBUTED BY HASH(R_REGIONKEY) BUCKETS 1


