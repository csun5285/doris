CREATE TABLE IF NOT EXISTS fulltext_t1_uk (
    a VARCHAR(200),
	b TEXT,
    INDEX a_idx (a) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'a_idx',
    INDEX b_idx (b) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'b_idx'
)
UNIQUE KEY(a)
DISTRIBUTED BY HASH(a) BUCKETS 3
PROPERTIES ( 
<<<<<<< HEAD
    "replication_num" = "1" 
=======
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
>>>>>>> doris/branch-2.0-beta
);