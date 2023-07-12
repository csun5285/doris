CREATE TABLE IF NOT EXISTS join_t1_uk (
    venue_id int(11) default null,
    venue_text varchar(255) default null,
    dt datetime default null
)
UNIQUE KEY(venue_id)
DISTRIBUTED BY HASH(venue_id) BUCKETS 3
PROPERTIES ( 
<<<<<<< HEAD
    "replication_num" = "1" 
=======
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
>>>>>>> doris/branch-2.0-beta
);

