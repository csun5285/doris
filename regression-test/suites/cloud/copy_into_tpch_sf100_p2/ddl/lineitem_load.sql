copy into lineitem from
( select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16 from @${stageName}('${prefix}/lineitem.tbl.*') )
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');