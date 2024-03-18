copy into partsupp from
( select $1, $2, $3, $4, $5 from @${stageName}('${prefix}/partsupp.tbl*') )
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');