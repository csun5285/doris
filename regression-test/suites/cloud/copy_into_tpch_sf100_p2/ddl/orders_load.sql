copy into orders from
( select $1, $2, $3, $4, $5, $6, $7, $8, $9 from @${stageName}('${prefix}/orders.tbl*'))
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');