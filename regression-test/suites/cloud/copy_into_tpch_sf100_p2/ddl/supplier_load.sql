copy into supplier
( select $1, $2, $3, $4, $5, $6, $7 from @${stageName}('${prefix}/supplier.tbl') )
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');
