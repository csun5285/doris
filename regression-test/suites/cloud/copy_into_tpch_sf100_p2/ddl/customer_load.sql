copy into customer from 
( select $1, $2, $3, $4, $5, $6, $7, $8 from @${stageName}('${prefix}/customer.tbl') )
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');