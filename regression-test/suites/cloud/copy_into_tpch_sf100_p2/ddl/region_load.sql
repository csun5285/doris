copy into region from 
( select $1, $2, $3 from @${stageName}('${prefix}/region.tbl') )
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');
