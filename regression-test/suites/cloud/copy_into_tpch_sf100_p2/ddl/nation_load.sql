copy into nation from 
( select $1, $2, $3, $4 from @${stageName}('${prefix}/nation.tbl') )
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');