copy into lineitem
from @${stageName}('${prefix}/lineitem.tbl.*')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');