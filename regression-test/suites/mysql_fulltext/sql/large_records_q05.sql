
<<<<<<< HEAD
=======

>>>>>>> 2.0.0-rc01
SELECT COUNT(*) FROM large_records_t3_uk;

SELECT COUNT(*) from large_records_t3_uk WHERE b like '%samerowword%';

SELECT COUNT(*) FROM large_records_t3_uk WHERE a MATCH_ANY 'samerowword' OR b MATCH_ANY 'samerowword';

SELECT COUNT(*) from large_records_t3_uk 
    WHERE (a MATCH_ANY 'samerowword' OR b MATCH_ANY 'samerowword')
    AND (a MATCH_ANY 'samerowword' OR b MATCH_ANY 'samerowword');

SELECT COUNT(*) from large_records_t3_uk 
    WHERE (a MATCH_ANY 'samerowword' OR b MATCH_ANY 'samerowword')
    AND NOT (a MATCH_ANY 'row45col2word49' OR b MATCH_ANY 'row45col2word49');

SELECT COUNT(*) from large_records_t3_uk 
<<<<<<< HEAD
    WHERE (a MATCH_ANY 'sameroww' OR b MATCH_ANY 'sameroww');
=======
    WHERE (a MATCH_ANY 'sameroww' OR b MATCH_ANY 'sameroww');
>>>>>>> 2.0.0-rc01
