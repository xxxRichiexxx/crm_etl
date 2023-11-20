INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH sq AS(
    SELECT SUM(Продано)
    FROM sttgaz.dm_crm_sales s 
    WHERE s.period = '2023-02-01'
)
SELECT 
    '{{params.dm}}',
    'comparison_with_target:' || ' 1813=?',
    NOW(),
    1813 = (SELECT * FROM sq);