INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH sq AS(
    SELECT COUNT(*)
    FROM sttgaz.stage_crm_requests r
    WHERE r.period ='2023-02-01'
)
SELECT 
    '{{params.dm}}',
    'comparison_with_target 2:' || ' 3438=' || (SELECT * FROM sq),
    NOW(),
    3438 = (SELECT * FROM sq);