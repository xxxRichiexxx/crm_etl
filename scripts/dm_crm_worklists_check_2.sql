INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH sq AS(
    SELECT SUM(w."РЛ на ранних этапах")
    FROM sttgaz.dm_crm_worklists w 
    WHERE w.period = '2023-10-01'
)
SELECT 
    '{{params.dm}}',
    'comparison_with_target:' || ' 402=' || (SELECT * FROM sq),
    NOW(),
    402 = (SELECT * FROM sq);