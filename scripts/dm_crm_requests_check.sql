INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH sq AS(
    SELECT SUM(Просрочено)
    FROM sttgaz.'{{params.dm}}' s
    WHERE period = '2023-10-01'
)
SELECT 
    '{{params.dm}}',
    'comparison_with_target:' || ' 89=' || (SELECT * FROM sq),
    NOW(),
    89 = (SELECT * FROM sq);