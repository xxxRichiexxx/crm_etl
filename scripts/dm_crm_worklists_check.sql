INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH sq AS(
    SELECT SUM(w."Всего РЛ")
    FROM sttgaz.'{{params.dm}}' w 
    WHERE w.period = '2023-10-01'
)
SELECT 
    '{{params.dm}}',
    'comparison_with_target:' || ' 1238=?',
    NOW(),
    1238 = (SELECT * FROM sq);