INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH sq AS(
    SELECT SUM(w."РЛ на начальных этапах Первичный контакт+Потребности")
    FROM sttgaz.'{{params.dm}}' w 
    WHERE w."Период" = '2023-10-01'
)
SELECT 
    '{{params.dm}}',
    'comparison_with_target:' || ' 1238=?',
    NOW(),
    277 = (SELECT * FROM sq);