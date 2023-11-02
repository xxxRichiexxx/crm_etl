
CREATE OR REPLACE VIEW sttgaz.dm_crm_slow_requests AS
WITH
	sq AS(
		SELECT
			r.period
			,r.KodDC
			,r.Dealer
			,r.Holding 
			,r.Region
			,CASE 
				WHEN r.Region iLIKE '%регион%' THEN 'РФ'
				ELSE 'СНГ'
			END 																												AS "Страна"
			,r.NomerObrashenia 
			,CAST(SPLIT_PART(r.Skorost , ':', 1) AS INTERVAL HOUR) + CAST(SPLIT_PART(r.Skorost , ':', 2) AS INTERVAL MINUTE)	AS "Skorost"
			,COUNT(NomerObrashenia)	OVER (PARTITION BY period, Dealer)															AS "Количество просроченных дилером заявок"
			,r.IstochnicTrafica 
			,r.Status
			,r.DataSozdania
			,r.DataSmeniStatusa
			,r.Comments 
		FROM sttgaz.stage_crm_requests r
		WHERE 
			(CAST(SPLIT_PART(r.Skorost , ':', 1) AS INTERVAL HOUR) + CAST(SPLIT_PART(r.Skorost , ':', 2) AS INTERVAL MINUTE) > INTERVAL '60 MINUTE' OR (r.Skorost = '00:00' AND r.DataSmeniStatusa IS NULL))
			AND (r.IstochnicTrafica iLIKE '%Сайт Дистрибьютора%' OR r.IstochnicTrafica iLIKE '%Сайт дилера%')
			AND r.Dealer NOT iLIKE '%тест%'
			AND (r.Comments IS NULL OR r.Comments NOT iLIKE '%тест perx%')
			AND r.Status <> 'Ошибочное (просрочено)'
			AND NomerObrashenia NOT IN (169682, 169690, 170332, 170339, 170341, 170369, 170370, 170403, 170418, 170457, 170458, 170474, 170601, 171243, 171243, 171270)
	)
SELECT 
	*
FROM sq
WHERE sq."Страна" = 'РФ'
	AND  NOT ("Количество просроченных дилером заявок" = 1 AND Skorost < INTERVAL '78 MINUTE' );

GRANT SELECT ON TABLE sttgaz.dm_crm_slow_requests TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_crm_slow_requests IS 'Витрина с просроченными обращениями из CRM';