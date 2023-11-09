CREATE OR REPLACE VIEW sttgaz.dm_crm_requests AS
WITH
	prepared_data AS(
		SELECT
			*
			,CASE 
				WHEN r.Region iLIKE '%регион%' THEN 'РФ'
				ELSE 'СНГ'
			END 																												AS "Страна"
			,CAST(SPLIT_PART(r.Skorost , ':', 1) AS INTERVAL HOUR) + CAST(SPLIT_PART(r.Skorost , ':', 2) AS INTERVAL MINUTE)	AS "Скорость"
		FROM sttgaz.stage_crm_requests r
		WHERE 
			(r.IstochnicTrafica iLIKE '%Сайт Дистрибьютора%' OR r.IstochnicTrafica iLIKE '%Сайт дилера%')
			AND r.Dealer NOT iLIKE '%тест%'
			AND (r.Comments IS NULL OR r.Comments NOT iLIKE '%тест perx%')	
	),
	sq AS(
		SELECT
			r.period
			,r.KodDC
			,r.Dealer
			,r.Holding 
			,r.Region
			,r."Страна"																											
			,r.NomerObrashenia 
			,r."Скорость"
			,r.IstochnicTrafica 
			,r.Status
			,r.DataSozdania
			,r.DataSmeniStatusa
			,r.Comments
			,CASE
				WHEN 
					("Скорость" > INTERVAL '60 MINUTE' OR (r.Skorost = '00:00' AND r.DataSmeniStatusa IS NULL)) 
					AND r.Status <> 'Ошибочное (просрочено)'
					AND NomerObrashenia NOT IN 
                        (169682, 169690, 170332, 170339, 170341, 170369, 170370, 170403, 170418,
                        170457, 170458, 170474, 170601, 171243, 171243, 171270, 169071, 167482,
						173091, 173093, 169884)
					THEN 1			
			END 																												AS "Просрочено Да/Нет"
			,COUNT(NomerObrashenia)	OVER (PARTITION BY period, Dealer)															AS "Количество обращений у дилера в тек. месяце"
			,COUNT("Просрочено Да/Нет")	OVER (PARTITION BY period, Dealer)														AS "Количество просроченных обращений у дилера в тек. месяце"
		FROM prepared_data r
		WHERE  r."Страна" = 'РФ'
	)
SELECT 
	period													AS "Период"
	,KodDC													AS "Код ДЦ"
	,Dealer													AS "Дилер"
	,Holding 												AS "Холдинг"
	,Region													AS "Регион"
	,"Страна"																											
	,NomerObrashenia 										AS "Номер обращения"
	,"Скорость"
	,IstochnicTrafica 										AS "Источник трафика"
	,Status													AS "Статус"
	,DataSozdania											AS "Дата создания"
	,DataSmeniStatusa										AS "Дата смены статуса"
	,Comments												AS "Комментарии"
	,CASE 
		WHEN "Просрочено Да/Нет" = 1 AND "Количество просроченных обращений у дилера в тек. месяце" = 1 AND "Скорость" < INTERVAL '78 MINUTE'
			THEN 0
		ELSE "Просрочено Да/Нет"
	END																															AS "Просрочено"
	,"Количество обращений у дилера в тек. месяце"
	,SUM("Просрочено") OVER (PARTITION BY period, Dealer)																		AS "Количество просроченных обращений у дилера в тек. месяце"
FROM sq;

GRANT SELECT ON TABLE sttgaz.dm_crm_requests TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_crm_requests IS 'Витрина с обращениями из CRM';