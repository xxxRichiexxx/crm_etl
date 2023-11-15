
CREATE OR REPLACE VIEW sttgaz.dm_crm_sales AS
SELECT
	period
	,KodDC
	,Dealer
	,Region
	,GorodDC
	,IMPLODE(VIN)											AS "VINы"
	,COUNT(VIN)												AS "Продано"
	,COUNT(CASE WHEN NowiyBU = 'Новый' THEN 1 END)			AS "Продано новых"
	,COUNT(CASE WHEN NowiyBU = 'С пробегом' THEN 1 END)		AS "Продано с пробегом"
FROM sttgaz.stage_crm_sales s
GROUP BY
	period
	,KodDC
	,Dealer
	,Region
	,GorodDC;

GRANT SELECT ON TABLE sttgaz.dm_crm_sales TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_crm_sales IS 'Витрина с статистикой по продажам из CRM';