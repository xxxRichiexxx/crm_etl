CREATE OR REPLACE VIEW sttgaz.dm_crm_stats AS
SELECT 
    s."Region"													AS "Регион",
    s."Holding"													AS "Холдинг",
    r.KodDC														AS "Код ДЦ", 
    s."Dealer"													AS "Дилерский центр",
    s."TegiClienta"												AS "Тэги Клиента", 
    s."TegiRL"													AS "Тэги РЛ",
    s."IstochnicTrafica"										AS "Источник трафика",
    s."RLsoStatusomTekushiy"::int								AS "Кол-во РЛ в отчетном периоде со статусом Текущий",
    s."RLsoStatusomSdelkaSostoyalas"::int						AS "Кол-во РЛ в отчетном периоде со Статусом Сделка состоялась",
    s."RLnaEtapePervichniyKontact"::int							AS "РЛ на этапе Первичный контакт",
    s."RLnaEtapePervichniyKontactPC"							AS "РЛ на этапе Первичный контакт %",
    s."RLnaEtapePotrebnosti"::int								AS "РЛ на этапе Потребности",
    s."RLnaEtapePotrebnostiPC"									AS "РЛ на этапе Потребности %",
    s."RLnaNachalnihEtapahPervichniyContactPotrebnosti"::int	AS "РЛ на начальных этапах Первичный контакт+Потребности",
    s."RLnaNachalnihEtapahPervichniyContactPotrebnostiPC"		AS "РЛ на этапах Первичный контакт+Потребности %",
    s."SootvetstvieTrebovaniyam1"::int							AS "Соответствие требованиям 0/20 баллов 1",
    s."RLsNenaznachennimSleduyushimSobitiem"::int				AS "РЛ с не назначенным следующим событием (отсутствует событие)",
    s."RLsNenaznachennimSleduyushimSobitiemPC"					AS "РЛ с не назначенным следующим событием (отсутствует событие) %",
    s."SootvetstvieTrebovaniyam2"::int							AS "Соответствие требованиям 0/20 баллов 2",
    s.period 													AS "Период"
FROM sttgaz.stage_crm_stats s
LEFT JOIN (SELECT DISTINCT KodDC, Dealer FROM sttgaz.stage_crm_requests) r
	ON s.Dealer = r.Dealer; 

GRANT SELECT ON TABLE sttgaz.dm_crm_stats TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_crm_stats IS 'Витрина с статистикой работы в CRM';
