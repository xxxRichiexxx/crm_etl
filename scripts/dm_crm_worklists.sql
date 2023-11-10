CREATE OR REPLACE VIEW sttgaz.dm_crm_worklists AS 
WITH
	data AS (
		SELECT
		    r.Skorost,
		    r.Region,
		    r.Holding,
		    r.NomerObrashenia,
		    r.KodDC,
		    r.Dealer,
		    r.DataSozdania,
		    r.DataSmeniStatusa,
		    r.Status,
		    r.Comments,
		    r.Prichina,
		    r.IstochnicTrafica,
		    r.TipZaiavki,
		    r.Domen,
		    r.IPAdresClienta,
		    r.NomerRL,
		    r.ModelInteresuiushegoTS,
		    r.ClientID,
		    r.Client,
		    r.Phone,
		    r.Email,
		    r.TextObrashenia,
		    r.SoglasieNaObrabotcuPD,
		    r.SoglasieNaPoluchenieInformacii,
		    r.ID,
		    r.ObrashenieZakrito,
		    r.TegiClienta,
		    r.StatusRL							AS request_StatusRL,
		    r.EtapProdaz						AS request_EtapProdaz,
		    r.OblastClienta,
		    r.ObsheeVremiaZvonca,
		    r.StatusSviazannogoSobitiya,
		    r.TypeOcherednogoSobitiya,
		    r.DataOcherednogoSobitiya,
		    r.OtvetstvenniyZaRL,
		    w.period,
		    w.NoRabochegoLista,
		    w.DataSozdania,
		    w.Potrebnost,
		    w.NaimenovanieCompanii,
		    w.Client,
		    w.OtvetstvenniProdavets,
		    w.PervichniContakt,
		    w.EtapProdaz						AS worklist_EtapProdaz,
		    w.OcherednoeSobitie,
		    w.InitsiatorRL,
		    w.INNClienta  
		FROM sttgaz.stage_crm_requests r 
		INNER JOIN sttgaz.stage_crm_worklists w 
			ON r.NomerRL::varchar  = w.NoRabochegoLista::varchar
	),
	RL_vsego AS(
		SELECT
			KodDC
			,NaimenovanieCompanii
			,period
			,COUNT(DISTINCT NomerRL)
		FROM data
		WHERE 
			request_StatusRL IN ('Текущий', 'Сделка состоялась')
		GROUP BY KodDC, NaimenovanieCompanii, period	
	),
	RL_na_nachalnih_etapah AS( 
		SELECT
			KodDC
			,NaimenovanieCompanii
			,period
			,COUNT(DISTINCT NomerRL)
		FROM data
		WHERE 
			request_StatusRL IN ('Текущий', 'Сделка состоялась')
			AND
			worklist_EtapProdaz IN ('Первичный контакт', 'Потребности')
		GROUP BY KodDC, NaimenovanieCompanii, period
	)
	SELECT
		RL_vsego.period
		,RL_vsego.KodDC
		,RL_vsego.NaimenovanieCompanii
		,RL_vsego.COUNT 									AS "Всего РЛ"
		,COALESCE(RL_na_nachalnih_etapah.COUNT, 0)			AS "РЛ на ранних этапах"
	FROM RL_vsego
	LEFT JOIN RL_na_nachalnih_etapah
		ON RL_vsego.period = RL_na_nachalnih_etapah.period
		AND RL_vsego.KodDC = RL_na_nachalnih_etapah.KodDC;

GRANT SELECT ON TABLE sttgaz.dm_crm_worklists TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_crm_worklists IS 'Витрина с статистикой по рабочим листам из CRM';

