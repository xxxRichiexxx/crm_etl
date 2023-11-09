DROP TABLE IF EXISTS sttgaz.stage_crm_requests;
CREATE TABLE sttgaz.stage_crm_requests
(
    Skorost varchar(100),
    Region varchar(500),
    Holding varchar(500),
    NomerObrashenia int,
    KodDC int,
    Dealer varchar(500),
    DataSozdania timestamp,
    DataSmeniStatusa timestamp,
    Status varchar(100),
    Comments varchar(3000),
    Prichina varchar(500),
    IstochnicTrafica varchar(100),
    TipZaiavki varchar(500),
    Domen varchar(500),
    IPAdresClienta varchar(500),
    NomerRL int,
    ModelInteresuiushegoTS varchar(500),
    ClientID varchar(500),
    Client varchar(500),
    Phone varchar(500),
    Email varchar(500),
    TextObrashenia varchar(50000),
    SoglasieNaObrabotcuPD varchar(500),
    SoglasieNaPoluchenieInformacii varchar(500),
    ID varchar(500),
    ObrashenieZakrito varchar(500),
    TegiClienta varchar(500),
    StatusRL varchar(500),
    EtapProdaz varchar(500),
    OblastClienta varchar(500),
    ObsheeVremiaZvonca varchar(500),
    StatusSviazannogoSobitiya varchar(500),
    TypeOcherednogoSobitiya varchar(500),
    DataOcherednogoSobitiya varchar(500),
    OtvetstvenniyZaRL varchar(500),
    period date
)
PARTITION BY (stage_crm_requests.period);


DROP TABLE IF EXISTS sttgaz.stage_crm_worklists;
CREATE TABLE sttgaz.stage_crm_worklists
(
    NoRabochegoLista int,
    DataSozdania timestamp,
    Potrebnost varchar(300),
    NaimenovanieCompanii varchar(500),
    Client varchar(500),
    OtvetstvenniProdavets varchar(300),
    PervichniContakt varchar(300),
    EtapProdaz varchar(200),
    OcherednoeSobitie timestamp,
    InitsiatorRL varchar(500),
    INNClienta varchar(200),
    period date
)
PARTITION BY (period);