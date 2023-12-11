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
    TextObrashenia long varchar,
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
    NoRabochegoLista varchar(500),
    DataSozdania varchar(300),
    Potrebnost varchar(500),
    NaimenovanieCompanii varchar(500),
    Client varchar(500),
    OtvetstvenniProdavets varchar(300),
    PervichniContakt varchar(300),
    EtapProdaz varchar(200),
    OcherednoeSobitie varchar(300),
    InitsiatorRL varchar(500),
    INNClienta varchar(200),
    period date
)
PARTITION BY (period);

DROP TABLE IF EXISTS sttgaz.stage_crm_sales;
CREATE TABLE sttgaz.stage_crm_sales(
    DataSozdaniaRL  varchar(300),
    VIN  varchar(300),
    Marka  varchar(300),
    Model  varchar(300),
    DataVidachi  varchar(300),
    NomerRL  varchar(500),
    IstochnicTrafica  varchar(300),
    NomerObrashenia INT,
    DataObrashenia  timestamp,
    KodDC int,
    Dealer varchar(500), 
    Region varchar(500),
    GorodDC varchar(300),
    RegionalniManager varchar(300),
    NomenclaturniyCode  varchar(300),
    TipClienta  varchar(300),
    Client  varchar(300),
    SubiektClienta varchar(300),
    SferaDeyatelnostiClienta varchar(300),
    INNClienta varchar(300),
    KPPClienta  varchar(300),
    TegClienta   varchar(700),
    FIOPocupatelia  varchar(300),
    ClassTS   varchar(300),
    NowiyBU   varchar(100),
    TegRL   varchar(800),
    ManagerPoProdaze   varchar(300),
    OtvetstvenniyZaClienta  varchar(300),
    TegSobitiya   varchar(300),
    TipContacta  varchar(300),
    period date
)
PARTITION BY (period);

DROP TABLE IF EXISTS sttgaz.stage_crm_stats;
CREATE TABLE sttgaz.stage_crm_stats (
    "Region" varchar(800),
    "Holding" varchar(800),
    "Dealer" varchar(800),
    "TegiClienta" varchar(800), 
    "TegiRL" varchar(800),
    "RLsoStatusomTekushiy" varchar(800),
    "RLsoStatusomSdelkaSostoyalas" varchar(800),
    "RLnaEtapePervichniyKontact" varchar(800),
    "RLnaEtapePervichniyKontact%" varchar(800),
    "RLnaEtapePotrebnosti" varchar(800),
    "RLnaEtapePotrebnosti%" varchar(800),
    "RLnaNachalnihEtapahPervichniyContact+Potrebnosti" varchar(800),
    "RLnaNachalnihEtapahPervichniyContact+Potrebnosti%" varchar(800),
    "SootvetstvieTrebovaniyam1" varchar(800),
    "RLsNenaznachennimSleduyushimSobitiem" varchar(800),
    "RLsNenaznachennimSleduyushimSobitiem%" varchar(800),
    "SootvetstvieTrebovaniyam2" varchar(800),
    period date   
)
PARTITION BY (period);
