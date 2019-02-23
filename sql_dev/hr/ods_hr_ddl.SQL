---------------------------------------- DC 信息 ----------------------------------
drop table if exists ods_hr.jt_dcinfo;

CREATE TABLE ods_hr.jt_dcinfo (
  shopid     String,
  shopname   String,
  propertype String,
  dctype     String,
  square     Nullable(Decimal (18, 2)),
  bu_q       Nullable(String),
  thoughput  Nullable(Decimal (18, 2)),
  storage_g  Nullable(Decimal (18, 2)),
  storage_b  Nullable(Decimal (18, 2)),
  remarks    Nullable(String),
  bu_z       Nullable(String),
  buid_q     Decimal (18, 2),
  buid_z     Decimal (18, 2),
  flag       Decimal (18, 2)
)
  ENGINE = Log;
  
  

---------------------------------------- 区域 信息 ----------------------------------
drop table if exists ods_hr.jv_buinfo;

CREATE TABLE ods_hr.jv_buinfo (
  buid             String,
  buname           String,
  buflag           Nullable(UInt32),
  rptbuid          Nullable(String),
  shortbuid        Nullable(String),
  buprefix         Nullable(String),
  itsysalias       Nullable(String),
  sysid            Nullable(String),
  retailformgrpid  Nullable(String),
  categorytreeid   Nullable(UInt32),
  costdiscflag     Nullable(UInt32),
  shopexttabname   Nullable(String),
  venderexttabname Nullable(String),
  goodsexttabname  Nullable(String),
  headbuid         Nullable(String),
  status           Nullable(UInt32),
  note             Nullable(String),
  ismorelevel      Nullable(UInt32),
  isjv             Nullable(UInt32)
)
  ENGINE = Log;
  

---------------------------------------- 供应商 信息 ----------------------------------
CREATE TABLE ods_hr.jv_vender (
  sysvenderid        String,
  buid               String,
  vendercode         String,
  venderid           String,
  searchcode         Nullable(String),
  vendername         String,
  venderlname        Nullable(String),
  venderename        Nullable(String),
  venderlename       Nullable(String),
  taxpayingid        String,
  customertypeid     String,
  insidevendertype   String,
  retailtype         Nullable(String),
  goodsvendertype    String,
  venderpurtype      String,
  origintype         String,
  vendercontractflag String,
  localflag          String,
  payshoplevel       String,
  payshopflag        String,
  payshopid          Nullable(String),
  permitno           Nullable(String),
  identitycard       Nullable(String),
  daypayflag         String,
  primaryflag        String,
  paymode            String,
  address            Nullable(String),
  zipno              Nullable(String),
  telno              Nullable(String),
  faxno              Nullable(String),
  www                Nullable(String),
  email              Nullable(String),
  cman               Nullable(String),
  cmanphone          Nullable(String),
  mobilephone        Nullable(String),
  bankname           Nullable(String),
  bankaccount        Nullable(String),
  bankaccname        Nullable(String),
  wj                 Nullable(String),
  zj                 Nullable(String),
  bizscope           Nullable(String),
  bizlevel           Nullable(String),
  chairman           Nullable(String),
  zoneid             Nullable(String),
  taxlevel           Nullable(String),
  invadd             Nullable(String),
  invhead            Nullable(String),
  authcapital        Nullable(String),
  enterprisekind     Nullable(String),
  orderday           Nullable(String),
  inbrand            Nullable(String),
  shipmentsto        Nullable(String),
  deliverymethods    Nullable(String),
  ordermode          Nullable(String),
  indate             Nullable(String),
  inbranch           Nullable(String),
  invendee           Nullable(String),
  endvendee          Nullable(String),
  endreason          Nullable(String),
  enddate            Nullable(String),
  changereason       Nullable(String),
  contracttype       Nullable(String),
  rinvoiceflag       Nullable(String),
  prepayflag         Nullable(String),
  defavendercode     Nullable(String),
  venderstatus       Nullable(String),
  vmanagelevel       Nullable(String),
  paytypeid          Nullable(String),
  categoryid         Nullable(String),
  masterbrandid      Nullable(String),
  venderruntype      Nullable(String),
  vendertype         Nullable(String),
  venderflag         Nullable(String),
  headvendercode     Nullable(String),
  regionid           Nullable(String),
  venderform         Nullable(String),
  inoutside          Nullable(String),
  quotecurrency      Nullable(String),
  settlementcurrency Nullable(String),
  catetorydepart     Nullable(String),
  mallflag           Nullable(String),
  venderlevel        Nullable(String),
  vendercardid       Nullable(String),
  taxpayerid         Nullable(String),
  zccurrency         Nullable(String),
  isotherpayshop     Nullable(String),
  financetype        Nullable(String),
  lshopid            Nullable(String),
  runbuidstr         Nullable(String),
  stockingdays       Nullable(String),
  ordercosting       Nullable(String),
  deliverypolicy     Nullable(String),
  headbuid           Nullable(String)
)
  ENGINE = Log;  
  
  
  
---------------------------------------- 全国 店铺 信息 ----------------------------------
DROP TABLE ods_hr.jv_shop;

CREATE TABLE ods_hr.jv_shop (
  sysshopid            String,
  shopcode             String,
  shopid               String,
  shopname             Nullable(String),
  shoplname            Nullable(String),
  shopename            Nullable(String),
  shopelname           Nullable(String),
  headshopid           Nullable(String),
  buid                 Nullable(String),
  itshoptype           Nullable(UInt16),
  dcshoptype           Nullable(String),
  shopstatus           Nullable(UInt16),
  shopclass            Nullable(UInt16),
  portclass            Nullable(UInt16),
  sizeclass            Nullable(UInt16),
  workingfloor         Nullable(UInt16),
  retailformgrpid      Nullable(UInt16),
  shopformid           Nullable(UInt16),
  commercecircleid     Nullable(UInt16),
  regionid             Nullable(UInt16),
  retailbrandid        Nullable(String),
  parrentshopid        Nullable(String),
  workforce            Nullable(UInt16),
  coveredared          Nullable(Decimal (18, 3)),
  businessarea         Nullable(Decimal (18, 3)),
  lenarea              Nullable(Decimal (18, 3)),
  prelenarea           Nullable(Decimal (18, 3)),
  showsize             Nullable(Decimal (18, 3)),
  manager              Nullable(String),
  zipcode              Nullable(String),
  telephone            Nullable(String),
  faxno                Nullable(String),
  email                Nullable(String),
  countryid            Nullable(String),
  admindivid           Nullable(String),
  controltype          Nullable(UInt16),
  signdate             Nullable(String),
  preopendate1         Nullable(String),
  preopendate2         Nullable(String),
  opendate             Nullable(String),
  closedate            Nullable(String),
  preclosedate         Nullable(String),
  itopendate           Nullable(String),
  itclosedate          Nullable(String),
  address              Nullable(String),
  telno                Nullable(String),
  rdtelno              Nullable(String),
  cooperatetype        Nullable(String),
  tenancystartdate     Nullable(String),
  tenancyenddate       Nullable(String),
  permissionrange      Nullable(String),
  payshopid            Nullable(String),
  tsbuid               Nullable(String),
  inbuid               Nullable(String),
  dclevel              Nullable(UInt16),
  npayshopid           Nullable(String),
  shoppropid           Nullable(UInt16),
  logisticscode        Nullable(String),
  autopursysid         Nullable(String),
  wmssysid             Nullable(String),
  provinces_regions_id Nullable(String),
  provinces_regions    Nullable(String)
)
  ENGINE = Log;

------------------------------------------- 全国 ------------------------------------------- 
drop table if exists ods_hr.jv_goods;

CREATE TABLE ods_hr.jv_goods (
  buid           String,
  sysgoodsid     UInt8,
  goodsid        UInt32,
  goodscode      UInt32,
  categoryid     String,
  searchcode     Nullable(String),
  goodsname      Nullable(String),
  goodslname     Nullable(String),
  goodsename     Nullable(String),
  goodselname    Nullable(String),
  brandid        Nullable(UInt32),
  barcode        Nullable(String),
  goodstypeid    Nullable(UInt32),
  stdcategoryid  Nullable(UInt32),
  goodskind      Nullable(UInt32),
  dcategoryid    Nullable(UInt32),
  nrlulevelbu    Nullable(String),
  runtype        Nullable(UInt32),
  saletaxrate    Nullable(String),
  unioncodetype  Nullable(UInt32),
  abcflag        Nullable(String),
  goodsstatus    Nullable(UInt32),
  goodsspec      Nullable(String),
  unitname       Nullable(String),
  ycomp          Nullable(String),
  orignareaid    Nullable(String),
  producingarea  Nullable(String),
  goodsfunction  Nullable(String),
  pricerate      Nullable(UInt32),
  advicecost     Nullable(String),
  addrate        Nullable(String),
  adviceprice    Nullable(String),
  timesflag      Nullable(UInt32),
  season         Nullable(String),
  goodsclass     Nullable(UInt32),
  goodslevel     Nullable(String),
  begindate      Nullable(String),
  endtodate      Nullable(String),
  shelfspecl     Nullable(String),
  shelfspecw     Nullable(String),
  shelfspech     Nullable(String),
  qadays         Nullable(String),
  lowtemp        Nullable(String),
  hightemp       Nullable(String),
  goodscolor     Nullable(String),
  goodssize      Nullable(String),
  displaytype    Nullable(UInt32),
  regionid       Nullable(UInt32),
  indate         Nullable(String),
  inbuid         Nullable(String),
  goodsflag      Nullable(UInt32),
  headgoodsid    Nullable(UInt32),
  goodspname     Nullable(String),
  stockmode      Nullable(UInt32),
  invenderid     Nullable(String),
  tare           Nullable(UInt32),
  particularspec Nullable(String),
  headbuid       Nullable(String),
  weighttype     Nullable(String),
  newgoodsflag   Nullable(UInt32),
  goodspluname   Nullable(String),
  pricespec      Nullable(String)
)
  ENGINE = Log;
  
------------------------------------------- 全国 品类 信息  ---------------------------------

CREATE TABLE ods_hr.jv_category (
  categorytreeid UInt8,
  categoryid     UInt32,
  hqcategoryid   UInt32,
  categoryname   String,
  categorylname  String,
  categoryename  String,
  categoryelname String,
  deptlevelid    UInt32,
  headcatid      UInt32,
  catetorydepart UInt32,
  categorytype   UInt32,
  categorystatus UInt32,
  maxordervalue  UInt32,
  minordervalue  UInt32,
  goodstype      UInt32,
  goodsflag      UInt32,
  addrate        Decimal (7, 4),
  taxrate        Decimal (4, 2),
  demanddays     UInt32,
  minstockdays   UInt32,
  maxstockdays   UInt32,
  supplyflag     UInt32,
  note           String,
  consumertax    UInt32,
  priceeareflag  String
)
  ENGINE = Log;
  
---------------------------------------- 全国 验收 日 信息 ----------------------------------
CREATE TABLE ods_hr.receipt_l (
  check_date        Date,
  sheetid           String,
  bu                String,
  dc                String,
  logistics         UInt8,
  check_box_num     Decimal (18, 4),
  check_scatter_num Decimal (18, 4),
  cost              Decimal (18, 4),
  categoryid        UInt16,
  goodsid           UInt32,
  venderid          String,
  refsheetid        String,
  shopid            String
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(check_date) ORDER BY shopid
    SETTINGS index_granularity = 8192;
    
---------------------------------------- 全国 配送 日 信息 ----------------------------------    
drop table ods_hr.ration_l;

CREATE TABLE ods_hr.ration_l (
  deliver_date Date,
  out_shop_id  String,
  in_shop_id   String,
  goodsid      UInt32,
  packing_qty  Decimal (18, 4),
  qty          Decimal (18, 4),
  cost         Decimal (18, 4),
  deliver_id   String,
  type         UInt8,
  logistics    UInt8,
  costvalue    Decimal (18, 4),
  venderid     String,
  categoryid   UInt32,
  box_flag     UInt8
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(deliver_date) ORDER BY out_shop_id
    SETTINGS index_granularity = 8192;
  
---------------------------------------- 全国  门店 直送 信息 ----------------------------------    
CREATE TABLE ods_hr.receipt_store_l (
  check_date        Date,
  sheetid           String,
  bu                String,
  shop_id           String,
  logistics         Decimal (18, 4),
  check_box_num     Decimal (18, 4),
  check_scatter_num Decimal (18, 4),
  cost              Decimal (18, 4),
  categoryid        UInt32,
  goodsid           UInt32,
  venderid          String,
  refsheetid        String,
  shopformid        UInt8,
  provinces_regions String
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(check_date) ORDER BY shop_id
    SETTINGS index_granularity = 8192;
    
    

  
---------------------------------------- 香港 验收 日 信息 ----------------------------------
CREATE TABLE ods_hr.xg_check_t (
  check_date        Date,
  shop_id           String,
  order_id          String,
  logistics         String,
  check_id          String,
  barcode           String,
  goods_name        String,
  pknum             Decimal (18, 4),
  cost              Decimal (18, 4),
  category_id       String,
  check_box_num     Decimal (18, 4),
  check_scatter_num Decimal (18, 4),
  goods_code        String
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(check_date) ORDER BY shop_id
    SETTINGS index_granularity = 8192;
  
---------------------------------------- 香港 配送 日 信息 ----------------------------------

CREATE TABLE ods_hr.xg_deliver_t (
  deliver_date        Date,
  out_shop_id         String,
  in_shop_id          String,
  deliver_id          String,
  logistics           String,
  check_id            String,
  barcode             String,
  goods_name          String,
  pknum               Decimal (18, 4),
  cost                Decimal (18, 4),
  category_id         String,
  deliver_box_num     Decimal (18, 4),
  deliver_scatter_num Decimal (18, 4),
  goods_code          String
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(deliver_date) ORDER BY out_shop_id
    SETTINGS index_granularity = 8192;
  
---------------------------------------- 苏果 店铺 信息 ----------------------------------
CREATE TABLE ods_hr.sg_shop (
  shopid    String,
  shopname  String,
  status    String,
  zhuangtai String,
  yt_bm     UInt64,
  yt_name   String
)
  ENGINE = Log;
  
  

---------------------------------------- 苏果 配送 信息 ----------------------------------
CREATE TABLE ods_hr.sg_deliver_t (
  deliver_date        Date,
  out_shop_id         String,
  in_shop_id          String,
  deliver_id          String,
  logistics           String,
  check_id            String,
  barcode             String,
  goods_name          String,
  pknum               Decimal (18, 4),
  cost                Decimal (18, 4),
  category_id         String,
  deliver_box_num     Decimal (18, 4),
  deliver_scatter_num Decimal (18, 4),
  goods_code          String,
  deliver_cost        Decimal (18, 4),
  deliver_taxamount   Decimal (18, 4),
  venderid            String,
  vendername          String
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(deliver_date) ORDER BY out_shop_id
    SETTINGS index_granularity = 8192;
    

---------------------------------------- 苏果 验收 信息 ----------------------------------
CREATE TABLE ods_hr.sg_check_t (
  check_date        Date,
  shop_id           String,
  order_id          String,
  logistics         String,
  check_id          String,
  barcode           String,
  goods_name        String,
  pknum             Decimal (18, 4),
  cost              Decimal (18, 4),
  category_id       String,
  check_box_num     Decimal (18, 4),
  check_scatter_num Decimal (18, 4),
  goods_code        String,
  check_cost        Decimal (18, 4),
  check_taxamount   Decimal (18, 4),
  venderid          String,
  vendername        String
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(check_date) ORDER BY shop_id
    SETTINGS index_granularity = 8192