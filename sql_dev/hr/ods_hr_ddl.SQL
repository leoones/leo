---------------------------------------- DC 信息 ----------------------------------
CREATE TABLE ods_hr.jt_dcinfo (
  shopid     String,
  shopname   String,
  propertype String,
  dctype     String,
  square     Decimal (18, 2),
  bu_q       String,
  thoughput  Decimal (18, 2),
  storage_g  Decimal (18, 2),
  storage_b  Decimal (18, 2),
  remarks    String,
  bu_z       String,
  buid_q     Decimal (18, 2),
  buid_z     Decimal (18, 2),
  flag       Decimal (18, 2)
)
  ENGINE = Log;
  
  

---------------------------------------- 区域 信息 ----------------------------------
CREATE TABLE ods_hr.jv_buinfo (
  buid             String,
  buname           String,
  buflag           UInt32,
  rptbuid          String,
  shortbuid        String,
  buprefix         String,
  itsysalias       String,
  sysid            String,
  retailformgrpid  String,
  categorytreeid   UInt32,
  costdiscflag     UInt32,
  shopexttabname   String,
  venderexttabname String,
  goodsexttabname  String,
  headbuid         String,
  status           UInt32,
  note             String,
  ismorelevel      UInt32,
  isjv             UInt32
)
  ENGINE = Log;
  

---------------------------------------- 供应商 信息 ----------------------------------
CREATE TABLE ods_hr.jv_vender (
  sysvenderid        String,
  buid               String,
  vendercode         String,
  venderid           String,
  searchcode         String,
  vendername         String,
  venderlname        String,
  venderename        String,
  venderlename       String,
  taxpayingid        String,
  customertypeid     String,
  insidevendertype   String,
  retailtype         String,
  goodsvendertype    String,
  venderpurtype      String,
  origintype         String,
  vendercontractflag String,
  localflag          String,
  payshoplevel       String,
  payshopflag        String,
  payshopid          String,
  permitno           String,
  identitycard       String,
  daypayflag         String,
  primaryflag        String,
  paymode            String,
  address            String,
  zipno              String,
  telno              String,
  faxno              String,
  www                String,
  email              String,
  cman               String,
  cmanphone          String,
  mobilephone        String,
  bankname           String,
  bankaccount        String,
  bankaccname        String,
  wj                 String,
  zj                 String,
  bizscope           String,
  bizlevel           String,
  chairman           String,
  zoneid             String,
  taxlevel           String,
  invadd             String,
  invhead            String,
  authcapital        String,
  enterprisekind     String,
  orderday           String,
  inbrand            String,
  shipmentsto        String,
  deliverymethods    String,
  ordermode          String,
  indate             String,
  inbranch           String,
  invendee           String,
  endvendee          String,
  endreason          String,
  enddate            String,
  changereason       String,
  contracttype       String,
  rinvoiceflag       String,
  prepayflag         String,
  defavendercode     String,
  venderstatus       String,
  vmanagelevel       String,
  paytypeid          String,
  categoryid         String,
  masterbrandid      String,
  venderruntype      String,
  vendertype         String,
  venderflag         String,
  headvendercode     String,
  regionid           String,
  venderform         String,
  inoutside          String,
  quotecurrency      String,
  settlementcurrency String,
  catetorydepart     String,
  mallflag           String,
  venderlevel        String,
  vendercardid       String,
  taxpayerid         String,
  zccurrency         String,
  isotherpayshop     String,
  financetype        String,
  lshopid            String,
  runbuidstr         String,
  stockingdays       String,
  ordercosting       String,
  deliverypolicy     String,
  headbuid           String
)
  ENGINE = Log;  
  
  
  
---------------------------------------- 全国 店铺 信息 ----------------------------------

CREATE TABLE ods_hr.jv_shop (
  sysshopid            String,
  shopcode             String,
  shopid               String,
  shopname             String,
  shoplname            String,
  shopename            String,
  shopelname           String,
  headshopid           String,
  buid                 String,
  itshoptype           UInt16,
  dcshoptype           String,
  shopstatus           UInt16,
  shopclass            UInt16,
  portclass            UInt16,
  sizeclass            UInt16,
  workingfloor         UInt16,
  retailformgrpid      UInt16,
  shopformid           UInt16,
  commercecircleid     UInt16,
  regionid             UInt16,
  retailbrandid        String,
  parrentshopid        String,
  workforce            UInt16,
  coveredared          Decimal (18, 3),
  businessarea         Decimal (18, 3),
  lenarea              Decimal (18, 3),
  prelenarea           Decimal (18, 3),
  showsize             Decimal (18, 3),
  manager              String,
  zipcode              String,
  telephone            String,
  faxno                String,
  email                String,
  countryid            String,
  admindivid           String,
  controltype          UInt16,
  signdate             String,
  preopendate1         String,
  preopendate2         String,
  opendate             String,
  closedate            String,
  preclosedate         String,
  itopendate           String,
  itclosedate          String,
  address              String,
  telno                String,
  rdtelno              String,
  cooperatetype        String,
  tenancystartdate     String,
  tenancyenddate       String,
  permissionrange      String,
  payshopid            String,
  tsbuid               String,
  inbuid               String,
  dclevel              UInt16,
  npayshopid           String,
  shoppropid           UInt16,
  logisticscode        String,
  autopursysid         String,
  wmssysid             String,
  provinces_regions_id String,
  provinces_regions    String
)
  ENGINE = Log;

------------------------------------------- 全国 ------------------------------------------- 
CREATE TABLE ods_hr.jv_goods (
  buid           String,
  sysgoodsid     UInt8,
  goodsid        UInt32,
  goodscode      UInt32,
  categoryid     UInt32,
  searchcode     String,
  goodsname      String,
  goodslname     String,
  goodsename     String,
  goodselname    String,
  brandid        UInt32,
  barcode        String,
  goodstypeid    UInt32,
  stdcategoryid  UInt32,
  goodskind      UInt32,
  dcategoryid    UInt32,
  nrlulevelbu    String,
  runtype        UInt32,
  saletaxrate    String,
  unioncodetype  UInt32,
  abcflag        String,
  goodsstatus    UInt32,
  goodsspec      String,
  unitname       String,
  ycomp          String,
  orignareaid    String,
  producingarea  String,
  goodsfunction  String,
  pricerate      UInt32,
  advicecost     String,
  addrate        String,
  adviceprice    String,
  timesflag      UInt32,
  season         String,
  goodsclass     UInt32,
  goodslevel     String,
  begindate      String,
  endtodate      String,
  shelfspecl     String,
  shelfspecw     String,
  shelfspech     String,
  qadays         String,
  lowtemp        String,
  hightemp       String,
  goodscolor     String,
  goodssize      String,
  displaytype    UInt32,
  regionid       UInt32,
  indate         String,
  inbuid         String,
  goodsflag      UInt32,
  headgoodsid    UInt32,
  goodspname     String,
  stockmode      UInt32,
  invenderid     String,
  tare           UInt32,
  particularspec String,
  headbuid       String,
  weighttype     String,
  newgoodsflag   UInt32,
  goodspluname   String,
  pricespec      String
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
    
    


---------------------------------------- 全国 配送 日 信息 ----------------------------------    
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