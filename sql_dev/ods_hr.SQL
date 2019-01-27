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
  
  
  
  
