--------------------------------------- 维度表 - 供应商 ---------------------------------------
CREATE TABLE dw_hr.dw_vender_info (
  buid              String,
  venderid          String,
  vendername        String,
  vendercode        String,
  parent_buid       String,
  parent_venderid   String,
  parent_vendername String
)
  ENGINE = Log;
  
  
--------------------------------------- 维度表 - 商品品类 ---------------------------------------
CREATE TABLE dw_hr.dw_category_info (
  categorytreeid   UInt8,
  deptlevelid      UInt32,
  categoryid       UInt32,
  categoryname     String,
  mid_categoryid   UInt32,
  mid_categoryname String,
  h_categoryid     UInt32,
  h_categoryname   String
)
  ENGINE = Log;
  
  
--------------------------------------- 维度表 - 商品 ---------------------------------------
CREATE TABLE dw_hr.dw_good_info (
  buid             String,
  categoryid       UInt32,
  categorytreeid   UInt32,
  sysgoodsid       UInt8,
  goodsid          UInt32,
  goodscode        UInt32,
  goodsname        String,
  barcode          String,
  unitname         String,
  inbuid           String,
  producingarea    String,
  adviceprice      String,
  advicecost       String,
  addrate          String,
  brandid          UInt32,
  categoryname     String,
  mid_categoryid   UInt32,
  mid_categoryname String,
  h_categoryid     UInt32,
  h_categoryname   String
)
  ENGINE = Log;
  
  
--------------------------------------- 维度表 - 店铺信息 ---------------------------------------

CREATE TABLE dw_hr.dw_shop_info (
  shopid            String,
  shopcode          String,
  shopname          String,
  buid              String,
  inbuid            String,
  shopformid        UInt16,
  shopstatus        UInt16,
  buname            String,
  categorytreeid    UInt32,
  payshopid         String,
  regionid          UInt16,
  provinces_regions String,
  propertype        String,
  dctype            String
)
  ENGINE = Log;


--------------------------------------- 事实表 - 验收日清单表 ---------------------------------------
CREATE TABLE dw_hr.fct_rpt_dc_shop_sku_vender_day (
  stat_year   UInt16,
  stat_month  UInt32,
  stat_day    Date,
  buid        String,
  dc_shop_id  String,
  dctype      UInt8,
  shop_id     String,
  shopformid  UInt8,
  goodsid     UInt32,
  logistics   UInt8,
  venderid    String,
  datasource  UInt8,
  rpt_qty     Decimal (18, 4),
  rpt_boxes   Decimal (18, 4),
  rpt_cost    Decimal (18, 4),
  rpt_taxcost Decimal (18, 4)
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(stat_day) ORDER BY (stat_day, dc_shop_id)
    SETTINGS index_granularity = 8192;


--------------------------------------- 事实表 - 配送日清单表 ---------------------------------------
CREATE TABLE dw_hr.fct_rt_dc_shop_sku_vender_day (
  stat_year                  UInt16,
  stat_month                 UInt32,
  stat_day                   Date,
  out_buid                   String,
  out_shop_id                String,
  in_shop_id                 String,
  datasource                 UInt8,
  venderid                   String,
  categorytreeid             UInt8,
  categoryid                 UInt32,
  buntype                    UInt8,
  dctype                     UInt8,
  shopformid                 UInt8,
  rt_qty                     Decimal (18, 4),
  rt_cost                    Decimal (18, 4),
  rt_taxcost                 Decimal (18, 4),
  rt_shops                   String,
  rt_drygood_qty             Decimal (18, 4),
  rt_drygood_cost            Decimal (18, 4),
  rt_drygood_boxes           Decimal (18, 4),
  rt_drygood_shops           String,
  rt_fresh_qty               Decimal (18, 4),
  rt_fresh_cost              Decimal (18, 4),
  rt_fresh_shops             String,
  rt_supshop_cost            Decimal (18, 4),
  rt_supshop_qty             Decimal (18, 4),
  rt_supshop_shops           String,
  rt_smallshop_cost          Decimal (18, 4),
  rt_smallshop_qty           Decimal (18, 4),
  rt_smallshop_shops         String,
  rt_dc_cost                 Decimal (18, 4),
  rt_dc_qty                  Decimal (18, 4),
  rt_drygood_supshop_cost    Decimal (18, 4),
  rt_drygood_supshop_qty     Decimal (18, 4),
  rt_drygood_supshop_boxes   Decimal (18, 4),
  rt_drygood_supshop_shops   String,
  rt_drygood_smallshop_cost  Decimal (18, 4),
  rt_drygood_smallshop_qty   Decimal (18, 4),
  rt_drygood_smallshop_boxes Decimal (18, 4),
  rt_drygood_smallshop_shops String,
  rt_drygood_dc_cost         Decimal (18, 4),
  rt_drygood_dc_qty          Decimal (18, 4),
  rt_drygood_dc_boxes        Decimal (18, 4),
  rt_drygood_dc_shops        String,
  rt_fresh_supshop_cost      Decimal (18, 4),
  rt_fresh_supshop_qty       Decimal (18, 4),
  rt_fresh_supshop_shops     String,
  rt_fresh_smallshop_cost    Decimal (18, 4),
  rt_fresh_smallshop_qty     Decimal (18, 4),
  rt_fresh_smallshop_shops   String,
  rt_fresh_dc_cost           Decimal (18, 4),
  rt_fresh_dc_qty            Decimal (18, 4)
)
  ENGINE = MergeTree
    PARTITION BY toYYYYMM(stat_day) ORDER BY (stat_day, out_shop_id)
    SETTINGS index_granularity = 8192;
    
    

--------------------------------------- 事实表 - 配送月清单表 ---------------------------------------
CREATE TABLE dw_hr.fct_rt_dc_shop_sku_vender_mon (
  stat_year                  UInt16,
  stat_month                 UInt32,
  out_buid                   String,
  out_shop_id                String,
  in_shop_id                 String,
  datasource                 UInt8,
  venderid                   String,
  categorytreeid             UInt8,
  categoryid                 UInt32,
  buntype                    UInt8,
  dctype                     UInt8,
  shopformid                 UInt8,
  rt_qty                     Decimal (18, 4),
  rt_cost                    Decimal (18, 4),
  rt_taxcost                 Decimal (18, 4),
  rt_shops                   String,
  rt_drygood_qty             Decimal (18, 4),
  rt_drygood_cost            Decimal (18, 4),
  rt_drygood_boxes           Decimal (18, 4),
  rt_drygood_shops           String,
  rt_fresh_qty               Decimal (18, 4),
  rt_fresh_cost              Decimal (18, 4),
  rt_fresh_shops             String,
  rt_supshop_cost            Decimal (18, 4),
  rt_supshop_qty             Decimal (18, 4),
  rt_supshop_shops           String,
  rt_smallshop_cost          Decimal (18, 4),
  rt_smallshop_qty           Decimal (18, 4),
  rt_smallshop_shops         String,
  rt_dc_cost                 Decimal (18, 4),
  rt_dc_qty                  Decimal (18, 4),
  rt_drygood_supshop_cost    Decimal (18, 4),
  rt_drygood_supshop_qty     Decimal (18, 4),
  rt_drygood_supshop_boxes   Decimal (18, 4),
  rt_drygood_supshop_shops   String,
  rt_drygood_smallshop_cost  Decimal (18, 4),
  rt_drygood_smallshop_qty   Decimal (18, 4),
  rt_drygood_smallshop_boxes Decimal (18, 4),
  rt_drygood_smallshop_shops String,
  rt_drygood_dc_cost         Decimal (18, 4),
  rt_drygood_dc_qty          Decimal (18, 4),
  rt_drygood_dc_boxes        Decimal (18, 4),
  rt_drygood_dc_shops        String,
  rt_fresh_supshop_cost      Decimal (18, 4),
  rt_fresh_supshop_qty       Decimal (18, 4),
  rt_fresh_supshop_shops     String,
  rt_fresh_smallshop_cost    Decimal (18, 4),
  rt_fresh_smallshop_qty     Decimal (18, 4),
  rt_fresh_smallshop_shops   String,
  rt_fresh_dc_cost           Decimal (18, 4),
  rt_fresh_dc_qty            Decimal (18, 4),
  rt_outshop_cost            Decimal (18, 4),
  rt_outshop_boxes           Decimal (18, 4),
  rt_outshop_qty             Decimal (18, 4)
)
  ENGINE = MergeTree
    PARTITION BY stat_month ORDER BY out_shop_id
    SETTINGS index_granularity = 8192;
