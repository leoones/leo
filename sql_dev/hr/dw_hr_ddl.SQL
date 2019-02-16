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
  
CREATE VIEW dw_hr.vw_dw_buinfo (buid String, buname String, mapping_buid String, mapping_buname String, categorytreeid UInt32) AS
  SELECT jb.buid,
         jb.buname,
         multiIf(jb.buid = '32', '15', jb.buid)   AS mapping_buid,
         multiIf(jb.buid = '32', '东北', jb.buname) AS mapping_buname,
         jb.categorytreeid
  FROM ods_hr.jv_buinfo AS jb
;

  
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
    
CREATE VIEW dw_hr.vw_fct_rpt_dc_shop_sku_vender_day (stat_year UInt16, stat_month UInt32, stat_day Date, out_buname String, provinces_regions String, dc_shop_id String, out_shopname String, rpt_dctype String, shop_id String, shopformid_type String, venderid String, rpt_datasource String, rpt_qty Decimal (18, 4), rpt_boxes Decimal (18, 4), rpt_cost Decimal (18, 4), rpt_taxcost Decimal (18, 4)) AS
  SELECT frdssvd.stat_year,
         frdssvd.stat_month,
         frdssvd.stat_day,
         dictGetString('dw_buinfo', 'buname', tuple(buid))                          AS out_buname,
         dictGetString('dim_shop', 'provinces_regions', tuple(dc_shop_id))          AS provinces_regions,
         frdssvd.dc_shop_id,
         dictGetString('dim_shop', 'shopname', tuple(dc_shop_id))                   AS out_shopname,
         multiIf(frdssvd.dctype = 1, '干货', frdssvd.dctype = 2, '生鲜', '未知')          AS rpt_dctype,
         frdssvd.shop_id,
         multiIf(frdssvd.shopformid = 1, '大超', frdssvd.shopformid = 2, '小业态', frdssvd.shopformid = 3, '标超',
                 frdssvd.shopformid = 4, '便利店', frdssvd.shopformid = 5, '仓间', '未知') AS shopformid_type,
         frdssvd.venderid,
         multiIf(frdssvd.datasource = 1, '全国', frdssvd.datasource = 2, '香港', frdssvd.datasource = 3, '苏果',
                 frdssvd.datasource = 4, 'OLE', '未知')                               AS rpt_datasource,
         frdssvd.rpt_qty,
         frdssvd.rpt_boxes,
         frdssvd.rpt_cost,
         frdssvd.rpt_taxcost
  FROM dw_hr.fct_rpt_dc_shop_sku_vender_day AS frdssvd
;



--------------------------------------- 事实表 - 直送日清单表 ---------------------------------------
drop table if exists dw_hr.fct_rsl_dc_shop_vender_day;

create table dw_hr.fct_rsl_dc_shop_vender_day(
  stat_year UInt16 comment '直送日期-年',
	stat_month UInt32 comment '直送日期-月' ,
	stat_day Date comment '直送日期',
	out_buid String comment '区域',
	shop_id  String comment '门店id',
	logistics UInt16 comment '物流模式 1: 存储 2: 直通 3: 直送',
	categoryid UInt32 comment '品类',
	venderid  String comment '供应商',
	dc_type  UInt16 comment '干货/生鲜 1: 干货 2: 生鲜',
	shopformid UInt16 comment '门店业态 1:大超 2:小业态 3: 标超 4: 便利店 5 仓间',
	rsl_cost  Decimal (18, 4) comment '供应商直送门店金额',
	rsl_boxes Decimal (18, 4) comment '供应商直送门店箱数',
	rsl_qty Decimal (18, 4) comment '供应商直送门店散数',
	
	rsl_drygood_cost Decimal (18, 4) comment '供应商直送门店金额(干货)',
	rsl_drygood_boxes Decimal (18, 4) comment '供应商直送门店箱数(干货)',
	rsl_drygood_qty  Decimal (18, 4) comment '供应商直送门店散数(干货)',
	
	rsl_fresh_cost   Decimal (18, 4) comment '供应商直送门店金额(生鲜)',
	rsl_fresh_boxes   Decimal (18, 4) comment '供应商直送门店箱数(生鲜)',
	rsl_fresh_qty   Decimal (18, 4) comment '供应商直送门店散数(生鲜)',
	
	rsl_drygood_supshop_cost Decimal (18, 4) comment '供应商直送门店金额(干货,大超)',
	rsl_drygood_supshop_boxes Decimal (18, 4) comment '供应商直送门店箱数(干货,大超)',
	rsl_drygood_supshop_qty   Decimal (18, 4) comment '供应商直送门店散数(干货,大超)',
	
	rsl_drygood_smallshop_cost Decimal (18, 4) comment '供应商直送门店金额(干货,小业态)',
	rsl_drygood_smallshop_boxes Decimal (18, 4) comment '供应商直送门店箱数(干货,小业态)',
	rsl_drygood_smallshop_qty Decimal (18, 4) comment '供应商直送门店散数(干货,小业态)',
	
	rsl_fresh_supshop_cost   Decimal (18, 4) comment '供应商直送门店金额(生鲜,大超)',
	rsl_fresh_supshop_boxes  Decimal (18, 4) comment '供应商直送门店箱数(生鲜,大超)',
	rsl_fresh_supshop_qty  Decimal (18, 4) comment '供应商直送门店散数(生鲜,大超)',
	
	rsl_fresh_smallshop_cost Decimal (18, 4) comment '供应商直送门店金额(生鲜,小业态)',
	rsl_fresh_smallshop_boxes Decimal (18, 4) comment '供应商直送门店箱数(生鲜,小业态)',
	rsl_fresh_smallshop_qty   Decimal (18, 4) comment '供应商直送门店散数(生鲜,小业态)'
) 
ENGINE = MergeTree
    PARTITION BY toYYYYMM(stat_day) ORDER BY (shop_id, venderid)
    SETTINGS index_granularity = 8192;
    
    
create view vw_fct_rsl_dc_shop_vender_day as 
select
        stat_year                   ,
        stat_month                  ,
        stat_day                    ,
        out_buid                    ,
        dictGetString('dw_buinfo', 'buname', tuple(out_buid)) as buname,
        shop_id                     ,
        dictGetString('dim_shop', 'shopname', tuple(shop_id)) as shopname,
        dictGetString('dim_shop', 'provinces_regions', tuple(shop_id)) as provinces_regions,
        dictGetUInt32('dim_shop', 'categorytreeid', tuple(shop_id)) as categorytreeid,
        multiIf(logistics=1,'存储', logistics=2, '直通', logistics=3, '直送','未知') as rsl_logistics                   ,
        categoryid                  ,
        dictGetString('dw_category_info', 'categoryname', tuple(toUInt8(categorytreeid), categoryid)) as categoryname,
        dictGetUInt32('dw_category_info', 'mid_categoryid', tuple(toUInt8(categorytreeid), categoryid)) as mid_categoryid,
        dictGetString('dw_category_info', 'mid_categoryname', tuple(toUInt8(categorytreeid), categoryid)) as mid_categoryname,
        dictGetUInt32('dw_category_info', 'h_categoryid', tuple(toUInt8(categorytreeid), categoryid)) as h_categoryid,
        dictGetString('dw_category_info', 'h_categoryname', tuple(toUInt8(categorytreeid), categoryid)) as h_categoryname,
        venderid                    ,
        dictGetString('dw_vender_info', 'vendername', tuple(out_buid, venderid)) as vendername,
        dictGetString('dw_vender_info', 'parent_venderid', tuple(out_buid, venderid)) as parent_venderid,
        dictGetString('dw_vender_info', 'parent_vendername', tuple(out_buid, venderid)) as parent_vendername,
        multiIf(dc_type=1, '干货', dc_type=2, '生鲜', '未知' ) as rsl_dctype,
        multiIf(shopformid=1, '大超', shopformid=2, '小业态', shopformid=3, '标超',
                 shopformid=4, '便利店',  shopformid=4, '仓间', '未知') as rsl_shopformid,
        rsl_cost                    ,
        rsl_boxes                   ,
        rsl_qty                     ,
        rsl_drygood_cost            ,
        rsl_drygood_boxes           ,
        rsl_drygood_qty             ,
        rsl_fresh_cost              ,
        rsl_fresh_boxes             ,
        rsl_fresh_qty               ,
        rsl_drygood_supshop_cost    ,
        rsl_drygood_supshop_boxes   ,
        rsl_drygood_supshop_qty     ,
        rsl_drygood_smallshop_cost  ,
        rsl_drygood_smallshop_boxes ,
        rsl_drygood_smallshop_qty   ,
        rsl_fresh_supshop_cost      ,
        rsl_fresh_supshop_boxes     ,
        rsl_fresh_supshop_qty       ,
        rsl_fresh_smallshop_cost    ,
        rsl_fresh_smallshop_boxes
from dw_hr.fct_rsl_dc_shop_vender_day



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
    
CREATE VIEW dw_hr.vw_fct_rt_dc_shop_sku_vender_day (stat_year UInt16, stat_month UInt32, stat_day Date, out_buid String, out_buname String, out_shop_id String, out_shopname String, out_provinces_regions String, rt_dctype String, rt_buntype String, in_shop_id String, in_shopname String, in_provinces_regions String, shopformid_type String, rt_datasource String, categoryname String, mid_categoryid UInt32, mid_categoryname String, h_categoryid UInt32, h_categoryname String, p_venderid String, p_vendernm String, rt_qty Decimal (18, 4), rt_cost Decimal (18, 4), rt_taxcost Decimal (18, 4), rt_shops String, rt_drygood_qty Decimal (18, 4), rt_drygood_cost Decimal (18, 4), rt_drygood_boxes Decimal (18, 4), rt_drygood_shops String, rt_fresh_qty Decimal (18, 4), rt_fresh_cost Decimal (18, 4), rt_fresh_shops String, rt_supshop_cost Decimal (18, 4), rt_supshop_qty Decimal (18, 4), rt_supshop_shops String, rt_smallshop_cost Decimal (18, 4), rt_smallshop_qty Decimal (18, 4), rt_smallshop_shops String, rt_dc_cost Decimal (18, 4), rt_dc_qty Decimal (18, 4), rt_drygood_supshop_cost Decimal (18, 4), rt_drygood_supshop_qty Decimal (18, 4), rt_drygood_supshop_boxes Decimal (18, 4), rt_drygood_supshop_shops String, rt_drygood_smallshop_cost Decimal (18, 4), rt_drygood_smallshop_qty Decimal (18, 4), rt_drygood_smallshop_boxes Decimal (18, 4), rt_drygood_smallshop_shops String, rt_drygood_dc_cost Decimal (18, 4), rt_drygood_dc_qty Decimal (18, 4), rt_drygood_dc_boxes Decimal (18, 4), rt_drygood_dc_shops String, rt_fresh_supshop_cost Decimal (18, 4), rt_fresh_supshop_qty Decimal (18, 4), rt_fresh_supshop_shops String, rt_fresh_smallshop_cost Decimal (18, 4), rt_fresh_smallshop_qty Decimal (18, 4), rt_fresh_smallshop_shops String, rt_fresh_dc_cost Decimal (18, 4), rt_fresh_dc_qty Decimal (18, 4)) AS
  SELECT frt.stat_year,
         frt.stat_month,
         frt.stat_day,
         frt.out_buid,
         dictGetString('dw_buinfo', 'buname', tuple(out_buid))                                       AS out_buname,
         frt.out_shop_id,
         dictGetString('dw_shop', 'shopname', tuple(out_shop_id))                                    AS out_shopname,
         dictGetString('dw_shop', 'provinces_regions', tuple(out_shop_id))                           AS out_provinces_regions,
         multiIf(frt.dctype = 1, '干货', frt.dctype = 2, '生鲜', '未知')                                   AS rt_dctype,
         multiIf(frt.buntype = 1, '配送', frt.buntype = 3, '返配', frt.buntype = 5, '调拨', frt.buntype = 6, '批发',
                 frt.buntype = 7, '提货', '未知')                                                        AS rt_buntype,
         frt.in_shop_id,
         dictGetString('dw_shop', 'shopname', tuple(in_shop_id))                                     AS in_shopname,
         dictGetString('dw_shop', 'provinces_regions', tuple(in_shop_id))                            AS in_provinces_regions,
         multiIf(frt.shopformid = 1, '大超', frt.shopformid = 2, '小业态', frt.shopformid = 3, '标超', frt.shopformid = 4,
                 '便利店', frt.shopformid = 5, '仓间', '未知')                                              AS shopformid_type,
         multiIf(frt.datasource = 1, '全国', frt.datasource = 2, '香港', frt.datasource = 3, '苏果', frt.datasource = 4,
                 'OLE', '未知')                                                                        AS rt_datasource,
         dictGetString('dw_category_info', 'categoryname', (frt.categorytreeid, frt.categoryid))     AS categoryname,
         dictGetUInt32('dw_category_info', 'mid_categoryid', (frt.categorytreeid, frt.categoryid))   AS mid_categoryid,
         dictGetString('dw_category_info', 'mid_categoryname', (frt.categorytreeid,
                                                                frt.categoryid))                     AS mid_categoryname,
         dictGetUInt32('dw_category_info', 'h_categoryid', (frt.categorytreeid, frt.categoryid))     AS h_categoryid,
         dictGetString('dw_category_info', 'h_categoryname', (frt.categorytreeid, frt.categoryid))   AS h_categoryname,
         dictGetString('dw_vender_info', 'parent_venderid', (out_buid, frt.venderid))                AS p_venderid,
         dictGetString('dw_vender_info', 'parent_vendername', (out_buid, frt.venderid))              AS p_vendernm,
         frt.rt_qty,
         frt.rt_cost,
         frt.rt_taxcost,
         frt.rt_shops,
         rt_drygood_qty,
         rt_drygood_cost,
         rt_drygood_boxes,
         rt_drygood_shops,
         rt_fresh_qty,
         rt_fresh_cost,
         rt_fresh_shops,
         rt_supshop_cost,
         rt_supshop_qty,
         rt_supshop_shops,
         rt_smallshop_cost,
         rt_smallshop_qty,
         rt_smallshop_shops,
         rt_dc_cost,
         rt_dc_qty,
         rt_drygood_supshop_cost,
         rt_drygood_supshop_qty,
         rt_drygood_supshop_boxes,
         rt_drygood_supshop_shops,
         rt_drygood_smallshop_cost,
         rt_drygood_smallshop_qty,
         rt_drygood_smallshop_boxes,
         rt_drygood_smallshop_shops,
         rt_drygood_dc_cost,
         rt_drygood_dc_qty,
         rt_drygood_dc_boxes,
         rt_drygood_dc_shops,
         rt_fresh_supshop_cost,
         rt_fresh_supshop_qty,
         rt_fresh_supshop_shops,
         rt_fresh_smallshop_cost,
         rt_fresh_smallshop_qty,
         rt_fresh_smallshop_shops,
         rt_fresh_dc_cost,
         rt_fresh_dc_qty
  FROM dw_hr.fct_rt_dc_shop_sku_vender_day AS frt
;

    

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


CREATE VIEW dw_hr.vw_fct_rt_dc_shop_sku_vender_mon (stat_year UInt16, stat_month UInt32, out_buid String, out_buname String, out_shop_id String, out_shopname String, out_provinces_regions String, rt_dctype String, rt_buntype String, in_shop_id String, in_shopname String, in_provinces_regions String, shopformid_type String, rt_datasource String, categoryname String, mid_categoryid UInt32, mid_categoryname String, h_categoryid UInt32, h_categoryname String, p_venderid String, p_vendernm String, rt_qty Decimal (18, 4), rt_cost Decimal (18, 4), rt_taxcost Decimal (18, 4), rt_shops String, rt_drygood_qty Decimal (18, 4), rt_drygood_cost Decimal (18, 4), rt_drygood_boxes Decimal (18, 4), rt_drygood_shops String, rt_fresh_qty Decimal (18, 4), rt_fresh_cost Decimal (18, 4), rt_fresh_shops String, rt_supshop_cost Decimal (18, 4), rt_supshop_qty Decimal (18, 4), rt_supshop_shops String, rt_smallshop_cost Decimal (18, 4), rt_smallshop_qty Decimal (18, 4), rt_smallshop_shops String, rt_dc_cost Decimal (18, 4), rt_dc_qty Decimal (18, 4), rt_drygood_supshop_cost Decimal (18, 4), rt_drygood_supshop_qty Decimal (18, 4), rt_drygood_supshop_boxes Decimal (18, 4), rt_drygood_supshop_shops String, rt_drygood_smallshop_cost Decimal (18, 4), rt_drygood_smallshop_qty Decimal (18, 4), rt_drygood_smallshop_boxes Decimal (18, 4), rt_drygood_smallshop_shops String, rt_drygood_dc_cost Decimal (18, 4), rt_drygood_dc_qty Decimal (18, 4), rt_drygood_dc_boxes Decimal (18, 4), rt_drygood_dc_shops String, rt_fresh_supshop_cost Decimal (18, 4), rt_fresh_supshop_qty Decimal (18, 4), rt_fresh_supshop_shops String, rt_fresh_smallshop_cost Decimal (18, 4), rt_fresh_smallshop_qty Decimal (18, 4), rt_fresh_smallshop_shops String, rt_fresh_dc_cost Decimal (18, 4), rt_fresh_dc_qty Decimal (18, 4), cost_percent Decimal (18, 6)) AS
  SELECT frt.stat_year,
         frt.stat_month,
         frt.out_buid,
         dictGetString('dw_buinfo', 'buname', tuple(out_buid))                                       AS out_buname,
         frt.out_shop_id,
         dictGetString('dw_shop', 'shopname', tuple(out_shop_id))                                    AS out_shopname,
         dictGetString('dw_shop', 'provinces_regions', tuple(out_shop_id))                           AS out_provinces_regions,
         multiIf(frt.dctype = 1, '干货', frt.dctype = 2, '生鲜', '未知')                                   AS rt_dctype,
         multiIf(frt.buntype = 1, '配送', frt.buntype = 3, '返配', frt.buntype = 5, '调拨', frt.buntype = 6, '批发',
                 frt.buntype = 7, '提货', '未知')                                                        AS rt_buntype,
         frt.in_shop_id,
         dictGetString('dw_shop', 'shopname', tuple(in_shop_id))                                     AS in_shopname,
         dictGetString('dw_shop', 'provinces_regions', tuple(in_shop_id))                            AS in_provinces_regions,
         multiIf(frt.shopformid = 1, '大超', frt.shopformid = 2, '小业态', frt.shopformid = 3, '标超', frt.shopformid = 4,
                 '便利店', frt.shopformid = 5, '仓间', '未知')                                              AS shopformid_type,
         multiIf(frt.datasource = 1, '全国', frt.datasource = 2, '香港', frt.datasource = 3, '苏果', frt.datasource = 4,
                 'OLE', '未知')                                                                        AS rt_datasource,
         dictGetString('dw_category_info', 'categoryname', (frt.categorytreeid, frt.categoryid))     AS categoryname,
         dictGetUInt32('dw_category_info', 'mid_categoryid', (frt.categorytreeid, frt.categoryid))   AS mid_categoryid,
         dictGetString('dw_category_info', 'mid_categoryname', (frt.categorytreeid,
                                                                frt.categoryid))                     AS mid_categoryname,
         dictGetUInt32('dw_category_info', 'h_categoryid', (frt.categorytreeid, frt.categoryid))     AS h_categoryid,
         dictGetString('dw_category_info', 'h_categoryname', (frt.categorytreeid, frt.categoryid))   AS h_categoryname,
         dictGetString('dw_vender_info', 'parent_venderid', (out_buid, frt.venderid))                AS p_venderid,
         dictGetString('dw_vender_info', 'parent_vendername', (out_buid, frt.venderid))              AS p_vendernm,
         frt.rt_qty,
         frt.rt_cost,
         frt.rt_taxcost,
         frt.rt_shops,
         rt_drygood_qty,
         rt_drygood_cost,
         rt_drygood_boxes,
         rt_drygood_shops,
         rt_fresh_qty,
         rt_fresh_cost,
         rt_fresh_shops,
         rt_supshop_cost,
         rt_supshop_qty,
         rt_supshop_shops,
         rt_smallshop_cost,
         rt_smallshop_qty,
         rt_smallshop_shops,
         rt_dc_cost,
         rt_dc_qty,
         rt_drygood_supshop_cost,
         rt_drygood_supshop_qty,
         rt_drygood_supshop_boxes,
         rt_drygood_supshop_shops,
         rt_drygood_smallshop_cost,
         rt_drygood_smallshop_qty,
         rt_drygood_smallshop_boxes,
         rt_drygood_smallshop_shops,
         rt_drygood_dc_cost,
         rt_drygood_dc_qty,
         rt_drygood_dc_boxes,
         rt_drygood_dc_shops,
         rt_fresh_supshop_cost,
         rt_fresh_supshop_qty,
         rt_fresh_supshop_shops,
         rt_fresh_smallshop_cost,
         rt_fresh_smallshop_qty,
         rt_fresh_smallshop_shops,
         rt_fresh_dc_cost,
         rt_fresh_dc_qty,
         toDecimal64(frt.rt_cost, 6) / toDecimal64(frt.rt_outshop_cost, 4)                           AS cost_percent
  FROM dw_hr.fct_rt_dc_shop_sku_vender_mon AS frt
;

