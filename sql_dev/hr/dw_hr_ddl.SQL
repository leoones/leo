--------------------------------------- 维度表 - 供应商 ---------------------------------------
drop table dw_hr.dw_vender_info;

CREATE TABLE dw_hr.dw_vender_info (
  buid              UInt8,
  venderid          UInt64,
  vendername        String,
  vendercode        String,
  parent_buid       UInt8,
  parent_venderid   UInt64,
  parent_vendername String
) ENGINE = Log;
  
CREATE VIEW dw_hr.vw_dw_buinfo (buid String, buname String, mapping_buid String, mapping_buname String, categorytreeid UInt32) AS
  SELECT jb.buid,
         jb.buname,
         multiIf(jb.buid = '32', '15', jb.buid)   AS mapping_buid,
         multiIf(jb.buid = '32', '东北', jb.buname) AS mapping_buname,
         jb.categorytreeid
  FROM ods_hr.jv_buinfo AS jb
;

  
--------------------------------------- 维度表 - 商品品类 ---------------------------------------
drop table if exists dw_hr.dw_category_info;


create table dw_hr.dw_category_info
(
  categorytreeid   UInt8,
  deptlevelid      UInt8,
  categoryid       String,
  categoryname     String,
  mid_categoryid   String,
  mid_categoryname String,
  h_categoryid     String,
  h_categoryname   String
)
  engine = Log;


  
  
--------------------------------------- 维度表 - 商品 ---------------------------------------
drop table dw_hr.dw_good_info;

create table dw_hr.dw_good_info
(
  buid             UInt8,
  categoryid       String,
  categorytreeid   UInt8,
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
  mid_categoryid   String,
  mid_categoryname String,
  h_categoryid     String,
  h_categoryname   String
)
  engine = Log;



  
  
--------------------------------------- 维度表 - 店铺信息 ---------------------------------------
drop table dw_hr.dw_shop_info;

create table dw_hr.dw_shop_info
(
  shopid            String,
  shopcode          String,
  shopname          String,
  buid              UInt8,
  inbuid            UInt8,
  shopformid        UInt8,
  shopstatus        UInt8,
  buname            String,
  categorytreeid    UInt8,
  payshopid         String,
  regionid          UInt16,
  provinces_regions String,
  propertype        String,
  dctype            String
)
  engine = Log;




--------------------------------------- 事实表 --> 验收主题 ---------------------------------------
		  --- 天, dc, 小类,供应商
			DROP TABLE dw_hr.fct_rpt_dc_shop_sku_vender_day;
			
			CREATE TABLE dw_hr.fct_rpt_dc_shop_sku_vender_day (
			  stat_year   UInt16,
			  stat_month  UInt32,
			  stat_day    Date,
			  buid        String,
			  dc_shop_id  String,
			  dctype      UInt8,
			  shop_id     String,
			  shopformid  UInt8,
			  categorytreeid UInt8,
		  categoryid  String,
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
		    
		    
		 --- 月,dc, 小类,供应商
		 drop table if exists dw_hr.fct_rpt_dc_shop_sku_vender_mon;
		
		 create table dw_hr.fct_rpt_dc_shop_sku_vender_mon
			(
				stat_year UInt16,
				stat_month UInt32,
				buid String,
				dc_shop_id String,
				dctype UInt8,
				shop_id String,
				shopformid UInt8,
				categorytreeid UInt8,
				categoryid String,
				logistics UInt8,
				venderid String,
				datasource UInt8,
				rpt_qty Decimal(18,4),
				rpt_boxes Decimal(18,4),
				rpt_cost Decimal(18,4),
				rpt_taxcost Decimal(18,4)
			)
			engine = MergeTree PARTITION BY stat_month ORDER BY (stat_month, dc_shop_id) SETTINGS index_granularity = 8192;
			
		 drop table dw_hr.vw_fct_rpt_dc_shop_sku_vender_day;
		
		 CREATE VIEW dw_hr.vw_fct_rpt_dc_shop_sku_vender_day  as 
			  SELECT frdssvd.stat_year,
			         frdssvd.stat_month,
			         frdssvd.stat_day,
			         dictGetString('dw_buinfo', 'buname', tuple(buid))                                                   AS out_buname,
			         dictGetString('dw_shop', 'provinces_regions', tuple(dc_shop_id))                                    AS provinces_regions,
			         frdssvd.dc_shop_id,
			         dictGetString('dw_shop', 'shopname', tuple(dc_shop_id))                                             AS out_shopname,
			         multiIf(frdssvd.dctype = 1, '干货', frdssvd.dctype = 2, '生鲜', '未知')                                   AS rpt_dctype,
			         frdssvd.shop_id,
			         multiIf(frdssvd.shopformid = 1, '大超', frdssvd.shopformid = 2, '小业态', frdssvd.shopformid = 3, '标超',
			                 frdssvd.shopformid = 4, '便利店', frdssvd.shopformid = 5, '仓间',
			                 '未知')                                                                                       AS shopformid_type,
			         frdssvd.venderid,
			         dictGetString('dw_vender_info', 'parent_venderid', (buid,
			                                                             venderid))                                      AS p_venderid,
			         dictGetString('dw_vender_info', 'parent_vendername', (buid,
			                                                               venderid))                                    AS p_vendernm,
			         multiIf(frdssvd.datasource = 1, '全国', frdssvd.datasource = 2, '香港', frdssvd.datasource = 3, '苏果',
			                 frdssvd.datasource = 4, 'OLE',
			                 '未知')                                                                                       AS rpt_datasource,
			         frdssvd.categoryid,
			         dictGetString('dw_category_info', 'categoryname', (frdssvd.categorytreeid,
			                                                            frdssvd.categoryid))                             AS categoryname,
			         dictGetString('dw_category_info', 'mid_categoryid', (frdssvd.categorytreeid,
			                                                              frdssvd.categoryid))                           AS mid_categoryid,
			         dictGetString('dw_category_info', 'mid_categoryname', (frdssvd.categorytreeid,
			                                                                frdssvd.categoryid))                         AS mid_categoryname,
			         dictGetString('dw_category_info', 'h_categoryid', (frdssvd.categorytreeid,
			                                                            frdssvd.categoryid))                             AS h_categoryid,
			         dictGetString('dw_category_info', 'h_categoryname', (frdssvd.categorytreeid,
			                                                              frdssvd.categoryid))                           AS h_categoryname,
			         frdssvd.rpt_qty,
			         frdssvd.rpt_boxes,
			         frdssvd.rpt_cost,
			         frdssvd.rpt_taxcost
			  FROM dw_hr.fct_rpt_dc_shop_sku_vender_day AS frdssvd;
			  
			  
			  
				 drop table if exists dw_hr.vw_fct_rpt_dc_shop_sku_vender_mon;
				
				 CREATE VIEW dw_hr.vw_fct_rpt_dc_shop_sku_vender_mon  as 
					  SELECT frdssvd.stat_year,
					         frdssvd.stat_month,
					         dictGetString('dw_buinfo', 'buname', tuple(buid))                                                   AS out_buname,
					         dictGetString('dw_shop', 'provinces_regions', tuple(dc_shop_id))                                    AS provinces_regions,
					         frdssvd.dc_shop_id,
					         dictGetString('dw_shop', 'shopname', tuple(dc_shop_id))                                             AS out_shopname,
					         multiIf(frdssvd.dctype = 1, '干货', frdssvd.dctype = 2, '生鲜', '未知')                                   AS rpt_dctype,
					         frdssvd.shop_id,
					         multiIf(frdssvd.shopformid = 1, '大超', frdssvd.shopformid = 2, '小业态', frdssvd.shopformid = 3, '标超',
					                 frdssvd.shopformid = 4, '便利店', frdssvd.shopformid = 5, '仓间',
					                 '未知')                                                                                       AS shopformid_type,
					         frdssvd.venderid,
					         dictGetString('dw_vender_info', 'parent_venderid', (buid,
					                                                             venderid))                                      AS p_venderid,
					         dictGetString('dw_vender_info', 'parent_vendername', (buid,
					                                                               venderid))                                    AS p_vendernm,
					         multiIf(frdssvd.datasource = 1, '全国', frdssvd.datasource = 2, '香港', frdssvd.datasource = 3, '苏果',
					                 frdssvd.datasource = 4, 'OLE',
					                 '未知')                                                                                       AS rpt_datasource,
					         frdssvd.categoryid,
					         dictGetString('dw_category_info', 'categoryname', (frdssvd.categorytreeid,
					                                                            frdssvd.categoryid))                             AS categoryname,
					         dictGetString('dw_category_info', 'mid_categoryid', (frdssvd.categorytreeid,
					                                                              frdssvd.categoryid))                           AS mid_categoryid,
					         dictGetString('dw_category_info', 'mid_categoryname', (frdssvd.categorytreeid,
					                                                                frdssvd.categoryid))                         AS mid_categoryname,
					         dictGetString('dw_category_info', 'h_categoryid', (frdssvd.categorytreeid,
					                                                            frdssvd.categoryid))                             AS h_categoryid,
					         dictGetString('dw_category_info', 'h_categoryname', (frdssvd.categorytreeid,
					                                                              frdssvd.categoryid))                           AS h_categoryname,
					         frdssvd.rpt_qty,
					         frdssvd.rpt_boxes,
					         frdssvd.rpt_cost,
					         frdssvd.rpt_taxcost
					  FROM dw_hr.fct_rpt_dc_shop_sku_vender_mon AS frdssvd;







--------------------------------------- 事实表 - 直送日清单表 ---------------------------------------
drop table if exists dw_hr.fct_rsl_dc_shop_vender_day;

create table dw_hr.fct_rsl_dc_shop_vender_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	out_buid String,
	shop_id String,
	logistics UInt16,
	categoryid String,
	venderid String,
	dc_type UInt8,
	shopformid UInt8,
	rsl_cost Decimal(18,4),
	rsl_boxes Decimal(18,4),
	rsl_qty Decimal(18,4),
	rsl_drygood_cost Decimal(18,4),
	rsl_drygood_boxes Decimal(18,4),
	rsl_drygood_qty Decimal(18,4),
	rsl_fresh_cost Decimal(18,4),
	rsl_fresh_boxes Decimal(18,4),
	rsl_fresh_qty Decimal(18,4),
	rsl_drygood_supshop_cost Decimal(18,4),
	rsl_drygood_supshop_boxes Decimal(18,4),
	rsl_drygood_supshop_qty Decimal(18,4),
	rsl_drygood_smallshop_cost Decimal(18,4),
	rsl_drygood_smallshop_boxes Decimal(18,4),
	rsl_drygood_smallshop_qty Decimal(18,4),
	rsl_fresh_supshop_cost Decimal(18,4),
	rsl_fresh_supshop_boxes Decimal(18,4),
	rsl_fresh_supshop_qty Decimal(18,4),
	rsl_fresh_smallshop_cost Decimal(18,4),
	rsl_fresh_smallshop_boxes Decimal(18,4),
	rsl_fresh_smallshop_qty Decimal(18,4)
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (shop_id, venderid) SETTINGS index_granularity = 8192;


    
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


