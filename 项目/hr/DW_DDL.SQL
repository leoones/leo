create table dw_hr.dw_bu_info
(
	buid UInt8,
	buname String,
	mapping_buid UInt8,
	mapping_buname String,
	categorytreeid UInt8
)
engine = Log
;

create table dw_hr.dw_category_info
(
	categorytreeid UInt8,
	deptlevelid UInt8,
	categoryid String,
	categoryname String,
	mid_categoryid String,
	mid_categoryname String,
	h_categoryid String,
	h_categoryname String
)
engine = Log
;

create table dw_hr.dw_good_info
(
	buid UInt8,
	categoryid String,
	categorytreeid UInt8,
	sysgoodsid UInt8,
	goodsid String,
	goodscode String,
	goodsname String,
	goodsname_weigth_unit_list Array(String),
	goodsname_weigth_list Array(Decimal(18, 4)),
	goodsname_weigth_total Decimal(18,4),
	barcode String,
	unitname String,
	inbuid String,
	goodsspec String,
	goodsspec_weigth_unit_list Array(String),
	goodsspec_weigth_list Array(Decimal(18, 4)),
	goodsspec_weigth_total Decimal(18,4),
	producingarea String,
	adviceprice String,
	advicecost String,
	addrate String,
	brandid UInt32,
	categoryname String,
	mid_categoryid String,
	mid_categoryname String,
	h_categoryid String,
	h_categoryname String,
	create_time Date
)
engine = MergeTree PARTITION BY toYYYYMM(create_time) ORDER BY (buid, categoryid) SETTINGS index_granularity = 8192
;

create table dw_hr.dw_good_info_new
(
	buid UInt8,
	categoryid String,
	categorytreeid UInt8,
	sysgoodsid UInt8,
	goodsid String,
	goodscode String,
	goodsname String,
	barcode String,
	unitname String,
	inbuid String,
	producingarea String,
	adviceprice String,
	advicecost String,
	addrate String,
	brandid UInt32,
	categoryname String,
	mid_categoryid String,
	mid_categoryname String,
	h_categoryid String,
	h_categoryname String,
	create_time Date
)
engine = Join(ALL, INNER, buid, goodsid)
;

create table dw_hr.dw_shop_info
(
	shopid String,
	shopcode String,
	shopname String,
	address String,
	buid UInt8,
	inbuid UInt8,
	shopformid UInt8,
	shofortype UInt8,
	shopstatus UInt8,
	buname String,
	categorytreeid UInt8,
	payshopid String,
	regionid UInt64,
	provinces_regions String,
	propertype String,
	dctype UInt8,
	prov_name String,
	city_name String
)
engine = Log
;

create table dw_hr.dw_vender_info
(
	buid UInt8,
	venderid String,
	vendername String,
	vendercode String,
	parent_buid String,
	parent_venderid String,
	parent_vendername String
)
engine = Log
;

create table dw_hr.fct_rpt_dc_shop_sku_vender_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	buid UInt8,
	dc_id String,
	dctype UInt8,
	shop_id String,
	shopformid UInt8,
	categorytreeid UInt8,
	categoryid String,
	goodsid LowCardinality(String),
	logistics UInt8,
	venderid String,
	datasource UInt8,
	rpt_qty Decimal(18,4),
	rpt_boxes Decimal(18,4),
	rpt_cost Decimal(18,4),
	rpt_taxcost Decimal(18,4),
	stat_day_num String default formatDateTime(stat_day, '%F'),
	rt_zt_qty Decimal(18,4),
	rt_zt_cost Decimal(18,4),
	rt_zt_taxcost Decimal(18,4),
	rt_zt_boxes Decimal(18,4),
	rt_cc_qty Decimal(18,4),
	rt_cc_cost Decimal(18,4),
	rt_cc_taxcost Decimal(18,4),
	rt_cc_boxes Decimal(18,4),
	rt_zs_qty Decimal(18,4),
	rt_zs_cost Decimal(18,4),
	rt_zs_taxcost Decimal(18,4),
	rt_zs_boxes Decimal(18,4),
	rpt_zs_qty Decimal(18,4),
	rpt_zs_cost Decimal(18,4),
	rpt_zs_taxcost Decimal(18,4),
	rpt_zs_boxes Decimal(18,4),
	rpt_zt_qty Decimal(18,4),
	rpt_zt_cost Decimal(18,4),
	rpt_zt_taxcost Decimal(18,4),
	rpt_zt_boxes Decimal(18,4),
	rpt_cc_qty Decimal(18,4),
	rpt_cc_cost Decimal(18,4),
	rpt_cc_taxcost Decimal(18,4),
	rpt_cc_boxes Decimal(18,4)
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (stat_day, dc_id) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_rpt_dc_shop_sku_vender_mon
(
	stat_year UInt16,
	stat_month UInt32,
	buid UInt8,
	dc_id String,
	dctype UInt8,
	shop_id String,
	shopformid UInt8,
	categorytreeid UInt8,
	categoryid String,
	goodsid LowCardinality(String),
	logistics UInt8,
	venderid String,
	datasource UInt8,
	rpt_qty Decimal(18,4),
	rpt_boxes Decimal(18,4),
	rpt_cost Decimal(18,4),
	rpt_taxcost Decimal(18,4),
	rpt_zs_qty Decimal(18,4),
	rpt_zs_cost Decimal(18,4),
	rpt_zs_taxcost Decimal(18,4),
	rpt_zs_boxes Decimal(18,4),
	rpt_zt_qty Decimal(18,4),
	rpt_zt_cost Decimal(18,4),
	rpt_zt_taxcost Decimal(18,4),
	rpt_zt_boxes Decimal(18,4),
	rpt_cc_qty Decimal(18,4),
	rpt_cc_cost Decimal(18,4),
	rpt_cc_taxcost Decimal(18,4),
	rpt_cc_boxes Decimal(18,4)
)
engine = MergeTree PARTITION BY stat_month ORDER BY (stat_month, dc_id) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_rpt_dc_shop_vender_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	buid UInt8,
	dc_id String,
	venderid String,
	dctype UInt8,
	shop_id String,
	shopformid UInt8,
	logistics UInt8,
	datasource UInt8,
	rpt_qty Decimal(18,4),
	rpt_boxes Decimal(18,4),
	rpt_cost Decimal(18,4),
	rpt_taxcost Decimal(18,4),
	stat_day_num String default formatDateTime(stat_day, '%F'),
	rpt_zs_qty Decimal(18,4),
	rpt_zs_cost Decimal(18,4),
	rpt_zs_taxcost Decimal(18,4),
	rpt_zs_boxes Decimal(18,4),
	rpt_zt_qty Decimal(18,4),
	rpt_zt_cost Decimal(18,4),
	rpt_zt_taxcost Decimal(18,4),
	rpt_zt_boxes Decimal(18,4),
	rpt_cc_qty Decimal(18,4),
	rpt_cc_cost Decimal(18,4),
	rpt_cc_taxcost Decimal(18,4),
	rpt_cc_boxes Decimal(18,4)
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (stat_day, dc_id) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_rsl_dc_shop_vender_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	out_buid String,
	shop_id String,
	logistics UInt16,
	goodsid LowCardinality(String),
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
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (shop_id, venderid) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_rt_dc_shop_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	out_buid UInt8,
	out_shop_id String,
	in_shop_id LowCardinality(String),
	datasource UInt8,
	logistics UInt8,
	buntype UInt8,
	dctype UInt8,
	shopformid UInt8,
	rt_qty Decimal(18,4),
	rt_cost Decimal(18,4),
	rt_taxcost Decimal(18,4),
	rt_boxes Decimal(18,4),
	rt_shops LowCardinality(Nullable(String)),
	rt_drygood_qty Decimal(18,4),
	rt_drygood_cost Decimal(18,4),
	rt_drygood_boxes Decimal(18,4),
	rt_drygood_shops LowCardinality(Nullable(String)),
	rt_fresh_qty Decimal(18,4),
	rt_fresh_cost Decimal(18,4),
	rt_fresh_shops LowCardinality(Nullable(String)),
	rt_supshop_cost Decimal(18,4),
	rt_supshop_qty Decimal(18,4),
	rt_supshop_boxes Decimal(18,4),
	rt_supshop_shops LowCardinality(Nullable(String)),
	rt_smallshop_cost Decimal(18,4),
	rt_smallshop_qty Decimal(18,4),
	rt_smallshop_boxes Decimal(18,4),
	rt_smallshop_shops LowCardinality(Nullable(String)),
	rt_dc_cost Decimal(18,4),
	rt_dc_qty Decimal(18,4),
	rt_dc_boxes Decimal(18,4),
	rt_dc_shops LowCardinality(Nullable(String)),
	rt_drygood_supshop_cost Decimal(18,4),
	rt_drygood_supshop_qty Decimal(18,4),
	rt_drygood_supshop_boxes Decimal(18,4),
	rt_drygood_supshop_shops LowCardinality(Nullable(String)),
	rt_drygood_smallshop_cost Decimal(18,4),
	rt_drygood_smallshop_qty Decimal(18,4),
	rt_drygood_smallshop_boxes Decimal(18,4),
	rt_drygood_smallshop_shops LowCardinality(Nullable(String)),
	rt_drygood_dc_cost Decimal(18,4),
	rt_drygood_dc_qty Decimal(18,4),
	rt_drygood_dc_boxes Decimal(18,4),
	rt_drygood_dc_shops LowCardinality(Nullable(String)),
	rt_fresh_supshop_cost Decimal(18,4),
	rt_fresh_supshop_qty Decimal(18,4),
	rt_fresh_supshop_shops LowCardinality(Nullable(String)),
	rt_fresh_smallshop_cost Decimal(18,4),
	rt_fresh_smallshop_qty Decimal(18,4),
	rt_fresh_smallshop_shops LowCardinality(Nullable(String)),
	rt_fresh_dc_cost Decimal(18,4),
	rt_fresh_dc_qty Decimal(18,4),
	rt_fresh_dc_shops LowCardinality(Nullable(String)),
	stat_day_num String default formatDateTime(stat_day, '%F'),
	rt_zt_qty Decimal(18,4),
	rt_zt_cost Decimal(18,4),
	rt_zt_taxcost Decimal(18,4),
	rt_zt_boxes Decimal(18,4),
	rt_cc_qty Decimal(18,4),
	rt_cc_cost Decimal(18,4),
	rt_cc_taxcost Decimal(18,4),
	rt_cc_boxes Decimal(18,4)
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (stat_day, out_shop_id) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_rt_dc_shop_sku_vender_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	out_buid UInt8,
	out_shop_id String,
	in_shop_id LowCardinality(String),
	datasource UInt8,
	venderid String,
	categorytreeid UInt8,
	categoryid String,
	goodsid LowCardinality(String),
	logistics UInt8,
	buntype UInt8,
	dctype UInt8,
	shopformid UInt8,
	rt_qty Decimal(18,4),
	rt_cost Decimal(18,4),
	rt_taxcost Decimal(18,4),
	rt_boxes Decimal(18,4),
	rt_shops Nullable(String),
	rt_drygood_qty Decimal(18,4),
	rt_drygood_cost Decimal(18,4),
	rt_drygood_boxes Decimal(18,4),
	rt_drygood_shops LowCardinality(Nullable(String)),
	rt_fresh_qty Decimal(18,4),
	rt_fresh_cost Decimal(18,4),
	rt_fresh_shops LowCardinality(Nullable(String)),
	rt_supshop_cost Decimal(18,4),
	rt_supshop_qty Decimal(18,4),
	rt_supshop_boxes Decimal(18,4),
	rt_supshop_shops LowCardinality(Nullable(String)),
	rt_smallshop_cost Decimal(18,4),
	rt_smallshop_qty Decimal(18,4),
	rt_smallshop_boxes Decimal(18,4),
	rt_smallshop_shops LowCardinality(Nullable(String)),
	rt_dc_cost Decimal(18,4),
	rt_dc_qty Decimal(18,4),
	rt_dc_boxes Decimal(18,4),
	rt_dc_shops LowCardinality(Nullable(String)),
	rt_drygood_supshop_cost Decimal(18,4),
	rt_drygood_supshop_qty Decimal(18,4),
	rt_drygood_supshop_boxes Decimal(18,4),
	rt_drygood_supshop_shops LowCardinality(Nullable(String)),
	rt_drygood_smallshop_cost Decimal(18,4),
	rt_drygood_smallshop_qty Decimal(18,4),
	rt_drygood_smallshop_boxes Decimal(18,4),
	rt_drygood_smallshop_shops LowCardinality(Nullable(String)),
	rt_drygood_dc_cost Decimal(18,4),
	rt_drygood_dc_qty Decimal(18,4),
	rt_drygood_dc_boxes Decimal(18,4),
	rt_drygood_dc_shops LowCardinality(Nullable(String)),
	rt_fresh_supshop_cost Decimal(18,4),
	rt_fresh_supshop_qty Decimal(18,4),
	rt_fresh_supshop_shops LowCardinality(Nullable(String)),
	rt_fresh_smallshop_cost Decimal(18,4),
	rt_fresh_smallshop_qty Decimal(18,4),
	rt_fresh_smallshop_shops LowCardinality(Nullable(String)),
	rt_fresh_dc_cost Decimal(18,4),
	rt_fresh_dc_qty Decimal(18,4),
	rt_fresh_dc_shops LowCardinality(Nullable(String)),
	stat_day_num String default formatDateTime(stat_day, '%F'),
	rt_zt_qty Decimal(18,4),
	rt_zt_cost Decimal(18,4),
	rt_zt_taxcost Decimal(18,4),
	rt_zt_boxes Decimal(18,4),
	rt_cc_qty Decimal(18,4),
	rt_cc_cost Decimal(18,4),
	rt_cc_taxcost Decimal(18,4),
	rt_cc_boxes Decimal(18,4)
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (stat_day, out_shop_id) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_rt_dc_shop_sku_vender_day_ti
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	out_buid UInt8,
	categorytreeid UInt8,
	out_shop_id String,
	dctype UInt8,
	in_shop_id String,
	shopformid UInt8,
	goodsid String,
	category_id String,
	venderid String,
	logistics UInt8,
	buntype UInt8,
	datasource UInt8,
	rt_boxes Decimal(18,4),
	rt_qty Decimal(18,4),
	rt_cost Decimal(18,4),
	rt_taxcost Decimal(18,4)
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (out_buid, out_shop_id) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_rt_dc_shop_sku_vender_mon
(
	stat_year UInt16,
	stat_month UInt32,
	out_buid UInt8,
	out_shop_id String,
	in_shop_id LowCardinality(String),
	datasource UInt8,
	venderid String,
	categorytreeid UInt8,
	categoryid String,
	goodsid LowCardinality(String),
	logistics UInt8,
	buntype UInt8,
	dctype UInt8,
	shopformid UInt8,
	rt_qty Decimal(18,4),
	rt_cost Decimal(18,4),
	rt_taxcost Decimal(18,4),
	rt_boxes Decimal(18,4),
	rt_shops LowCardinality(Nullable(String)),
	rt_drygood_qty Decimal(18,4),
	rt_drygood_cost Decimal(18,4),
	rt_drygood_boxes Decimal(18,4),
	rt_drygood_shops LowCardinality(Nullable(String)),
	rt_fresh_qty Decimal(18,4),
	rt_fresh_cost Decimal(18,4),
	rt_fresh_shops LowCardinality(Nullable(String)),
	rt_supshop_cost Decimal(18,4),
	rt_supshop_qty Decimal(18,4),
	rt_supshop_boxes Decimal(18,4),
	rt_supshop_shops LowCardinality(Nullable(String)),
	rt_smallshop_cost Decimal(18,4),
	rt_smallshop_qty Decimal(18,4),
	rt_smallshop_boxes Decimal(18,4),
	rt_smallshop_shops LowCardinality(Nullable(String)),
	rt_dc_cost Decimal(18,4),
	rt_dc_qty Decimal(18,4),
	rt_dc_boxes Decimal(18,4),
	rt_dc_shops LowCardinality(Nullable(String)),
	rt_drygood_supshop_cost Decimal(18,4),
	rt_drygood_supshop_qty Decimal(18,4),
	rt_drygood_supshop_boxes Decimal(18,4),
	rt_drygood_supshop_shops LowCardinality(Nullable(String)),
	rt_drygood_smallshop_cost Decimal(18,4),
	rt_drygood_smallshop_qty Decimal(18,4),
	rt_drygood_smallshop_boxes Decimal(18,4),
	rt_drygood_smallshop_shops LowCardinality(Nullable(String)),
	rt_drygood_dc_cost Decimal(18,4),
	rt_drygood_dc_qty Decimal(18,4),
	rt_drygood_dc_boxes Decimal(18,4),
	rt_drygood_dc_shops LowCardinality(Nullable(String)),
	rt_fresh_supshop_cost Decimal(18,4),
	rt_fresh_supshop_qty Decimal(18,4),
	rt_fresh_supshop_shops LowCardinality(Nullable(String)),
	rt_fresh_smallshop_cost Decimal(18,4),
	rt_fresh_smallshop_qty Decimal(18,4),
	rt_fresh_smallshop_shops LowCardinality(Nullable(String)),
	rt_fresh_dc_cost Decimal(18,4),
	rt_fresh_dc_qty Decimal(18,4),
	rt_fresh_dc_shops LowCardinality(Nullable(String)),
	rt_zt_qty Decimal(18,4),
	rt_zt_cost Decimal(18,4),
	rt_zt_taxcost Decimal(18,4),
	rt_zt_boxes Decimal(18,4),
	rt_cc_qty Decimal(18,4),
	rt_cc_cost Decimal(18,4),
	rt_cc_taxcost Decimal(18,4),
	rt_cc_boxes Decimal(18,4)
)
engine = MergeTree PARTITION BY stat_month ORDER BY (stat_month, out_shop_id) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_sale_shop_sku_day
(
	stat_year UInt16,
	stat_month UInt32,
	stat_day Date,
	buid UInt8,
	shopid LowCardinality(String),
	placeid String,
	brandid UInt32,
	familyid UInt64,
	categorytreeid UInt8,
	categoryid String,
	goodsid LowCardinality(String),
	shelfid String,
	venderid String,
	payflag UInt8,
	paytypeid UInt16,
	costtaxrate Decimal(18,4),
	saletaxrate Decimal(18,4),
	logistics UInt8,
	disctype String,
	pickflag UInt8,
	saleqty Decimal(18,4),
	salevalue Decimal(18,4),
	discvalue Decimal(18,4),
	saletax Decimal(18,4),
	salecostvalue Decimal(18,4),
	costdisc Decimal(18,4),
	costtax Decimal(18,4),
	discticketvalue Decimal(18,4),
	stat_day_num String default formatDateTime(stat_day, '%F')
)
engine = MergeTree PARTITION BY toYYYYMM(stat_day) ORDER BY (stat_day, shopid, goodsid) SETTINGS index_granularity = 8192
;

create table dw_hr.fct_sale_shop_sku_mon
(
	stat_year UInt16,
	stat_month UInt32,
	buid UInt8,
	shopid LowCardinality(String),
	placeid String,
	brandid UInt32,
	familyid UInt64,
	categorytreeid UInt8,
	goodsid LowCardinality(String),
	shelfid String,
	venderid String,
	payflag UInt8,
	paytypeid UInt16,
	logistics UInt8,
	saleqty Decimal(18,4),
	salevalue Decimal(18,4),
	discvalue Decimal(18,4),
	salecostvalue Decimal(18,4)
)
engine = MergeTree PARTITION BY stat_month ORDER BY (shopid, goodsid) SETTINGS index_granularity = 8192
;

