-----------------------------------------------  WMS,NWMS商品信息 -----------------------------------------------
create table ods_hr.wms_nwms_goods_info(
OWNER_NO String,
LOCNO String,
dc_id String,
ARTICLE_NO String,
goodsid String,
PACKING_QTY UInt8,
PACKING_UNIT,
A_LENGTH Decimal(18, 2),
A_WIDTH Decimal(18, 2),
A_HEIGHT Decimal(18, 2),
PACKING_WEIGHT Decimal(18, 2),
MEASURE_MODE UInt8
)
