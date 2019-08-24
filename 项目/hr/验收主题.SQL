----------------------------------------------  fct_rpt_dc_shop_sku_vender_day  ----------------------------------------------
alter table dw_hr.fct_rpt_dc_shop_sku_vender_day
delete where stat_day >= toDate('${G_DAY}') and stat_day < addDays(toDate('${G_DAY}'), 1);
insert into dw_hr.fct_rpt_dc_shop_sku_vender_day
        (  stat_year,
            stat_month,
            stat_day,
            buid,
            dc_id,
            dctype,
            shop_id,
            shopformid,
            categorytreeid,
            categoryid,
		       goodsid,
            logistics,
            venderid,
            datasource,
            rpt_qty,
            rpt_boxes,
            rpt_cost,
            rpt_taxcost
        )
        select  toYear(rpt.check_date) as stat_year,
                toYYYYMM(rpt.check_date)  as stat_month,
                rpt.check_date as stat_day,
                dictGetUInt8('dw_shop', 'buid', tuple(dc_shop_id)) as rpt_buid,
                rpt.dc_shop_id as dc_shop_id,
                dictGetUInt8('dw_shop', 'dctype', tuple(dc_shop_id)) as  dctype,

                rpt.order_dc_shop_id as order_shopid,
                dictGetUInt8('dw_shop', 'shofortype', tuple(order_dc_shop_id)) as shopformid,
                dictGetUInt8('dw_buinfo', 'categorytreeid', toUInt64(rpt_buid)) as categorytreeid,
                dictGetString('dw_good_info', 'categoryid', tuple(rpt_buid, goodsid)) as categoryid,
			          rpt.goodsid,
                multiIf(rpt.logistics=1, 3, 2) as logistics_new,
                rpt.venderid,
                case when rpt_buid = 19 then 4 else 1 end datasource,
                sum( rpt.qty) as rpt_qty,
                toDecimal128(sum(case when rpt_buid = 19
                    then multiIf(( trunc(toUInt64OrZero(categoryid)/10000) as d) = 53 , (rpt.qty / rpt.pknumber as check_box_num) / 50,
                                    d =  81 , 0, check_box_num )
                    else multiIf( d = 12 , check_box_num / 50,
                        d =  59 , 0, check_box_num )
                    end), 4)  as  rpt_boxes,
                sum(rpt.costvalue) as rpt_cost,
                sum(rpt.costtaxvalue) as rpt_taxcost
        from ods_hr.receipt_l rpt
        where rpt.check_date >= toDate('${G_DAY}')
          and rpt.check_date  < addDays(toDate('${G_DAY}'), 1)
         and (rpt.dc_shop_id <> '995045' and rpt.DATASOURCE <> 'crv')
        group by stat_year, stat_month, stat_day, rpt_buid,
                  dc_shop_id, order_shopid, dctype,dc_shop_id, shopformid, categorytreeid,
                 categoryid,rpt.goodsid, logistics_new, venderid, datasource
        union all
        select toYear(xct.check_date) as stat_year,
            toYYYYMM(xct.check_date)  as stat_month,
            xct.check_date as stat_day,
            21 as buid,
            xct.shop_id as dc_shop_id,
            multiIf((trunc(toUInt64(xct.category_id) / 10000) as c ) >=212 , 1, c < 212 , 2, 0) as dctype,
            xct.shop_id as order_shopid,
            2 as shopformid,
            21 AS categorytreeid,
            xct.category_id,
				    xct.goods_code,
            multiIf(xct.logistics = 'G', 1, xct.logistics IN ('T', 'F'), 2, 0) as logistics_id,
            '' as venderid,
            2 as datasource,
            toInt32(sum(xct.check_scatter_num)) as rpt_qty,
            sum(multiIf(c = 212, xct.check_box_num / 50, c >212, xct.check_box_num, 0)) as rpt_boxes,
            sum(xct.cost * xct.check_scatter_num) as rpt_cost,
            0 as rpt_taxcost
        from ods_hr.xg_check_t xct
        where xct.logistics IN ('T', 'G', 'F')
        and xct.check_date >= toDate('${G_DAY}')
        and xct.check_date  < addDays(toDate('${G_DAY}'), 1)
        group by stat_year, stat_month, stat_day, dc_shop_id,
                 order_shopid,dctype,category_id, goods_code, logistics_id

        union all
        select toYear(sct.check_date) as stat_year,
            toYYYYMM(sct.check_date)  as stat_month,
            sct.check_date as stat_day,
            17 as buid,
            sct.shop_id as dc_shop_id,
            dictGetUInt8('dw_shop', 'dctype', tuple(dc_shop_id)) as  dctype,
            sct.shop_id as order_shopid,
            dictGetUInt8('dw_shop', 'shofortype', tuple(shopid)) as shopformid,
            17 AS categorytreeid,
            category_id,
				    sct.goods_code,
            multiIf(sct.logistics in ('3调整', '0存储'), 3, sct.logistics in ('1直流'), 2, 0) as logistics_id,
            sct.venderid,
            3 as datasource,
            toInt32(sum(sct.check_scatter_num)) as rpt_qty,
            sum(multiIf( ( trunc(toUInt32(sct.category_id) / 10000) as d ) = 212, sct.check_box_num / 50,
                        d<>212 and sct.category_id not in('060101', '060201'), sct.check_box_num, 0 )) as rpt_boxes,
            sum(sct.check_taxamount) as rpt_cost,
            0 as rpt_taxcost
        from ods_hr.sg_check_t sct
        all inner join ods_hr.sg_shop ss on sct.shop_id = ss.shopid
        where sct.check_date >= toDate('${G_DAY}')
        and sct.check_date   <  addDays(toDate('${G_DAY}'), 1)
        and sct.logistics != '2退货'
        group by stat_year, stat_month, stat_day, dc_shop_id, dctype, dc_shop_id, order_shopid,
                shopformid, category_id, goods_code, logistics_id, venderid;


----------------------------------------------  fct_rpt_dc_shop_vender_day  ----------------------------------------------
alter table dw_hr.fct_rpt_dc_shop_vender_day
delete where stat_day >= toDate('${G_DAY}') and stat_day < addDays(toDate('${G_DAY}'), 1);
insert into dw_hr.fct_rpt_dc_shop_vender_day
(
	stat_year ,
	stat_month ,
	stat_day ,
	buid ,
	dc_id ,
    venderid ,
	dctype ,
	shop_id ,
	shopformid ,
	logistics ,
	datasource ,
	rpt_qty ,
	rpt_boxes ,
	rpt_cost ,
	rpt_taxcost
)
select 	stat_year ,
	stat_month ,
	stat_day ,
	buid ,
	dc_id ,
    venderid ,
	dctype ,
	shop_id ,
	shopformid ,
	logistics ,
	datasource ,
	sum(rpt_qty) ,
	sum(rpt_boxes) ,
	sum(rpt_cost) ,
	sum(rpt_taxcost)
  from dw_hr.fct_rpt_dc_shop_sku_vender_day frdssvd
where frdssvd.stat_day >= toDate('${G_DAY}')
  and frdssvd.stat_day  < addDays(toDate('${G_DAY}'), 1)
group by stat_year ,
	stat_month ,
	stat_day ,
	buid ,
	dc_id ,
  venderid ,
	dctype ,
	shop_id ,
	shopformid ,
	logistics ,
	datasource ;
  
  
  
----------------------------------------------  fct_rpt_dc_shop_sku_vender_mon  ----------------------------------------------
  alter table dw_hr.fct_rpt_dc_shop_sku_vender_mon
  drop partition ${G_MONTH};

insert into dw_hr.fct_rpt_dc_shop_sku_vender_mon
(stat_year,
 stat_month,
 buid,
 dc_id,
 dctype,
 shop_id,
 shopformid,
 categorytreeid,
 goodsid,
 categoryid,
 logistics,
 venderid,
 datasource,
 rpt_qty,
 rpt_boxes,
 rpt_cost,
 rpt_taxcost
)
select stat_year,
		 stat_month,
		 buid,
		 dc_id,
		 dctype,
		 shop_id,
		 shopformid,
		 categorytreeid,
		 goodsid,
		 categoryid,
		 logistics,
		 venderid,
		 datasource,
		 sum(rpt_qty),
		 sum(rpt_boxes),
		 sum(rpt_cost),
		 sum(rpt_taxcost)
from dw_hr.fct_rpt_dc_shop_sku_vender_day
where stat_month = ${G_MONTH}
 group by
		 stat_year,
		 stat_month,
		 buid,
		 dc_id,
		 dctype,
		 shop_id,
		 shopformid,
		 categorytreeid,
		 goodsid,
		 categoryid,
		 logistics,
		 venderid,
		 datasource;
