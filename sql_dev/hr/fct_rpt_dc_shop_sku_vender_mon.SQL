alter table dw_hr.fct_rpt_dc_shop_sku_vender_mon drop partition($stat_mon);

insert into dw_hr.fct_rpt_dc_shop_sku_vender_mon
(stat_year,
 stat_month,
 buid,
 dc_shop_id,
 dctype,
 shop_id,
 shopformid,
 categorytreeid,
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
		 dc_shop_id,
		 dctype,
		 shop_id,
		 shopformid,
		 categorytreeid,
		 categoryid,
		 logistics,
		 venderid,
		 datasource,
		 sum(rpt_qty),
		 sum(rpt_boxes),
		 sum(rpt_cost),
		 sum(rpt_taxcost)
from dw_hr.fct_rpt_dc_shop_sku_vender_day
where stat_day >= toDate('$stat_mon_first')
  and stat_day < addMonths(toDate('$stat_mon_first'), 1)
 group by 
		 stat_year,
		 stat_month,
		 buid,
		 dc_shop_id,
		 dctype,
		 shop_id,
		 shopformid,
		 categorytreeid,
		 categoryid,
		 logistics,
		 venderid,
		 datasource;