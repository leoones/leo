#!/bin/bash
startDate=$1
endDate=$2
while [ $startDate -le $endDate ];
do
stat_mon_first=$(date -d "$startDate+0days" +%Y-%m-%d)
stat_mon=$(date -d "$startDate+0days" +%Y%m)
sql="
alter table dw_hr.fct_rsl_dc_shop_vender_day drop partition($stat_mon);
insert into dw_hr.fct_rsl_dc_shop_vender_day
select toYear(check_date) as stat_year,
      toYYYYMM(check_date) as stat_month,
       check_date as stat_day,
       bu ,
       rsl.shop_id,
			 rsl.logistics,
			 rsl.categoryid,
			 rsl.venderid,
       multiIf( ( trunc(toUInt32(rsl.categoryid) / 10000) as dc_flag) in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and bu <>19 , 2,
                dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and bu <>19, 1,
                dc_flag  not in (1,2,3,4,5,6,7,8,9) and bu = 19, 1,
                dc_flag  in (1,2,3,4,5,6,7,8,9) and bu = 19, 2,
                 0 ) as dc_type,

       multiIf((dictGetUInt8('dw_shop','shopformid', tuple(shop_id))  as shop_type) in (1,6, 11), 1,
							 shop_type in (21, 51) , 2,
					     shop_type in (9), 5,
					     0) as rsl_shopformid,

       sum(rsl.cost) as rsl_cost,
       sum(rsl.check_box_num) as rsl_boxes,
       sum(rsl.check_scatter_num) as rsl_qty,

			 sum(multiIf(dc_type = 1, rsl.cost, 0)) as rpt_drygood_cost,
			 sum(multiIf(dc_type = 1, rsl.check_box_num, 0)) as rpt_drygood_boxes,
       sum(multiIf(dc_type = 1, rsl.check_scatter_num, 0)) as rpt_drygood_qty,

       sum(multiIf(dc_type = 2, rsl.cost, 0)) as rpt_fresh_cost,
			 sum(multiIf(dc_type = 2, rsl.check_box_num, 0)) as rpt_fresh_boxes,
       sum(multiIf(dc_type = 2, rsl.check_scatter_num, 0)) as rpt_fresh_qty,

       sum(multiIf(dc_type = 1 and rsl_shopformid=1, rsl.cost, 0)) as rpt_drygood_supshop_cost,
			 sum(multiIf(dc_type = 1 and rsl_shopformid=1, rsl.check_box_num, 0)) as rpt_drygood_supshop_boxes,
       sum(multiIf(dc_type = 1 and rsl_shopformid=1, rsl.check_scatter_num, 0)) as rpt_drygood_supshop_qty,

       sum(multiIf(dc_type = 1 and rsl_shopformid=2, rsl.cost, 0)) as rpt_drygood_smallshop_cost,
       sum(multiIf(dc_type = 1 and rsl_shopformid=2, rsl.check_box_num, 0)) as rpt_drygood_smallshop_boxes,
       sum(multiIf(dc_type = 1 and rsl_shopformid=2, rsl.check_scatter_num, 0)) as rpt_drygood_smallshop_qty,

       sum(multiIf(dc_type = 2 and rsl_shopformid=1, rsl.cost, 0)) as rpt_fresh_supshop_cost,
			 sum(multiIf(dc_type = 2 and rsl_shopformid=1, rsl.check_box_num, 0)) as rpt_fresh_supshop_boxes,
       sum(multiIf(dc_type = 2 and rsl_shopformid=1, rsl.check_scatter_num, 0)) as rpt_fresh_supshop_qty,

       sum(multiIf(dc_type = 2 and rsl_shopformid=2, rsl.cost, 0)) as rpt_fresh_smallshop_cost,
			 sum(multiIf(dc_type = 2 and rsl_shopformid=2, rsl.check_box_num, 0)) as rpt_fresh_smallshop_boxes,
       sum(multiIf(dc_type = 2 and rsl_shopformid=2, rsl.check_scatter_num, 0)) as rpt_fresh_smallshop_qty
  from ods_hr.receipt_store_l rsl
 where rsl.check_date >= toDate('$stat_mon_first')
   and rsl.check_date  < addMonths(toDate('$stat_mon_first'), 1)
group by stat_year,
         stat_month,
         stat_day,
         bu,
         rsl.shop_id,
			   logistics,
			   categoryid,
			   venderid,
         dc_type,
         rsl_shopformid;"
clickhouse-client -h $CLICKHOUSE_HOST --user=default --password=$CLICKHOUSE_PASSWORD -m -n --query="$sql"
echo "----------------- $stat_mon Complete---------------------"
startDate=$(date -d "$startDate+1months" +%Y%m%d)
done

