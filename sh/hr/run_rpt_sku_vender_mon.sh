#!/bin/bash
data_path='/data/'
startDate=20160101
endDate=20181231
while [ $startDate -le $endDate ];
do
stat_mon_first=$(date -d "$startDate+0days" +%Y-%m-%d)
stat_mon=$(date -d "$startDate+0days" +%Y%m)
sql="
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
"
clickhouse-client -h 192.168.89.102 --user=default --password=sMNl+f/n -m -n --query="$sql"
echo "----------------- $stat_mon Complete---------------------"
startDate=$(date -d "$startDate+1months" +%Y%m%d)
done

