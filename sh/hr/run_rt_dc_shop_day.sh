#!/bin/bash
data_path='/data/'
startDate=20160101
endDate=20181231
while [ $startDate -le $endDate ];
do
stat_mon_first=$(date -d "$startDate+0days" +%Y-%m-%d)
stat_mon=$(date -d "$startDate+0days" +%Y%m)
sql="
	 alter table dw_hr.fct_rt_dc_shop_day drop partition($stat_mon);
	 	  
	 insert into dw_hr.fct_rt_dc_shop_day
			  (
				stat_year ,
				stat_month ,
				stat_day ,
				out_buid ,
				out_shop_id ,
				in_shop_id ,
				datasource ,
				logistics ,
				buntype ,
				dctype ,
				shopformid ,
				rt_qty ,
				rt_cost ,
				rt_taxcost ,
				rt_shops ,
				rt_drygood_qty ,
				rt_drygood_cost ,
				rt_drygood_boxes ,
				rt_drygood_shops ,
				rt_fresh_qty ,
				rt_fresh_cost ,
				rt_fresh_shops ,
				rt_supshop_cost ,
				rt_supshop_qty ,
				rt_supshop_shops ,
				rt_smallshop_cost ,
				rt_smallshop_qty ,
				rt_smallshop_shops ,
				rt_dc_cost ,
				rt_dc_qty ,
				rt_drygood_supshop_cost ,
				rt_drygood_supshop_qty ,
				rt_drygood_supshop_boxes ,
				rt_drygood_supshop_shops ,
				rt_drygood_smallshop_cost ,
				rt_drygood_smallshop_qty ,
				rt_drygood_smallshop_boxes ,
				rt_drygood_smallshop_shops ,
				rt_drygood_dc_cost ,
				rt_drygood_dc_qty ,
				rt_drygood_dc_boxes ,
				rt_drygood_dc_shops ,
				rt_fresh_supshop_cost ,
				rt_fresh_supshop_qty ,
				rt_fresh_supshop_shops ,
				rt_fresh_smallshop_cost ,
				rt_fresh_smallshop_qty ,
				rt_fresh_smallshop_shops ,
				rt_fresh_dc_cost ,
				rt_fresh_dc_qty 
			)
			 select 
					  stat_year ,
						stat_month ,
						stat_day ,
						out_buid ,
						out_shop_id ,
						in_shop_id ,
						datasource ,
						logistics ,
						buntype ,
						dctype ,
						shopformid ,
						sum(rt_qty) ,
						sum(rt_cost) ,
						sum(rt_taxcost) ,
						max(rt_shops) ,
						sum(rt_drygood_qty) ,
						sum(rt_drygood_cost) ,
						sum(rt_drygood_boxes) ,
						max(rt_drygood_shops) ,
						sum(rt_fresh_qty) ,
						sum(rt_fresh_cost) ,
						max(rt_fresh_shops) ,
						sum(rt_supshop_cost) ,
						sum(rt_supshop_qty) ,
						max(rt_supshop_shops) ,
						sum(rt_smallshop_cost) ,
						sum(rt_smallshop_qty) ,
						max(rt_smallshop_shops) ,
						sum(rt_dc_cost) ,
						sum(rt_dc_qty) ,
						sum(rt_drygood_supshop_cost) ,
						sum(rt_drygood_supshop_qty) ,
						sum(rt_drygood_supshop_boxes) ,
						max(rt_drygood_supshop_shops) ,
						sum(rt_drygood_smallshop_cost) ,
						sum(rt_drygood_smallshop_qty) ,
						sum(rt_drygood_smallshop_boxes) ,
						max(rt_drygood_smallshop_shops) ,
						sum(rt_drygood_dc_cost) ,
						sum(rt_drygood_dc_qty) ,
						sum(rt_drygood_dc_boxes) ,
						max(rt_drygood_dc_shops) ,
						sum(rt_fresh_supshop_cost) ,
						sum(rt_fresh_supshop_qty) ,
						max(rt_fresh_supshop_shops) ,
						sum(rt_fresh_smallshop_cost) ,
						sum(rt_fresh_smallshop_qty) ,
						max(rt_fresh_smallshop_shops) ,
						sum(rt_fresh_dc_cost ),
						sum(rt_fresh_dc_qty) 
			 from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
			 where stat_day >= toDate('$stat_mon_first')
			   and stat_day  < addMonths(toDate('$stat_mon_first'),1)
			 group by stat_year ,
						stat_month ,
						stat_day ,
						out_buid ,
						out_shop_id ,
						in_shop_id ,
						datasource ,
						logistics ,
						buntype ,
						dctype ,
						shopformid;
"
clickhouse-client -h 192.168.89.102 --user=default --password=sMNl+f/n -m -n --query="$sql"
echo "----------------- $stat_mon Complete---------------------"
startDate=$(date -d "$startDate+1months" +%Y%m%d)
done

