alter table dw_hr.fct_rsl_dc_shop_vender_day drop partition(toYYYYMM(toDate('{0}')))$$

insert into dw_hr.fct_rsl_dc_shop_vender_day
select toYear(check_date) as stat_year,
      toYYYYMM(check_date) as stat_month,
       check_date as stat_day,
       bu,
       rsl.shop_id,
			 toUInt8(rsl.logistics) as r_logistics,
			 rsl.categoryid,
			 rsl.venderid,
       multiIf(( trunc(rsl.categoryid / 10000) as dc_flag) in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729), 2, 1 ) as dc_type,

       multiIf((dictGetUInt16('dim_shop','shopformid', tuple(shop_id)) as shop_type) in (1,6, 11), 1,
							 shop_type in (21, 51) , 2,
					     shop_type in (21, 51), 5,
					     0) as shopformid,

       sum(rsl.cost) as rsl_cost,
       sum(rsl.check_box_num) as rsl_boxes,
       sum(rsl.check_scatter_num) as rsl_qty,

			 sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729), rsl.cost, 0)) as rpt_drygood_cost,
			 sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729), rsl.check_box_num, 0)) as rpt_drygood_boxes,
       sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729), rsl.check_scatter_num, 0)) as rpt_drygood_qty,

       sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729), rsl.cost, 0)) as rpt_fresh_cost,
			 sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729), rsl.check_box_num, 0)) as rpt_fresh_boxes,
       sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729), rsl.check_scatter_num, 0)) as rpt_fresh_qty,

       sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=1, rsl.cost, 0)) as rpt_drygood_supshop_cost,
			 sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=1, rsl.check_box_num, 0)) as rpt_drygood_supshop_boxes,
       sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=1, rsl.check_scatter_num, 0)) as rpt_drygood_supshop_qty,

       sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=2, rsl.cost, 0)) as rpt_drygood_smallshop_cost,
       sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=2, rsl.check_box_num, 0)) as rpt_drygood_smallshop_boxes,
       sum(multiIf(dc_flag not in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=2, rsl.check_scatter_num, 0)) as rpt_drygood_smallshop_qty,

       sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=1, rsl.cost, 0)) as rpt_fresh_supshop_cost,
			 sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=1, rsl.check_box_num, 0)) as rpt_fresh_supshop_boxes,
       sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=1, rsl.check_scatter_num, 0)) as rpt_fresh_supshop_qty,

       sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=2, rsl.cost, 0)) as rpt_fresh_smallshop_cost,
			 sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=2, rsl.check_box_num, 0)) as rpt_fresh_smallshop_boxes,
       sum(multiIf(dc_flag  in (1101, 1102, 1103, 1104, 1205, 1206, 1207, 1208,
					      2110, 2111, 1227, 1228, 2729) and shopformid=2, rsl.check_scatter_num, 0)) as rpt_fresh_smallshop_qty
  from ods_hr.receipt_store_l rsl
 where rsl.check_date >= toDate('{0}')
   and rsl.check_date  < addMonths(toDate('{0}'), 1)
group by stat_year,
         stat_month,
         stat_day,
         bu,
         rsl.shop_id,
			   r_logistics,
			   rsl.categoryid,
			   rsl.venderid,
         dc_type,
         shopformid$$