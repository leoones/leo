-------------------------------------------------------------- 配送 ---------------------------------------------
ALTER TABLE dw_hr.fct_rt_dc_shop_sku_vender_day UPDATE rt_zt_qty =  multiIf(logistics = 2, rt_qty, 0),
           rt_zt_cost = multiIf(logistics = 2, rt_cost, 0),
           rt_zt_taxcost = multiIf(logistics = 2, rt_taxcost, 0),
           rt_zt_boxes = multiIf(logistics = 2, rt_boxes, 0),

           rt_cc_qty =  multiIf(logistics <> 2, rt_qty, 0),
           rt_cc_cost = multiIf(logistics <> 2, rt_cost, 0),
           rt_cc_taxcost = multiIf(logistics <> 2, rt_taxcost, 0),
           rt_cc_boxes = multiIf(logistics <> 2, rt_boxes, 0)
where 1 = 1;

-------------------------------------------------------------- 验收 ---------------------------------------------
ALTER TABLE dw_hr.fct_rpt_dc_shop_vender_day UPDATE
           rpt_zs_qty =  multiIf(logistics = 1, rpt_qty, 0),
           rpt_zs_cost = multiIf(logistics = 1, rpt_cost, 0),
           rpt_zs_taxcost = multiIf(logistics = 1, rpt_taxcost, 0),
           rpt_zs_boxes = multiIf(logistics = 1, rpt_boxes, 0),

           rpt_zt_qty =  multiIf(logistics = 2, rpt_qty, 0),
           rpt_zt_cost = multiIf(logistics = 2, rpt_cost, 0),
           rpt_zt_taxcost = multiIf(logistics = 2, rpt_taxcost, 0),
           rpt_zt_boxes = multiIf(logistics = 2, rpt_boxes, 0),

           rpt_cc_qty =  multiIf(logistics = 3, rpt_qty, 0),
           rpt_cc_cost = multiIf(logistics = 3, rpt_cost, 0),
           rpt_cc_taxcost = multiIf(logistics = 3, rpt_taxcost, 0),
           rpt_cc_boxes = multiIf(logistics = 3, rpt_boxes, 0)
where 1 = 1;

