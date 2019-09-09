select out_shop_id,
       out_shopname,
       months,
       month_cost,
       month_cum_cost
  from
  (select tmp1.out_shop_id,
         tmp1.out_shopname,
         groupArray(tmp1.stat_month) as month_lst,
         groupArray(tmp1.total_rt_cost) as rt_cost_lst,
         arrayMap(x->toInt64(x),rt_cost_lst) as rt_cost_lst_new,
         arrayCumSum(rt_cost_lst_new) as cum_cost_lst
   from
      (select frdssvm.out_shop_id,
             frdssvm.out_shopname,
             frdssvm.stat_month,
             sum(frdssvm.rt_cost) as total_rt_cost
       from dw_hr.vw_fct_rt_dc_shop_sku_vender_mon frdssvm
      where frdssvm.stat_month >= 201801
       and frdssvm.stat_month <=  201812
      group by  frdssvm.out_shop_id, frdssvm.out_shopname,frdssvm.stat_month
      order by frdssvm.out_shop_id, frdssvm.out_shopname,frdssvm.stat_month
          ) tmp1
  group by tmp1.out_shop_id,
         tmp1.out_shopname
      ) tmp2
array join month_lst as months, rt_cost_lst as month_cost, cum_cost_lst as month_cum_cost;
