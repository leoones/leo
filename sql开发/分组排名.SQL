select out_shop_id,
       out_shopname,
       in_shops,
       in_shopnames,
       costs,
       rnk
  from
  (select tmp1.out_shop_id,
         tmp1.out_shopname,
         groupArray(tmp1.in_shop_id) as in_shop_id_lst,
         groupArray(tmp1.in_shopname) as in_shopname_lst,
         groupArray(tmp1.total_rt_cost) as total_rt_cost_lst,
         arrayEnumerate(in_shop_id_lst) as rank
   from
      (select frdssvm.out_shop_id,
             frdssvm.out_shopname,
             frdssvm.in_shop_id,
             frdssvm.in_shopname,
             sum(frdssvm.rt_cost) as total_rt_cost
       from dw_hr.vw_fct_rt_dc_shop_sku_vender_mon frdssvm
      where frdssvm.stat_month >= 201801
       and frdssvm.stat_month <=  201806
      group by frdssvm.out_shop_id, frdssvm.out_shopname,frdssvm.in_shop_id, frdssvm.in_shopname
      order by frdssvm.out_shop_id, frdssvm.out_shopname,total_rt_cost asc
          ) tmp1
  group by tmp1.out_shop_id,
         tmp1.out_shopname
      ) tmp2
array join in_shop_id_lst as in_shops, in_shopname_lst as in_shopnames, rank as rnk, total_rt_cost_lst as costs
