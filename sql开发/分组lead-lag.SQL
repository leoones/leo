select out_shop_id,
       months,
       costs,
       lag_s,
       lead_s
from
  (select out_shop_id,
         groupArray(stat_month) as stat_month_lst,
         groupArray(m_rt_cost) as m_rt_cost_lst,
         arrayEnumerate(m_rt_cost_lst) as index,
         length(index) as len,
         arrayMap(x -> multiIf(x=1,m_rt_cost_lst[x], m_rt_cost_lst[x-1]),  index) as lag,
         arrayMap(x -> multiIf(x=len,m_rt_cost_lst[x], m_rt_cost_lst[x+1]),index) as lead
   from
  (select frdssvm.out_shop_id,
         frdssvm.stat_month,
         sum(frdssvm.rt_cost) as m_rt_cost
    from dw_hr.fct_rt_dc_shop_sku_vender_mon frdssvm
  group by frdssvm.out_shop_id,
           frdssvm.stat_month
  order by frdssvm.out_shop_id,
           frdssvm.stat_month asc
      ) xxx
  group by out_shop_id
      ) tmp
array join stat_month_lst as  months,
           m_rt_cost_lst as costs,
           lag as lag_s,
           lead as lead_s
