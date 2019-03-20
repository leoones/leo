#!/bin/bash
startDate=$1
endDate=$2
while [ $startDate -le $endDate ];
do
stat_mon_first=$(date -d "$startDate+0days" +%Y-%m-%d)
stat_mon=$(date -d "$startDate+0days" +%Y%m)
sql="
   alter table dw_hr.fct_ret_dc_shop_sku_day drop partition($stat_mon);
   insert into dw_hr.fct_ret_dc_shop_sku_day
    (stat_year,
     stat_month,
     stat_day,
     outshopid,
     out_buid,
     inshopid,
     in_buid,
     goodsid,
     categorytreeid,
     categoryid,
     ret_qty,
     ret_boxes,
     ret_cost,
     ret_taxcost)
  select stat_year,
          stat_mon,
          outcheckdate,
          outshopid,
          out_buid,
          inshopid,
          in_buid,
          goodsid,
          dgi.categorytreeid,
          dgi.categoryid,
          ret_qty,
          ret_boxes,
          ret_cost,
          ret_taxcost
  from
    (select toYear(ret.outcheckdate) as stat_year,
            toYYYYMM(ret.outcheckdate) as stat_mon,
            outcheckdate,
            ret.outshopid,
            dictGetUInt8('dw_shop', 'buid', tuple(outshopid)) as out_buid,
            ret.inshopid,
            dictGetUInt8('dw_shop', 'buid', tuple(inshopid)) as in_buid,
            ret.goodsid,
            ret.outqty as ret_qty,
            ret.outqty / ret.pknum as ret_boxes,
            round(ret.cost * ret.outqty, 4) as ret_cost,
            round((ret.cost  * ret.outqty)  / (1+ret.costtaxrate/100),4) as ret_taxcost
     from ods_hr.ration_ret_l ret
    where ret.outcheckdate >= toDate('$stat_mon_first')
      and ret.outcheckdate  < addMonths(toDate('$stat_mon_first'), 1)
    ) dd
  all left  join dw_hr.dw_good_info dgi on dd.out_buid = dgi.buid and dd.goodsid = toUInt32(dgi.goodsid);"
clickhouse-client -h $CLICKHOUSE_HOST --user=default --password=$CLICKHOUSE_PASSWORD -m -n --query="$sql"
echo "----------------- $stat_mon Complete---------------------"
startDate=$(date -d "$startDate+1months" +%Y%m%d)
done
