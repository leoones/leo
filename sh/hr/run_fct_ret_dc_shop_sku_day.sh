insert into dw_hr.fct_ret_dc_shop_sku_day
    (stat_year,
     stat_mon,
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
    where ret.outcheckdate >= toDate('2018-01-01')
      and ret.outcheckdate  < addMonths(toDate('2018-01-01'), 1)
    ) dd
  all left  join dw_hr.dw_good_info dgi on dd.out_buid = dgi.buid and dd.goodsid = toUInt32(dgi.goodsid);
