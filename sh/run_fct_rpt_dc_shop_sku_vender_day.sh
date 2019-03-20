#!/bin/bash
startDate=$1
endDate=$2
while [ $startDate -le $endDate ];
do
stat_mon_first=$(date -d "$startDate+0days" +%Y-%m-%d)
stat_mon=$(date -d "$startDate+0days" +%Y%m)
sql="
   alter table dw_hr.fct_rpt_dc_shop_sku_vender_day drop partition($stat_mon);

   insert into dw_hr.fct_rpt_dc_shop_sku_vender_day
        (  stat_year,
            stat_month,
            stat_day,
            buid,
            dc_shop_id,
            dctype,
            shop_id,
            shopformid,
            goodsid,
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
         select  toYear(rpt.check_date) as stat_year,
                toYYYYMM(rpt.check_date)  as stat_month,
                rpt.check_date as stat_day,
                dictGetUInt8('dw_shop', 'buid', tuple(dc)) as rpt_buid,
                dc as dc_shop_id,
                case when dictGetString('dw_shop', 'dctype', tuple(dc_shop_id)) = '干货' then 1
                    else 2 end dctype,
                rpt.shopid,
                case when ( dictGetUInt8('dw_shop', 'shopformid', tuple(shopid)) as c) in (21, 51) then 2
                    when  c in (1, 6, 11) then  1
                    when  c in (9) then 5
                    else 0 end shopformid,
               toString(goodsid) as rpt_goodsid,
               dictGetUInt8('dw_buinfo', 'categorytreeid', toUInt64(rpt_buid))  AS categorytreeid,
                toString(categoryid) as categoryid_new,
                multiIf(logistics=1, 3, logistics=2, 2, 0) as rpt_logistics,
                rpt.venderid,
                case when rpt_buid = 19 then 4 else 1 end datasource,
                sum(rpt.check_scatter_num) as rpt_qty,
                sum(case when rpt_buid = 19
                    then multiIf(( trunc(toUInt64(rpt.categoryid)/10000) as d) = 53 , rpt.check_box_num / 50,
                                    d =  81 , 0, rpt.check_box_num )
                    else multiIf( d = 12 , rpt.check_box_num / 50,
                        d =  59 , 0, rpt.check_box_num )
                    end)  as  rpt_boxes,
                sum(rpt.cost) as rpt_cost,
                0 as rpt_taxcost
        from ods_hr.receipt_l rpt
        where rpt.check_date >= toDate('$stat_mon_first')
        and rpt.check_date  < addMonths(toDate('$stat_mon_first'), 1)
        group by stat_year,
                 stat_month,
                 stat_day,
                 rpt_buid,
                 dc_shop_id,
                 dctype,
                 shopid,
                 rpt_goodsid,
                 categorytreeid,
                 shopformid,
                 categoryid_new,
                 rpt_logistics,
                 venderid,
                 datasource

        union all
        select toYear(xct.check_date) as stat_year,
            toYYYYMM(xct.check_date)  as stat_month,
            xct.check_date as stat_day,
            21 as buid,
            xct.shop_id as dc_shop_id,
            multiIf((trunc(toUInt64(xct.category_id) / 10000) as c ) >=212 , 1, c < 212 , 2, 0) as dctype,
            xct.shop_id as shop_id,
            2 as shopformid,
            goods_code,
            21 AS categorytreeid,
            category_id,
            multiIf(xct.logistics = 'G', 3, xct.logistics IN ('T', 'F'), 2, 0) as logistics_id,
            '' as venderid,
            2 as datasource,
            sum(xct.check_scatter_num) as rpt_qty,
            sum(multiIf(c = 212, xct.check_box_num / 50, c >212, xct.check_box_num, 0)) as rpt_boxes,
            sum(xct.cost * xct.check_scatter_num) as rpt_cost,
            0 as rpt_taxcost
        from ods_hr.xg_check_t xct
        where xct.logistics IN ('T', 'G', 'F')
         and xct.shop_id in ('001001', '006901')
        and xct.check_date >= toDate('$stat_mon_first')
        and xct.check_date  < addMonths(toDate('$stat_mon_first'), 1)
        group by stat_year, stat_month, stat_day, dc_shop_id, dctype,goods_code, category_id, logistics_id

        union all
        select toYear(sct.check_date) as stat_year,
            toYYYYMM(sct.check_date)  as stat_month,
            sct.check_date as stat_day,
            17 as buid,
            sct.shop_id as dc_shop_id,
            multiIf(sct.shop_id in ('9001', '9005', '9006') ,1, sct.shop_id='0000', 2, 0) as dctype,
            sct.shop_id as shop_id,
            case when (toUInt64(substring(toString(yt_bm), 1, 2 )) AS c)  = 91 then 5
                    when c >=21 and c <=60 then 2
                    when c in (92, 93, 97, 98)  then 1
                else  0 end shopformid,
            goods_code,
            17 AS categorytreeid,
            category_id,
            multiIf(sct.logistics in ('3调整', '0存储'), 3, sct.logistics in ('1直流'), 2, 0) as logistics_id,
            sct.venderid,
            3 as datasource,
            sum(sct.check_scatter_num) as rpt_qty,
            sum(multiIf( ( trunc(toUInt32(sct.category_id) / 10000) as d ) = 212, sct.check_box_num / 50,
                        d<>212 and sct.category_id not in('060101', '060201'), sct.check_box_num, 0 )) as rpt_boxes,
            sum(sct.check_taxamount) as rpt_cost,
            0 as rpt_taxcost
        from ods_hr.sg_check_t sct
        all inner join ods_hr.sg_shop ss on sct.shop_id = ss.shopid
        where sct.check_date >= toDate('$stat_mon_first')
        and sct.check_date   <  addMonths(toDate('$stat_mon_first'), 1)
        and sct.logistics != '2退货'
        and sct.goods_code NOT IN ('888886', '888887', '888894', '999258258')
        group by stat_year, stat_month, stat_day, dc_shop_id, dctype, shop_id,
                shopformid, goods_code, category_id, logistics_id, venderid;"
                
clickhouse-client -h $CLICKHOUSE_HOST --user=default --password=$CLICKHOUSE_PASSWORD -m -n --query="$sql"
echo "----------------- $stat_mon Complete---------------------"
startDate=$(date -d "$startDate+1months" +%Y%m%d)
done
