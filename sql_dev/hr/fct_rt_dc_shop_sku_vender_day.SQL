truncate table dw_hr.fct_rt_dc_shop_sku_vender_day_ti;
insert into dw_hr.fct_rt_dc_shop_sku_vender_day_ti
            ( stat_year,
            stat_month,
            stat_day,
            out_buid,
            out_shop_id,
            dctype,
            in_shop_id,
            shopformid,
            goodsid,
            category_id,
            venderid,
            logistics,
            buntype,
            datasource,
            rt_boxes,
            rt_qty,
            rt_cost,
            rt_taxcost
            )
select
        toYear(rl.deliver_date) as stat_year,
        toYYYYMM(rl.deliver_date) as stat_month,
        rl.deliver_date as stat_day,
        dictGetString('dw_shop', 'buid', tuple(out_shop_id)) as out_buid,
        rl.out_shop_id,
        case when dictGetString('dw_shop', 'dctype', tuple(out_shop_id)) = '干货' then 1
                else 2 end dctype,
        rl.in_shop_id,
        case when ( dictGetUInt16('dw_shop', 'shopformid', tuple(in_shop_id)) as c) in (21, 51) then 2
                when  c in (1, 6, 11) then  1
                when  c in (9) then 5
                else 0 end shopformid,
        rl.goodsid,
        toString(categoryid) as categoryid_new,
        rl.venderid,
        logistics,
        case when rl.type = 1 then 1
                when rl.type = 2 then 6
                when rl.type = 3 then 5
                when rl.type = 4 then 7
            end buntype,
        case when out_buid = '19' then 4 else 1 end datasource,
        sum(case when out_buid = '19'
                    then multiIf(( trunc(rl.categoryid / 10000)  as d ) = 53 , qty / (multiIf(rl.packing_qty<=0, 1, rl.packing_qty) as pcks )/50,
                                d not in (53, 81), qty/pcks, 0
                                )
                else multiIf(rl.box_flag = 1 , rl.qty/pcks/50,  rl.box_flag = 2 ,  rl.qty / pcks, 0)
                end ) as rt_boxes,
        sum(rl.qty) as rt_qty,
        sum(rl.cost) as rt_cost,
        sum(rl.costvalue) as rt_taxcost
    from ods_hr.ration_l rl
    where rl.deliver_date >= toDate('$stat_day')
    and rl.deliver_date <  addDays(toDate('$stat_day'), 1)
    group by stat_year,
            stat_month,
            stat_day,
            out_buid,
            rl.out_shop_id,
            dctype,
            rl.in_shop_id,
            shopformid,
            rl.goodsid,
            categoryid_new,
            rl.venderid,
            rl.logistics,
            buntype

    union all
    select  toYear(xdt.deliver_date) as stat_year,
        toYYYYMM(xdt.deliver_date) as stat_month,
        xdt.deliver_date as stat_day,
        '21' as out_buid,
        xdt.out_shop_id,
        case when trunc(toUInt32(xdt.category_id) / 10000)>=212 then 1
            else 2 end dctype,
        xdt.in_shop_id,
        2 as shopformid,
        toUInt32(xdt.goods_code) as goodsid,
        category_id as categoryid_new,
        '' as venderid,
        case when xdt.logistics in ('F', 'T') then 1
                when xdt.logistics in ('G') THEN 2
            ELSE 0 END logisticsid,
        1 as buntype,
        2 as datasource,
        sum(multiIf(trunc(toUInt32(xdt.category_id) / 10000) > 212 , deliver_box_num,
                trunc(toUInt32(xdt.category_id) / 10000) =212,  deliver_box_num / 50, 0)) as rt_boxes,
        sum(deliver_scatter_num) as rt_qty,
        sum(cost * deliver_scatter_num) as rt_cost,
        0 as rt_taxcost
    from ods_hr.xg_deliver_t xdt
    where xdt.deliver_date >= toDate('$stat_day')
    and xdt.deliver_date  < addDays(toDate('$stat_day'), 1)
    and xdt.logistics in ('F', 'G', 'T')
    group by stat_year,stat_month, stat_day,dctype, out_shop_id, in_shop_id, goodsid, categoryid_new , logisticsid

    union all
    select  toYear(xdt.deliver_date) as stat_year,
        toYYYYMM(xdt.deliver_date) as stat_month,
        xdt.deliver_date as stat_day,
        '17' as out_buid,
        xdt.out_shop_id,
        case when out_shop_id in ('9001', '9005', '9006') then 1
                when out_shop_id in ('0000') then 2
                else 0 end dctype,
        xdt.in_shop_id,
        case when (toUInt16(substring(toString(yt_bm), 1, 2 )) AS c)  = 91 then 5
                when c >=21 and c <=60 then 2
                when c in (92, 93, 97, 98)  then 1
            else  0 end shopformid,
        toUInt32(xdt.goods_code) as goodsid,
        category_id as categoryid_new,
        venderid,
        case when xdt.logistics in ('1直流') then 1
                when xdt.logistics in ('0存储',  '3调整') THEN 2
            ELSE 0 END logisticsid,
        1 as buntype,
        3 as datasource,
        sum(multiIf( (trunc(toUInt32(xdt.category_id) / 10000) as d)  = 212 , deliver_box_num / 50,
                    d<>212 and xdt.category_id not in('060101', '060201'),  deliver_box_num , 0)) as rt_boxes,
        sum(deliver_scatter_num) as rt_qty,
        sum(deliver_taxamount) as rt_cost,
        sum(cost*deliver_scatter_num) as rt_taxcost
    from ods_hr.sg_deliver_t xdt
    all inner join ods_hr.sg_shop ss on xdt.in_shop_id = ss.shopid
    where xdt.deliver_date >= toDate('$stat_day')
    and xdt.deliver_date  < addDays(toDate('$stat_day'), 1)
    and xdt.logistics in ('0存储', '1直流', '3调整')
    group by stat_year,stat_month, 
             stat_day, venderid,
             out_shop_id, dctype, 
             in_shop_id, shopformid, 
             goodsid, categoryid_new, logisticsid;           
 insert into dw_hr.fct_rt_dc_shop_sku_vender_day
  (
	stat_year ,
	stat_month ,
	stat_day ,
	out_buid ,
	out_shop_id ,
	in_shop_id ,
	datasource ,
	venderid ,
	categorytreeid ,
	categoryid ,
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
 select frdssvd.stat_year,
        frdssvd.stat_month,
        frdssvd.stat_day,
        frdssvd.out_buid,
        frdssvd.out_shop_id,
        frdssvd.in_shop_id,
        datasource,
        venderid,
        multiIf(datasource=2, 21, datasource=3, 17, dictGet('dw_buinfo', 'categorytreeid', tuple(out_buid))  ) as new_categorytreeid,
        multiIf(datasource in (2,3), category_id, dictGet('dw_good_info', 'categoryid',(out_buid, goodsid) )) as new_categoryid,
        logistics,
        buntype,
        dctype,
        shopformid,
        sum(rt_qty) as m_rt_qty, --配送数量
        sum(rt_cost) as m_rt_cost, --配送金额
        sum(rt_taxcost) as m_rt_taxcost, --配送金额
        max(in_shop_id) as m_rt_shops, -- 配送门店
        sum(multiIf(frdssvd.dctype = 1, frdssvd.rt_qty, 0)) as rt_drygood_qty, --配送数量(干货)
        sum(multiIf(frdssvd.dctype = 1, frdssvd.rt_cost, 0)) as rt_drygood_cost, --配送金额(干货)
        sum(multiIf(frdssvd.dctype = 1, frdssvd.rt_boxes, 0)) as rt_drygood_boxes, --配送箱数(干货)
        max(multiIf(frdssvd.dctype= 1, frdssvd.in_shop_id, '')) as rt_drygood_shops, --配送门店(干货)
        sum(multiIf(frdssvd.dctype = 2, frdssvd.rt_qty, 0)) as rt_fresh_qty,  --配送数量(生鲜)
        sum(multiIf(frdssvd.dctype = 2, frdssvd.rt_cost, 0)) as rt_fresh_cost, --配送金额(生鲜)
        max(multiIf(frdssvd.dctype = 2, frdssvd.in_shop_id, '')) as rt_fresh_shops, --配送门店(生鲜)
        sum(multiIf(frdssvd.shopformid =1, frdssvd.rt_cost, 0)) as rt_supshop_cost, --配送金额(大超)
        sum(multiIf(frdssvd.shopformid =1, frdssvd.rt_qty, 0)) as rt_supshop_qty, --配送数量(大超)
        max(multiIf(frdssvd.shopformid =1, frdssvd.in_shop_id, '')) as rt_supshop_shops, --配送门店(大超)
        sum(multiIf(frdssvd.shopformid =2, frdssvd.rt_cost, 0)) as rt_smallshop_cost, --配送金额(小业态)
        sum(multiIf(frdssvd.shopformid =2, frdssvd.rt_qty, 0)) as rt_smallshop_qty, --配送数量(小业态)
        max(multiIf(frdssvd.shopformid =2, frdssvd.in_shop_id, '')) as rt_smallshop_shops, --配送门店(小业态)
        sum(multiIf(frdssvd.shopformid =5, frdssvd.rt_cost, 0)) as rt_dc_cost, --配送金额(仓间)
        max(multiIf(frdssvd.shopformid =5, frdssvd.rt_qty, 0)) as rt_dc_qty, --配送数量(仓间)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.rt_cost, 0)) as rt_drygood_supshop_cost, --配送金额(干货,大超)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.rt_qty, 0)) as rt_drygood_supshop_qty, -- 配送数量(干货,大超)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.rt_boxes,0)) as rt_drygood_supshop_boxes, --配送箱数(干货,大超)
        max(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.in_shop_id, '')) as rt_drygood_supshop_shops, --配送门店(干货,大超)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.rt_cost, 0)) as rt_drygood_smallshop_cost, --配送金额(干货,小业态)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.rt_qty, 0)) as rt_drygood_smallshop_qty, -- 配送数量(干货,小业态)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.rt_boxes,0)) as rt_drygood_smallshop_boxes, --配送箱数(干货,小业态)
        max(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.in_shop_id, '')) as rt_drygood_smallshop_shops, --配送门店(干货,小业态)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.rt_cost, 0)) as rt_drygood_dc_cost, --配送金额(干货,仓间)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.rt_qty, 0)) as rt_drygood_dc_qty, -- 配送数量(干货,仓间)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.rt_boxes,0)) as rt_drygood_dc_boxes, --配送箱数(干货,仓间)
        max(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.in_shop_id, '')) as rt_drygood_dc_shops, --配送门店(干货,仓间)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =1, frdssvd.rt_cost, 0)) as rt_fresh_supshop_cost, --配送金额(生鲜,大超)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =1, frdssvd.rt_qty, 0)) as rt_fresh_supshop_qty, -- 配送数量(生鲜,大超)
        max(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =1, frdssvd.in_shop_id, '')) as rt_fresh_supshop_shops, --配送门店(生鲜,大超),
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =2, frdssvd.rt_cost, 0)) as rt_fresh_smallshop_cost, --配送金额(生鲜,小业态)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =2, frdssvd.rt_qty, 0)) as rt_fresh_smallshop_qty, -- 配送数量(生鲜,小业态)
        max(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =2, frdssvd.in_shop_id, '')) as rt_fresh_smallshop_shops, --配送门店(生鲜,小业态)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =5, frdssvd.rt_cost, 0)) as rt_fresh_dc_cost, --配送金额(生鲜,仓间)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =5, frdssvd.rt_qty, 0)) as rt_fresh_dc_qty -- 配送数量(生鲜,仓间)
        from dw_hr.fct_rt_dc_shop_sku_vender_day_ti frdssvd
    group by frdssvd.stat_year,
            frdssvd.stat_month,
            frdssvd.stat_day,
            frdssvd.out_buid,
            frdssvd.out_shop_id,
            frdssvd.in_shop_id,
            datasource,
            venderid,
            new_categorytreeid,
            new_categoryid,
            logistics,
            buntype,
            dctype,
            shopformid;