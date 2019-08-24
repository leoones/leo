------------------------------------- dw_hr.fct_rt_dc_shop_sku_vender_day -----------------------------------------
truncate table dw_hr.fct_rt_dc_shop_sku_vender_day_ti;
insert into dw_hr.fct_rt_dc_shop_sku_vender_day_ti
            ( stat_year,
            stat_month,
            stat_day,
            out_buid,
            categorytreeid,
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
        dictGetUInt8('dw_shop', 'buid', tuple(out_shop_id)) as out_buid,
        dictGetUInt8('dw_shop', 'categorytreeid', tuple(out_shop_id)) as categorytreeid,
        rl.out_shop_id,
        dictGetUInt8('dw_shop', 'dctype', tuple(out_shop_id)) as dctype,
        rl.in_shop_id,
        dictGetUInt8('dw_shop', 'shofortype', tuple(in_shop_id)) as  shopformid,
        rl.goodsid,
        categoryid,
        rl.venderid,
        logistics,
        case when rl.type = 1 then 1
                when rl.type = 2 then 6
                when rl.type = 3 then 5
                when rl.type = 4 then 7
            end buntype,
        case when out_buid = 19 then 4 else 1 end datasource,
        sum(case when dctype = 2 then
                multiIf(lowerUTF8(dictGetString('dw_good_info', 'unitname', (out_buid, goodsid)) as cat) ='kg', qty / 10, qty / 16)
             when out_buid = 19 then
                multiIf(( trunc(toUInt64(rl.categoryid) / 10000)  as d ) = 53 , qty / (multiIf(rl.packing_qty<=0, 1, rl.packing_qty) as pcks )/50,
                                d not in (53, 81), qty/pcks, 0
                                )
                else multiIf(rl.box_flag = 1 , rl.qty/pcks/50,  rl.box_flag = 2 ,  rl.qty / pcks, 0)
                end  ) as rt_boxes,
        sum(rl.qty) as rt_qty,
        sum(rl.cost) as rt_cost,
        sum(rl.costvalue) as rt_taxcost
    from ods_hr.ration_l rl
    where rl.deliver_date >= toDate('${G_DAY}')
    and rl.deliver_date <  addDays(toDate('${G_DAY}'), 1)
    group by stat_year,
            stat_month,
            stat_day,
            out_buid,
             categorytreeid,
            rl.out_shop_id,
            dctype,
            rl.in_shop_id,
            shopformid,
            rl.goodsid,
            categoryid,
            rl.venderid,
            rl.logistics,
            buntype

    union all
    select  toYear(xdt.deliver_date) as stat_year,
        toYYYYMM(xdt.deliver_date) as stat_month,
        xdt.deliver_date as stat_day,
        21 as out_buid,
        21 as categorytreeid,
        xdt.out_shop_id,
        case when trunc(toUInt32(xdt.category_id) / 10000)>=212 then 1
            else 2 end dctype,
        xdt.in_shop_id,
        2 as shopformid,
        xdt.goods_code as goodsid,
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
    where xdt.deliver_date >= toDate('${G_DAY}')
    and xdt.deliver_date  < addDays(toDate('${G_DAY}'), 1)
    and xdt.logistics in ('F', 'G', 'T')
    group by stat_year,stat_month, stat_day,dctype, out_shop_id, in_shop_id, goodsid, categoryid_new , logisticsid

    union all
    select  toYear(xdt.deliver_date) as stat_year,
        toYYYYMM(xdt.deliver_date) as stat_month,
        xdt.deliver_date as stat_day,
        17 as out_buid,
        17 as categorytreeid,
        xdt.out_shop_id,
       dictGetUInt8('dw_shop', 'dctype', tuple(out_shop_id)) as dctype,
        xdt.in_shop_id,
        dictGetUInt8('dw_shop', 'shofortype', tuple(in_shop_id))  as  shopformid,
        xdt.goods_code as goodsid,
        category_id,
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
    all inner join ods_hr.sg_shop ss on xdt.out_shop_id = ss.shopid
    where xdt.deliver_date >= toDate('${G_DAY}')
    and xdt.deliver_date  < addDays(toDate('${G_DAY}'), 1)
    and xdt.logistics in ('0存储', '1直流', '3调整')
    and xdt.goods_code not in ('888886', '888887', '888894', '999258258')
    group by stat_year,stat_month,
             stat_day, venderid,
             out_shop_id, dctype,
             in_shop_id, shopformid,
             goodsid, category_id, logisticsid;

alter table dw_hr.fct_rt_dc_shop_sku_vender_day 
   delete where stat_day>= toDate('${G_DAY}') and stat_day < addDays(toDate('${G_DAY}'), 1);

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
	goodsid,
	logistics ,
	buntype ,
	dctype ,
	shopformid ,
	rt_qty ,
	rt_cost ,
	rt_taxcost ,
  rt_boxes,
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
  rt_supshop_boxes,
	rt_supshop_shops ,

	rt_smallshop_cost ,
	rt_smallshop_qty ,
   rt_smallshop_boxes,
	rt_smallshop_shops ,

	rt_dc_cost ,
	rt_dc_qty ,
  rt_dc_boxes,
  rt_dc_shops,

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
	rt_fresh_dc_qty,
    rt_fresh_dc_shops
)
 select frdssvd.stat_year,
        frdssvd.stat_month,
        frdssvd.stat_day,
        frdssvd.out_buid,
        frdssvd.out_shop_id,
        frdssvd.in_shop_id,
        datasource,
        venderid,
        categorytreeid,
        category_id ,
        goodsid,
        logistics,
        buntype,
        dctype,
        shopformid,
        sum(rt_qty) as m_rt_qty, --配送数量
        sum(rt_cost) as m_rt_cost, --配送金额
        sum(rt_taxcost) as m_rt_taxcost, --配送金额
        sum(rt_boxes) as m_rt_boxes,  --配送箱数
        max(in_shop_id) as m_rt_shops, -- 配送门店
        sum(multiIf(frdssvd.dctype = 1, frdssvd.rt_qty, 0)) as rt_drygood_qty, --配送数量(干货)
        sum(multiIf(frdssvd.dctype = 1, frdssvd.rt_cost, 0)) as rt_drygood_cost, --配送金额(干货)
        sum(multiIf(frdssvd.dctype = 1, frdssvd.rt_boxes, 0)) as rt_drygood_boxes, --配送箱数(干货)
        max(multiIf(frdssvd.dctype= 1, frdssvd.in_shop_id, null)) as rt_drygood_shops, --配送门店(干货)

        sum(multiIf(frdssvd.dctype = 2, frdssvd.rt_qty, 0)) as rt_fresh_qty,  --配送数量(生鲜)
        sum(multiIf(frdssvd.dctype = 2, frdssvd.rt_cost, 0)) as rt_fresh_cost, --配送金额(生鲜)
        max(multiIf(frdssvd.dctype = 2, frdssvd.in_shop_id, null)) as rt_fresh_shops, --配送门店(生鲜)

        sum(multiIf(frdssvd.shopformid =1, frdssvd.rt_cost, 0)) as rt_supshop_cost, --配送金额(大超)
        sum(multiIf(frdssvd.shopformid =1, frdssvd.rt_qty, 0)) as rt_supshop_qty, --配送数量(大超)
        sum(multiIf(frdssvd.shopformid =1, frdssvd.rt_boxes, 0)) as rt_supshop_boxes, --配送箱数(大超)
        max(multiIf(frdssvd.shopformid =1, frdssvd.in_shop_id, null)) as rt_supshop_shops, --配送门店(大超)

        sum(multiIf(frdssvd.shopformid =2, frdssvd.rt_cost, 0)) as rt_smallshop_cost, --配送金额(小业态)
        sum(multiIf(frdssvd.shopformid =2, frdssvd.rt_qty, 0)) as rt_smallshop_qty, --配送数量(小业态)
        sum(multiIf(frdssvd.shopformid =2, frdssvd.rt_boxes, 0)) as rt_smallshop_boxes, --配送箱数(小业态)
        max(multiIf(frdssvd.shopformid =2, frdssvd.in_shop_id, null)) as rt_smallshop_shops, --配送门店(小业态)

        sum(multiIf(frdssvd.shopformid =5, frdssvd.rt_cost, 0)) as rt_dc_cost, --配送金额(仓间)
        sum(multiIf(frdssvd.shopformid =5, frdssvd.rt_qty, 0)) as rt_dc_qty, --配送数量(仓间)
        sum(multiIf(frdssvd.shopformid =5, frdssvd.rt_boxes, 0)) as rt_dc_boxes, --配送箱数(小业态)
        max(multiIf(frdssvd.shopformid =5, frdssvd.in_shop_id, null)) as rt_dc_shops, --配送门店(小业态)

        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.rt_cost, 0)) as rt_drygood_supshop_cost, --配送金额(干货,大超)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.rt_qty, 0)) as rt_drygood_supshop_qty, -- 配送数量(干货,大超)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.rt_boxes,0)) as rt_drygood_supshop_boxes, --配送箱数(干货,大超)
        max(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =1, frdssvd.in_shop_id, null)) as rt_drygood_supshop_shops, --配送门店(干货,大超)

        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.rt_cost, 0)) as rt_drygood_smallshop_cost, --配送金额(干货,小业态)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.rt_qty, 0)) as rt_drygood_smallshop_qty, -- 配送数量(干货,小业态)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.rt_boxes,0)) as rt_drygood_smallshop_boxes, --配送箱数(干货,小业态)
        max(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =2, frdssvd.in_shop_id, null)) as rt_drygood_smallshop_shops, --配送门店(干货,小业态)

        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.rt_cost, 0)) as rt_drygood_dc_cost, --配送金额(干货,仓间)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.rt_qty, 0)) as rt_drygood_dc_qty, -- 配送数量(干货,仓间)
        sum(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.rt_boxes,0)) as rt_drygood_dc_boxes, --配送箱数(干货,仓间)
        max(multiIf(frdssvd.dctype = 1 and frdssvd.shopformid =5, frdssvd.in_shop_id, null)) as rt_drygood_dc_shops, --配送门店(干货,仓间)

        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =1, frdssvd.rt_cost, 0)) as rt_fresh_supshop_cost, --配送金额(生鲜,大超)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =1, frdssvd.rt_qty, 0)) as rt_fresh_supshop_qty, -- 配送数量(生鲜,大超)
        max(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =1, frdssvd.in_shop_id, null)) as rt_fresh_supshop_shops, --配送门店(生鲜,大超)

        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =2, frdssvd.rt_cost, 0)) as rt_fresh_smallshop_cost, --配送金额(生鲜,小业态)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =2, frdssvd.rt_qty, 0)) as rt_fresh_smallshop_qty, -- 配送数量(生鲜,小业态)
        max(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =2, frdssvd.in_shop_id, null)) as rt_fresh_smallshop_shops, --配送门店(生鲜,小业态)

        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =5, frdssvd.rt_cost, 0)) as rt_fresh_dc_cost, --配送金额(生鲜,仓间)
        sum(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =5, frdssvd.rt_qty, 0)) as rt_fresh_dc_qty, -- 配送数量(生鲜,仓间)
        max(multiIf(frdssvd.dctype = 2 and frdssvd.shopformid =5, frdssvd.in_shop_id, null)) as rt_fresh_dc_shop
        from dw_hr.fct_rt_dc_shop_sku_vender_day_ti frdssvd
    group by frdssvd.stat_year,
            frdssvd.stat_month,
            frdssvd.stat_day,
            frdssvd.out_buid,
            frdssvd.out_shop_id,
            frdssvd.in_shop_id,
            datasource,
            venderid,
            categorytreeid,
            category_id,
            goodsid,
            logistics,
            buntype,
            dctype,
            shopformid;
            


-------------------------------------------------- fct_rt_dc_shop_day --------------------------------------------------
 alter table dw_hr.fct_rt_dc_shop_day 
delete where stat_day >= toDate('${G_DAY}') and stat_day < addDays(toDate('${G_DAY}'), 1);

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
  rt_boxes,
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
  rt_supshop_boxes,
	rt_supshop_shops ,

	rt_smallshop_cost ,
	rt_smallshop_qty ,
   rt_smallshop_boxes,
	rt_smallshop_shops ,

	rt_dc_cost ,
	rt_dc_qty ,
  rt_dc_boxes,
  rt_dc_shops,

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
	rt_fresh_dc_qty ,
  rt_fresh_shops
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
      sum(rt_boxes),
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
      sum(rt_supshop_boxes),
			max(rt_supshop_shops) ,

			sum(rt_smallshop_cost) ,
			sum(rt_smallshop_qty) ,
      sum(rt_smallshop_boxes),
			max(rt_smallshop_shops) ,

			sum(rt_dc_cost) ,
			sum(rt_dc_qty) ,
      sum(rt_dc_boxes),
      max(rt_dc_shops),

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
			sum(rt_fresh_dc_qty) ,
      max(rt_fresh_dc_shops)
 from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
 where stat_day >= toDate('${G_DAY}')
   and stat_day  < addDays(toDate('${G_DAY}'),1)
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
      
      
-------------------------------------------------- fct_rt_dc_shop_sku_vender_mon --------------------------------------------------
insert into dw_hr.fct_rt_dc_shop_sku_vender_mon
  (
	stat_year ,
	stat_month ,
	out_buid ,
	out_shop_id ,
	in_shop_id ,
	datasource ,
	venderid ,
	categorytreeid ,
	categoryid ,
	goodsid ,
	logistics ,
	buntype ,
	dctype ,
	shopformid ,

	rt_qty ,
	rt_cost ,
	rt_taxcost ,
  rt_boxes,
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
  rt_supshop_boxes,
	rt_supshop_shops ,

	rt_smallshop_cost ,
	rt_smallshop_qty ,
   rt_smallshop_boxes,
	rt_smallshop_shops ,

	rt_dc_cost ,
	rt_dc_qty ,
  rt_dc_boxes,
  rt_dc_shops,

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
	rt_fresh_dc_qty ,
  rt_fresh_shops
)
select
		  stat_year ,
			stat_month ,
			out_buid ,
			out_shop_id ,
			in_shop_id ,
			datasource ,
      venderid ,
	    categorytreeid ,
	    categoryid ,
	    goodsid ,
			logistics ,
			buntype ,
			dctype ,
			shopformid ,
			sum(rt_qty) ,
			sum(rt_cost) ,
			sum(rt_taxcost) ,
      sum(rt_boxes),
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
      sum(rt_supshop_boxes),
			max(rt_supshop_shops) ,

			sum(rt_smallshop_cost) ,
			sum(rt_smallshop_qty) ,
      sum(rt_smallshop_boxes),
			max(rt_smallshop_shops) ,

			sum(rt_dc_cost) ,
			sum(rt_dc_qty) ,
      sum(rt_dc_boxes),
      max(rt_dc_shops),

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
			sum(rt_fresh_dc_qty) ,
      max(rt_fresh_dc_shops)
 from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd
 where stat_day >=  toDate('${G_DAY}')
   and stat_day < addMonths(toDate('${G_DAY}'), 1)
   and frdssvd.out_buid  in (17)
 group by stat_year ,
			stat_month ,
			out_buid ,
			out_shop_id ,
			in_shop_id ,
			datasource ,
			venderid ,
	    categorytreeid ,
	    categoryid ,
	    goodsid ,
			logistics ,
			buntype ,
			dctype ,
			shopformid;
