insert into dw_hr.dw_shop_info
(
 shopid,
 shopcode,
 shopname,
 buid,
 inbuid,
 shopformid,
 shopstatus,
 buname,
 categorytreeid,
 payshopid,
 regionid,
 provinces_regions,
 propertype,
 dctype
)

select 
 tmp1.shopid,
 tmp1.shopcode,
 tmp1.shopname,
 tmp1.buid,
 tmp1.inbuid,
 tmp1.shopformid,
 tmp1.shopstatus,
 jb.buname,
 jb.categorytreeid,
 tmp1.payshopid,
 tmp1.regionid,
 tmp1.provinces_regions,
 tmp1.propertype,
 tmp1.dctype
 from 
	(select  js.shopid,
	 js.shopcode,
	 js.shopname,
	 js.buid,
	 js.inbuid,
	 js.shopformid,
	 js.shopstatus,
	 js.payshopid,
	 js.regionid,
	 js.provinces_regions,
	 jd.propertype,
	 jd.dctype
	  from ods_hr.jv_shop js
	left  join ods_hr.jt_dcinfo jd on js.shopid = jd.shopid) tmp1
left join ods_hr.jv_buinfo jb on tmp1.buid = jb.buid
