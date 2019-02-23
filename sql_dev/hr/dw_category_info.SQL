truncate table dw_hr.dw_category_info;

insert into  dw_hr.dw_category_info
(
 categorytreeid, deptlevelid, categoryid,categoryname, mid_categoryid, mid_categoryname,
 h_categoryid, h_categoryname
 ) 
select tmp1.categorytreeid,
 tmp1.deptlevelid,
 tmp1.categoryid,
 tmp1.categoryname,
 tmp1.mid_categoryid,
 tmp1.mid_categoryname,
 high.categoryid as higth_categoryid,
 high.categoryname as higth_categoryname
 from 
	(
select low.categorytreeid,
     low.deptlevelid,
     low.categoryid,
     low.categoryname,
     mid.categoryid as mid_categoryid,
     mid.categoryname as mid_categoryname
from ods_hr.jv_category low
all inner join ods_hr.jv_category mid on low.categorytreeid = mid.categorytreeid and low.headcatid = mid.categoryid
	 ) tmp1
all inner join ods_hr.jv_category high on tmp1.categorytreeid = high.categorytreeid and tmp1.mid_categoryid = high.categoryid;