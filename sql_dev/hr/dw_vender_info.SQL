insert into dw_hr.dw_vender_info
(
 buid,
 venderid,
 vendername,
 vendercode,
 parent_buid,
 parent_venderid,
 parent_vendername
)
select tt.buid,
       tt.venderid,
       tt.vendername,
       tt.vendercode,
       ifnull(tp.buid,buid)  as parent_buid,
       ifnull(tp.venderid,venderid)  as parent_venderid,
       ifnull(tp.vendername,vendername) as parent_vendername
  from ods_hr.jv_vender tt
  left join ods_hr.jv_vender tp
    on tt.headbuid = tp.buid
   and tt.headvendercode = tp.venderid;