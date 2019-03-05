truncate table dw_hr.dw_vender_info;


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
select toUInt8OrZero(tt.buid) as buid_new,
       toUInt64OrZero(tt.venderid) as venderid_new,
       tt.vendername,
       tt.vendercode,
       toUInt8OrZero(ifnull(tp.buid,buid))  as parent_buid,
       toUInt64OrZero(ifnull(tp.venderid,venderid))  as parent_venderid,
       ifnull(tp.vendername,vendername) as parent_vendername
  from ods_hr.jv_vender tt
  left join ods_hr.jv_vender tp
    on tt.headbuid = tp.buid
   and tt.headvendercode = tp.venderid;