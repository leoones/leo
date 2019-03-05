truncate table dw_hr.dw_bu_info;

insert into dw_hr.dw_bu_info(
  buid, buname,  mapping_buid, mapping_buname, categorytreeid
    )
select toUInt8(jb.buid) as buid_new,
       jb.buname,
      multiIf(jb.buid='32', 15, toUInt8(jb.buid)) as mapping_buid,
      multiIf(jb.buid='32', '', jb.buname) as mapping_buname,
      toUInt8(jb.categorytreeid)
 from ods_hr.jv_buinfo jb;