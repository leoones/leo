---表过期功能: 表在merge过程 会删除过期数据
create table default.expire_table(
  d DateTime,
  a int
)
 engine =MergeTree partition by toYYYYMM(d)
  order by d;



insert into default.expire_table(d, a)
values (now(), 12), (now()-3, 6);


alter table default.expire_table  MODIFY TTL d + interval 1 day;

--手动merge
OPTIMIZE TABLE default.expire_table FINAL;



--列过期功能:
  过期的话 列被设置默认值。
 create table default.expire_column(
  d DateTime,
  a Int TTL d + interval 1 day,
  c String TTL d + interval 1 day
)
  engine = MergeTree
  partition by toYYYYMM(d)
  order by  d;


insert into default.expire_column
    (d, a, c)
values (now(), 1, 'a'),
       (now(), 2, 'c'),
       ('2019-08-01 00:00:00', 5, 'd');


select * from default.expire_column;

optimize table default.expire_column final ;

