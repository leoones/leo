drop  table default.jion_a;

create table default.jion_a(
  user_id UInt64 comment '用户ID',
  user_name String comment '用户名称',
  start_time DateTime comment '开始时间',
  end_time DateTime comment '截止时间'
) engine=Log;

truncate  table  default.jion_a;

insert into default.jion_a
(user_id ,user_name ,start_time,end_time)
values (1, 'a', toDateTime('2017-09-20 00:00:00'), toDateTime('2018-09-20 00:00:00'))
       (1, 'a', toDateTime('2018-09-20 00:00:00'), toDateTime('2019-09-01 00:00:00')),
       (1, 'b', toDateTime('2019-09-01 00:00:00'), toDateTime('2020-09-01 00:00:00'));

drop table default.join_b;

create table default.join_b(
user_id UInt64 comment '用户ID',
pay_time DateTime comment '支付时间',
pay_channel String comment '支付渠道',
cost UInt64 comment '支付金额'
) engine = Log;

insert into default.join_b(user_id, pay_time, pay_channel, cost)
     values (1, toDateTime('2022-09-25 00:00:00'), 'Vivo', 979797);


---------------------------------
假设: 左边记录数为: n  右边为: m

---------------------------------- any join ----------------------------------
匹配右边表的第一条.
set any_join_distinct_right_table_keys=1;
select b.*,
       a.user_name,
       a.start_time,
       a.end_time
  from default.join_b b
any inner  join  default.jion_a a on b.user_id = a.user_id

---------------------------------- all join ----------------------------------
匹配右表所有行.
select b.*,
       a.user_name,
       a.start_time,
       a.end_time
  from default.join_b b
all inner  join  default.jion_a a on b.user_id = a.user_id
where b.pay_time >= a.start_time and b.pay_time < a.end_time

---------------------------------- ASOF join ----------------------------------
条件关联:
select b.*,
       a.user_name,
       a.start_time,
       a.end_time
  from default.join_b b
ASOF join default.jion_a a on b.user_id = a.user_id and b.pay_time >= a.start_time

