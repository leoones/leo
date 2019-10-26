--- 保存每日新增或者修改的用户信息
drop table default.states_join_ti;

create table default.states_join_ti
(id UInt64,
name String,
sale Decimal32(4),
create_time DateTime,
modify_time DateTime
)
  engine = Join(ANY , LEFT , id);

-- 用户信息表
drop table default.states_join;

create table default.states_join
(id UInt64,
name String,
sale Decimal32(4),
create_time DateTime,
modify_time DateTime
)
 engine  = MergeTree
  partition by toYYYYMM(create_time)
  order by id;



insert into default.states_join(
  id,  name,  sale, create_time, modify_time
    )
values (1, 'jerry',  234, '2019-06-06 12:21:00', '2019-06-06 12:21:00'),
        (2, 'jerry',  765, '2019-06-06 12:21:00', '2019-06-06 12:21:00'),
        (3, 'jerry',  434, '2019-06-06 12:21:00','2019-06-06 12:21:00'),
        (4, 'jerry',  934, '2019-06-06 12:21:00','2019-06-06 12:21:00'),
        (5, 'jerry',  534, '2019-06-06 12:21:00','2019-06-06 12:21:00');


-- 模拟每日修改用户
insert into default.states_join_ti(
  id,  name,  sale, create_time, modify_time
    )
values (1, 'jerry',  11234,  '2019-06-06 12:21:00', '2019-06-07 12:21:00'),
        (2, 'jerry',  11765, '2019-06-06 12:21:00', '2019-06-07 12:21:00');


-- 关联更新 注意: 不能更新主键字段
alter table default.states_join UPDATE  sale = joinGet('states_join_ti', 'sale', id),
                                         name = joinGet('states_join_ti', 'name', id),
                                         modify_time = joinGet('states_join_ti', 'modify_time', id)
where id <=2;
