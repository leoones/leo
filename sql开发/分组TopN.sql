create table t_group_top_n
(
employ_id String,
dep_id String,
sal  UInt64
) engine =  Log;


insert into t_group_top_n
values ('e_1', 'd_1', 123),
       ('e_1', 'd_1', 223),
       ('e_3', 'd_1', 89),
       ('e_4', 'd_2', 123),
       ('e_5', 'd_2', 123);


select *
 from t_group_top_n tgtn
order by tgtn.dep_id, tgtn.sal desc
limit 1 by tgtn.dep_id
