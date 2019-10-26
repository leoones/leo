create table default.t_row_column
(
  usrid String,
  productlst String,
  costlst String
) engine = Log;


insert into default.t_row_column
(usrid, productlst, costlst)
values ('user_1', 'p_1,p_2,p_5', '234,651,12'),
       ('user_2', 'p_1,p_5', '234, 24');

-------------------- 一行转多行 --------------------
select usrid,
        product,
        cost
 from
(select usrid,
      splitByChar(',', productlst) as p_list,
      splitByChar(',', costlst) as c_list
  from default.t_row_column
    )
array join p_list as product, c_list as cost;



-------------------- 多行转一行 --------------------
select userid,
       arrayStringConcat(groupArray(cost), '|')
 from
(select 'a' as userid,
       toString(arrayJoin([1,2,4])) as cost
    )
group by userid;

