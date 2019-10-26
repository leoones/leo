Create table t_loudou(
useid UInt8 comment '用户id',
accesstime DateTime comment '行为时间',
event String comment '时间'
) engine = MergeTree
partition by toYYYYMM(accesstime)
order by useid;



insert into t_loudou(useid, accesstime, event)
values(1, '2019-02-15 18:30:00', 'login'),
      (1, '2019-02-15 18:30:30', 'detail'),
      (1, '2019-02-15 18:30:40', 'order'),
      (1, '2019-02-15 18:30:50', 'pay'),
      
      (2, '2019-02-15 18:30:30', 'detail'),
      (2, '2019-02-15 18:30:40', 'order'),
      (2, '2019-02-15 18:30:50', 'pay'),
      
      (3, '2019-02-15 18:40:10', 'search'),
      (3, '2019-02-15 18:40:20', 'detail');
      




--- 
select sum(details=1) as detail_users,
       sum(orders=1) as order_users,
       sum(pays=1) as pay_users
from
(select useid,
       max(event='detail') as details,
       sequenceMatch('(?1).*(?2)')(accesstime, event='detail', event='order') as orders,
       sequenceMatch('(?1).*(?2).*(?3)')(accesstime, event='detail', event='order', event='pay') as pays
  from  t_loudou
  group by useid
 )
  
  
