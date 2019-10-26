Create table t_liucun(
useid UInt8 comment '用户id',
loginday Date comment '登录日期',
event String comment '事件'
) engine = MergeTree
partition by toYYYYMM(loginday)
order by useid;



insert into t_liucun(useid, loginday, event)
values(1, '2019-02-10', 'register'),
      (1, '2019-02-12', 'visit'),
      (1, '2019-02-13', 'order'),
      (1, '2019-02-15', 'pay'),
       
      (2, '2019-02-11', 'register'),
      (2, '2019-02-12', 'visit'),
      
      (3, '2019-02-10', 'register'),
      (3, '2019-02-12', 'visit'),
      (3, '2019-02-13', 'order');    



insert into t_liucun(useid, loginday, event)
values
       
      (22, '2019-02-11', 'register'),
      (22, '2019-02-12', 'visit'); 
      
      
      

-- 1 , 2, 3, 6 , 15, 30留存
select sum(retentions[1]) as registers,
       sum(retentions[2]) as first,
       sum(retentions[3]) as second,
       sum(retentions[4]) as third,
       sum(retentions[5]) as seven,
       sum(retentions[6]) as fifth,
       sum(retentions[7]) as thirty
from
(select tlc.useid,
       retention(loginday='2019-02-10' and event='register',
                 loginday=addDays(toDate('2019-02-10'), 1),
                 loginday=addDays(toDate('2019-02-10'), 2),
                 loginday=addDays(toDate('2019-02-10'), 3),
                 loginday=addDays(toDate('2019-02-10'), 7),
                 loginday=addDays(toDate('2019-02-10'), 15),
                  loginday=addDays(toDate('2019-02-10'), 30)
                 ) AS retentions
  from t_liucun tlc
where tlc.loginday >= '2019-02-10'
group by  tlc.useid
    );
  
