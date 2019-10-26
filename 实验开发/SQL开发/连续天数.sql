create table t_lianx_day(
user_id String,
login_day Date
) ENGINE=Log;


insert into t_lianx_day(user_id, login_day)
VALUES
('a', '2018-08-01'),
('a', '2018-08-02'),
('a', '2018-08-03'),
('a', '2018-08-06'),
('a', '2018-08-07'),
('a', '2018-08-08'),
('a', '2018-08-09'),
('b', '2018-08-09'),
('b', '2018-08-07'),
('b', '2018-08-08');


select user_id,
       diff_day,
       count(1) as "连续天数",
       min(day) as "起始日期",
       max(day) as "截止日期"
 from 
(select user_id,
       groupArray(login_day) as arr_days,
       arrayEnumerate(arr_days) as arr_index,
       arrayMap(x, y -> x -y, arr_days, arr_index) as diff_day
  from 
	(select user_id, login_day
	  from t_lianx_day 
	  order by user_id, login_day
	 )
 group by user_id
)
array join arr_days as day, diff_day as diff_day
group by user_id,
       diff_day;
