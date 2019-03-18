https://blog.csdn.net/zhenglit/article/details/88063821


create table default.test(
uid UInt8,
post_time DateTime,
content String
) engine =Log;

insert into default.test(uid, post_time, content)
values
(1,'2019-03-01 00:00:00','a'),
(2,'2019-03-01 00:00:01','b'),
(3,'2019-03-01 00:00:02','c'),
(3,'2019-03-01 00:00:03','d'),
(3,'2019-03-01 00:00:04','e'),
(2,'2019-03-01 00:00:05','f'),
(2,'2019-03-01 00:00:06','g'),
(1,'2019-03-01 00:00:07','h'),
(4,'2019-03-01 00:00:08','i'),
(4,'2019-03-01 00:00:09','j'),
(4,'2019-03-01 00:00:10','k'),
(5,'2019-03-01 00:00:11','l');


select aa.uid,
       aa.post_time,
       aa.contents,
       aa.rank,
       bb.rownum
from
    (select uid,
           post_time,
           contents,
           rank
    from
        (select ttx.uid  ,
           count() as total,
           arrayMap(x -> x +1, range(total)) as rnk_list,
           arraySort(groupArray(ttx.post_time)) as post_time_list,
           groupArray(ttx.content) as content_list
        from default.test ttx
        group by ttx.uid
        ) array join rnk_list as rank, post_time_list as post_time, content_list as contents
    )  as aa
all inner join
(select *, rowNumberInBlock() + 1 as rownum
 from (select * from default.test ttx order by ttx.post_time asc)) bb
        on aa.uid = bb.uid and aa.post_time = bb.post_time




