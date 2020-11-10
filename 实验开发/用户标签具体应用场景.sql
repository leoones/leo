---1活动预热 选取特定用户群
  --1.1 性别为'女'  且 最近60天有登录 且 渠道 好友推荐过来
     --1.1.1 总数
        WITH
           (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_int ctud
                where ctud.label_id = 'A2011'
                and ctud.label_value > 1000
                ) AS user0,
            (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_str ctud
                where ctud.label_id = 'A1005'
                and ctud.label_value = 'FRIENDS'
                ) AS user1,
            (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_date ctud
                where ctud.label_id = 'A3003'
                 and ctud.label_value >= subtractDays(now(), 60)
           ) AS user2
        select bitmapAndCardinality(user0, bitmapAnd(user1, user2)) as `用户总数`
        ;
     --1.2 用户明细
        WITH
           (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_int ctud
                where ctud.label_id = 'A2011'
                and ctud.label_value > 1000
                ) AS user0,
            (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_str ctud
                where ctud.label_id = 'A1005'
                and ctud.label_value = 'FRIENDS'
                ) AS user1,
            (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_date ctud
                where ctud.label_id = 'A3003'
                 and ctud.label_value >= subtractDays(now(), 60)
           ) AS user2
        select arrayJoin(bitmapToArray(bitmapAnd(user0, bitmapAnd(user1, user2)))) as `用户ID`
        ;

     --标签回写 用于用户后期效果分析
       alter table tag_dt.ch_tag_user_date drop partition 'CUSTOMER-3003';
       insert into tag_dt.ch_tag_user_int
        WITH
           (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_int ctud
                where ctud.label_id = 'A2011'
                and ctud.label_value > 1000
                ) AS user0,
            (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_str ctud
                where ctud.label_id = 'A1005'
                and ctud.label_value = 'FRIENDS'
                ) AS user1,
            (select groupBitmapMergeState(ctud.users)
                 from tag_dt.ch_tag_user_date ctud
                where ctud.label_id = 'A3003'
                 and ctud.label_value >= subtractDays(now(), 60)
           ) AS user2
       select  'CUSTOMER-3003' as tag_name,
               1 AS tag_value,
              bitmapAnd(bitmapAnd(user0, user1),user2);


--活动效果分析
  select bitmapToArray(groupBitmapMergeState(ctui.users))
    from tag_dt.ch_tag_user_int ctui
where label_id = 'CUSTOMER-3003';
