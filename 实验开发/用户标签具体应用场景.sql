---1�Ԥ�� ѡȡ�ض��û�Ⱥ
  --1.1 �Ա�Ϊ'Ů'  �� ���60���е�¼ �� ���� �����Ƽ�����
     --1.1.1 ����
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
        select bitmapAndCardinality(user0, bitmapAnd(user1, user2)) as `�û�����`
        ;
     --1.2 �û���ϸ
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
        select arrayJoin(bitmapToArray(bitmapAnd(user0, bitmapAnd(user1, user2)))) as `�û�ID`
        ;

     --��ǩ��д �����û�����Ч������
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


--�Ч������
  select bitmapToArray(groupBitmapMergeState(ctui.users))
    from tag_dt.ch_tag_user_int ctui
where label_id = 'CUSTOMER-3003';
