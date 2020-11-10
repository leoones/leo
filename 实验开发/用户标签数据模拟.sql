drop table if exists  tdm_hr.ch_tag_user_date;
create table if not exists tdm_hr.ch_tag_user_date
(
	tag_id String,
	tag_value Date,
	user_ids AggregateFunction(groupBitmap, UInt32)
)
engine = AggregatingMergeTree PARTITION BY tag_id
ORDER BY (tag_id, tag_value)
SETTINGS index_granularity = 8192;

drop table if exists tdm_hr.ch_tag_user_dicimal;
create table if not exists tdm_hr.ch_tag_user_dicimal
(
	tag_id String,
	tag_value Decimal(18,4),
	user_ids AggregateFunction(groupBitmap, UInt32)
)
engine = AggregatingMergeTree PARTITION BY tag_id
ORDER BY (tag_id, tag_value)
SETTINGS index_granularity = 8192;

drop table if exists tdm_hr.ch_tag_user_int;
create table if not exists tdm_hr.ch_tag_user_int
(
	tag_id String,
	tag_value UInt32,
	user_ids AggregateFunction(groupBitmap, UInt32)
)
engine = AggregatingMergeTree PARTITION BY tag_id
ORDER BY (tag_id, tag_value)
SETTINGS index_granularity = 8192;

drop table if exists  tdm_hr.ch_tag_user_str;
create table if not exists tdm_hr.ch_tag_user_str
(
	tag_id String,
	tag_value String,
	user_ids AggregateFunction(groupBitmap, UInt32)
)
engine = AggregatingMergeTree PARTITION BY tag_id
ORDER BY (tag_id, tag_value)
SETTINGS index_granularity = 8192;

drop table if exists  tdm_hr.ch_user_tag_info;
create table if not exists tdm_hr.ch_user_tag_info
(
	userid UInt32,
	tag_id String,
	tag_value String,
	INDEX  idx_userid(userid) TYPE bloom_filter granularity 1
)
engine = MergeTree PARTITION BY tag_id
ORDER BY userid
SETTINGS index_granularity = 8192;



alter table tdm_hr.ch_user_tag_info drop partition 'A1001';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
            SELECT useid,
                'A1001',
                   addDays(toDate('2015-01-01') , modulo(rand64(), 1988))
            FROM generateRandom('useid UInt32, register_time DateTime', 1, 10, 2)
            LIMIT 100000000;
alter table  tdm_hr.ch_tag_user_date drop partition 'A1001';
insert into tdm_hr.ch_tag_user_date(tag_id, tag_value, user_ids)
       select  tag_id,
               tag_value,
               groupBitmapState(userid)
         from tdm_hr.ch_user_tag_info
        where tag_id = 'A1001'
      group by tag_id, tag_value;
alter table tdm_hr.ch_user_tag_info drop partition 'A1002';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id,   tag_value)
        select  userid,
               'A1002',
               arrayElement(['QQ','WEIXIN', 'FRIENDS','ERWEIMA'],modulo(rand64(), length(['QQ','WEIXIN', 'FRIENDS','ERWEIMA'])))
          from tdm_hr.ch_user_tag_info
         where tag_id = 'A1001';

alter table tdm_hr.ch_tag_user_str drop partition 'A1002';
drop table if exists  tdm_hr.ch_tag_user_str;
create table if not exists tdm_hr.ch_tag_user_str
(
	tag_id String,
	tag_value String,
	user_ids AggregateFunction(groupBitmap, UInt32)
)
engine = AggregatingMergeTree PARTITION BY tag_id
ORDER BY (tag_id, tag_value)
SETTINGS index_granularity = 8192;
insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value, user_ids)
           select  tag_id,  tag_value,  groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A1002'
          group by tag_id, tag_value;
alter table tdm_hr.ch_user_tag_info drop partition 'A1003';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id,  tag_value)

         select  userid,
                   'A1003',
                   arrayElement(['�ӱ�ʡ','ɽ��ʡ','����ʡ','����ʡ','������ʡ','����ʡ','�㽭ʡ',
                            '����ʡ','����ʡ','����ʡ','ɽ��ʡ','����ʡ','����ʡ','����ʡ','�㶫ʡ','����ʡ','�Ĵ�ʡ','����ʡ','����ʡ','����ʡ',
                            '����ʡ','�ຣʡ','̨��ʡ','���ɹ�������','����׳��������','����������','���Ļ���������','�½�ά���������',
                            '������','�����','�Ϻ���','������'],modulo(rand64(),
                                length(['�ӱ�ʡ','ɽ��ʡ','����ʡ','����ʡ','������ʡ','����ʡ','�㽭ʡ',
                            '����ʡ','����ʡ','����ʡ','ɽ��ʡ','����ʡ','����ʡ','����ʡ','�㶫ʡ','����ʡ','�Ĵ�ʡ','����ʡ','����ʡ','����ʡ',
                            '����ʡ','�ຣʡ','̨��ʡ','���ɹ�������','����׳��������','����������','���Ļ���������','�½�ά���������',
                            '������','�����','�Ϻ���','������'])))
              from tdm_hr.ch_user_tag_info
             where tag_id = 'A1001'
             order by  rand()  limit 38876492;
alter table tdm_hr.ch_tag_user_str drop partition 'A1003';
insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value,  user_ids)
               select  tag_id, tag_value,  groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A1003'
              group by tag_id, tag_value;

alter table tdm_hr.ch_user_tag_info drop partition 'A1004';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id,  tag_value)
        select  userid,
               'A1004',
               arrayElement([133,149,153,173,177,180,181,189,199, 130,131,132,145,155,156,166,171,175,176,185,186,166,
               135,136,137,138,139,147,150,151,152,157,158,159,172,178,182,183,184,187,188,198],modulo(rand64(),
                   length([133,149,153,173,177,180,181,189,199, 130,131,132,145,155,156,166,171,175,176,185,186,166,
               135,136,137,138,139,147,150,151,152,157,158,159,172,178,182,183,184,187,188,198])))
          from tdm_hr.ch_user_tag_info
         where tag_id = 'A1001';

alter table  tdm_hr.ch_tag_user_str drop partition 'A1004';
insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value,  user_ids)
           select  tag_id, tag_value,   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A1004'
          group by tag_id, tag_value;


--A1005  ��������
  alter table tdm_hr.ch_user_tag_info drop partition 'A1005';
  insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
        select  userid,
               'A1005',
               addDays(toDate('1970-01-01'),modulo(rand(),dateDiff('day',toDate('1970-01-01'), toDate('2020-06-11'))))
          from tdm_hr.ch_user_tag_info ctum
          where tag_id =  'A1003';

    alter table tdm_hr.ch_tag_user_date drop partition 'A1005';
    insert into tdm_hr.ch_tag_user_date(tag_id,  tag_value, user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A1005'
          group by tag_id, tag_value;


--A1006 ְҵ
    alter table tdm_hr.ch_user_tag_info drop partition 'A1006';
    insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
            select  userid,
                   'A1006',
                   arrayElement(['��ҵԱ','����Ա','����ʦ','���ʦ','����Ա','������Ա','��ʦ','����Ա','Ӫ����Ա','����','˾��','����','��ƱԱ',
            '����ʦ','ӪҵԱ','����','��ķ','ҽ��','��ʿ','ҩ��ʦ','Ӫ��ʦ','����','�������','��Ħ��ʦ',
            '��Ա','����','��Ƭ','����','���','����','������'
            ] ,modulo(rand64(),
                length(['��ҵԱ','����Ա','����ʦ','���ʦ','����Ա','������Ա','��ʦ','����Ա','Ӫ����Ա','����','˾��','����','��ƱԱ',
            '����ʦ','ӪҵԱ','����','��ķ','ҽ��','��ʿ','ҩ��ʦ','Ӫ��ʦ','����','�������','��Ħ��ʦ',
            '��Ա','����','��Ƭ','����','���','����','������'
            ] )))
              from tdm_hr.ch_user_tag_info
             where tag_id = 'A1001'
            order by  rand() limit 859764;

    alter table  tdm_hr.ch_tag_user_str drop partition 'A1006';
    insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value,   user_ids)
               select  tag_id,  tag_value, groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A1006'
              group by tag_id, tag_value;

---A1007 ѧ��
    alter table tdm_hr.ch_user_tag_info drop partition 'A1007';
    insert into tdm_hr.ch_user_tag_info(userid,  tag_id,   tag_value)
            select  userid,
                   'A1007',
                   arrayElement(['����','����', '��ר','��ר', '����', '�о���', '��ʿ��'],modulo(rand64(),
                       length(['����','����', '��ר','��ר', '����', '�о���', '��ʿ��'])))
              from tdm_hr.ch_user_tag_info
             where tag_id = 'A1006'
           order by  rand() limit 789807;

    alter table tdm_hr.ch_tag_user_str drop partition 'A1007';
    insert into tdm_hr.ch_tag_user_str(tag_id, tag_value,   user_ids)
           select  tag_id, tag_value,  groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A1007'
          group by tag_id, tag_value;


--A1008 ����
    alter table tdm_hr.ch_user_tag_info drop partition 'A1008';
    insert into tdm_hr.ch_user_tag_info(userid,  tag_id,  tag_value)
            select  userid,
                   'A1008',
                   arrayElement(['��', '��'] ,modulo(rand64(), length(['��', '��'] )))
              from tdm_hr.ch_user_tag_info
             where tag_id = 'A1007'
           order by  rand() limit 449805;

    alter table tdm_hr.ch_tag_user_str drop partition 'A1008';
    insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value, user_ids)
               select  tag_id, tag_value,
                       groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A1008'
              group by tag_id, tag_value;


--A1009 �Ա�
    alter table tdm_hr.ch_user_tag_info drop partition 'A1009';
    insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
            select  userid,
                   'A1008',
                   arrayElement(['��', 'Ů'] ,modulo(rand64(), length(['��', 'Ů'] )))
              from tdm_hr.ch_user_tag_info
              where tag_id = 'A1003';

    alter table tdm_hr.ch_tag_user_str drop partition 'A1009';
    insert into tdm_hr.ch_tag_user_str(tag_id,
                                               tag_value,
                                               user_ids)
               select  tag_id, tag_value,
                       groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A1009'
              group by tag_id, tag_value;


--A1010 ʡ��
    alter table tdm_hr.ch_user_tag_info drop partition 'A1010';
    insert into tdm_hr.ch_user_tag_info(userid,   tag_id,  tag_value)

                select  userid,
                       'A1010',
                       arrayElement(['�ӱ�ʡ','ɽ��ʡ','����ʡ','����ʡ','������ʡ','����ʡ','�㽭ʡ',
                                '����ʡ','����ʡ','����ʡ','ɽ��ʡ','����ʡ','����ʡ','����ʡ','�㶫ʡ','����ʡ','�Ĵ�ʡ','����ʡ','����ʡ','����ʡ',
                                '����ʡ','�ຣʡ','̨��ʡ','���ɹ�������','����׳��������','����������','���Ļ���������','�½�ά���������',
                                '������','�����','�Ϻ���','������'],modulo(rand64(),
                                    length(['�ӱ�ʡ','ɽ��ʡ','����ʡ','����ʡ','������ʡ','����ʡ','�㽭ʡ',
                                '����ʡ','����ʡ','����ʡ','ɽ��ʡ','����ʡ','����ʡ','����ʡ','�㶫ʡ','����ʡ','�Ĵ�ʡ','����ʡ','����ʡ','����ʡ',
                                '����ʡ','�ຣʡ','̨��ʡ','���ɹ�������','����׳��������','����������','���Ļ���������','�½�ά���������',
                                '������','�����','�Ϻ���','������'])))
                  from tdm_hr.ch_user_tag_info
                 where tag_id = 'A1001';

    alter table tdm_hr.ch_tag_user_str drop partition 'A1010';
    insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value,  user_ids)
                   select  tag_id,
                          tag_value,
                           groupBitmapState(userid)
                     from tdm_hr.ch_user_tag_info
                    where tag_id = 'A1010'
                  group by tag_id, tag_value;

alter table tdm_hr.ch_user_tag_info drop partition 'A2001';
insert into tdm_hr.ch_user_tag_info(userid, tag_id,  tag_value)
            select tmp1.userid,
                   'A2001',
                     addDays(toDate(tmp1.tag_value) , modulo(rand(), 365))
                  from
                      (
                        select userid,
                             tag_value
                          from tdm_hr.ch_user_tag_info
                         where tag_id = 'A1001'
                         ) tmp1
                 SEMI LEFT JOIN
                (
                select userid
                  from tdm_hr.ch_user_tag_info
                 where tag_id = 'A1003'
                ) tmp2 on tmp1.userid = tmp2.userid
                    order by rand() limit  33738974;
alter table tdm_hr.ch_tag_user_date drop partition 'A2001';
insert into tdm_hr.ch_tag_user_date(tag_id,  tag_value,  user_ids)
               select  tag_id, tag_value,
                       groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A2001'
              group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2002';
insert into tdm_hr.ch_user_tag_info(userid, tag_id,  tag_value)
            select ctum.userid,
                   'A2002',
                   modulo(rand(), 1000)
              from tdm_hr.ch_user_tag_info ctum
            where ctum.tag_id = 'A2001';

alter table tdm_hr.ch_tag_user_int drop partition 'A2002';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value, user_ids)
               select  tag_id, tag_value,
                       groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A2002'
              group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2003';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
            select ctum.userid,
                   'A2003',
                   modulo(rand(), 200)
              from tdm_hr.ch_user_tag_info ctum
            where ctum.tag_id = 'A2001';
alter table tdm_hr.ch_tag_user_int drop partition 'A2003';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value, user_ids)
               select  tag_id, tag_value,
                       groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A2003'
              group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2004';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id,  tag_value)
        select ctum.userid,
               'A2004',
               toUInt64(ctum.tag_value) * modulo(rand(), 10)
          from tdm_hr.ch_user_tag_info ctum
        where ctum.tag_id = 'A2003';

alter table tdm_hr.ch_tag_user_int drop partition 'A2004';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value, user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A2004'
          group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2005';
insert into tdm_hr.ch_user_tag_info(userid, tag_id, tag_value)
        select ctum.userid,
               'A2005',
               toUInt64(ctum.tag_value) * modulo(rand64(), 500)
          from tdm_hr.ch_user_tag_info ctum
        where ctum.tag_id = 'A2004';

alter table tdm_hr.ch_tag_user_int drop partition 'A2005';
insert into tdm_hr.ch_tag_user_int(tag_id, tag_value, user_ids)
       select  tag_id, tag_value,
               groupBitmapState(userid)
         from tdm_hr.ch_user_tag_info
        where tag_id = 'A2005'
      group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2006';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
        select ctum.userid,
               'A2006',
               toDate(ctum.tag_value) + modulo(rand(), 712)
          from tdm_hr.ch_user_tag_info ctum
        where ctum.tag_id = 'A2001';

alter table tdm_hr.ch_tag_user_date drop partition 'A2006';
insert into tdm_hr.ch_tag_user_date(tag_id,  tag_value, user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A2006'
          group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2007';
insert into tdm_hr.ch_user_tag_info(userid, tag_id,  tag_value)
        select ctum.userid,
               'A2007',
               modulo(rand(), 500)
          from tdm_hr.ch_user_tag_info ctum
        where ctum.tag_id = 'A2001';

alter table tdm_hr.ch_tag_user_int drop partition 'A2007';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value, user_ids)
           select  tag_id,
                   tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A2007'
          group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2008';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id,  tag_value)
        select ctum.userid,
               'A2008',
               addDays(toDate(ctum.tag_value), modulo(rand64(), 10))
          from tdm_hr.ch_user_tag_info ctum
        where ctum.tag_id = 'A2001'
        order by rand64() limit 32798278;

alter table tdm_hr.ch_tag_user_date drop partition 'A2008';
insert into tdm_hr.ch_tag_user_date(tag_id,  tag_value,  user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A2008'
          group by tag_id, tag_value;



alter table tdm_hr.ch_user_tag_info drop partition 'A2009';
insert into tdm_hr.ch_user_tag_info(userid, tag_id,  tag_value)
        select tmp1.userid,
              'A2009',
              tmp2.tag_value
       from
        (select userid
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2008') tmp1
    semi left join     (select userid, tag_value
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2002') tmp2  on tmp1.userid = tmp2.userid;

alter table tdm_hr.ch_tag_user_int drop partition 'A2009';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value,  user_ids)
               select  tag_id, tag_value,
                       groupBitmapState(userid)
                 from tdm_hr.ch_user_tag_info
                where tag_id = 'A2009'
              group by tag_id, tag_value;
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
    select tmp1.userid,
'A2010',
          tmp2.tag_value
       from
        (select userid
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2003') tmp2  on tmp1.userid = tmp2.userid;



alter table tdm_hr.ch_tag_user_int drop partition 'A2010';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value,  user_ids)
       select  tag_id, tag_value,  groupBitmapState(userid)
         from tdm_hr.ch_user_tag_info
        where tag_id = 'A2010'
      group by tag_id, tag_value;

alter table tdm_hr.ch_user_tag_info drop partition 'A2010';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id,  tag_value)
    select tmp1.userid,
'A2011',
          tmp2.tag_value
       from
        (select userid
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2005') tmp2  on tmp1.userid = tmp2.userid;

alter table tdm_hr.ch_tag_user_int drop partition 'A2011';
insert into tdm_hr.ch_tag_user_int(tag_id, tag_value,  user_ids)
       select  tag_id,
               toInt64(tag_value) as tag_value_new,
               groupBitmapState(userid)
         from tdm_hr.ch_user_tag_info
        where tag_id = 'A2011'
      group by tag_id, tag_value_new;



alter table tdm_hr.ch_user_tag_info drop partition 'A2012';
insert into tdm_hr.ch_user_tag_info(userid, tag_id,  tag_value)
    select tmp1.userid,
'A2012',
          tmp2.tag_value
       from
        (select userid
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2005') tmp2  on tmp1.userid = tmp2.userid;

alter table tdm_hr.ch_tag_user_int drop partition 'A2012';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value, user_ids)
       select  tag_id,
               tag_value,
               groupBitmapState(userid)
         from tdm_hr.ch_user_tag_info
        where tag_id = 'A2012'
      group by tag_id, tag_value;
alter table tdm_hr.ch_user_tag_info drop partition 'A2013';
insert into tdm_hr.ch_user_tag_info(userid, tag_id,  tag_value)
    select tmp1.userid,
'A2013',
          toDate(tmp2.tag_value) + modulo(rand64(), 5)
       from
        (select userid
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2006') tmp2  on tmp1.userid = tmp2.userid;
alter table tdm_hr.ch_tag_user_int drop partition 'A2013';
insert into tdm_hr.ch_tag_user_date(tag_id,  tag_value, user_ids)
       select  tag_id, tag_value,
               groupBitmapState(userid)
         from tdm_hr.ch_user_tag_info
        where tag_id = 'A2013'
      group by tag_id, tag_value;
alter table tdm_hr.ch_user_tag_info drop partition 'A2014';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
    select tmp1.userid,
'A2014',
            tmp2.tag_value
       from
        (select userid
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2009') tmp1
    semi left join   (select userid, tag_value
            from tdm_hr.ch_user_tag_info
        where tag_id = 'A2007') tmp2  on tmp1.userid = tmp2.userid;
alter table tdm_hr.ch_tag_user_int drop partition 'A2014';
insert into tdm_hr.ch_tag_user_int(tag_id,  tag_value, user_ids)
       select  tag_id, tag_value,
               groupBitmapState(userid)
         from tdm_hr.ch_user_tag_info
        where tag_id = 'A2014'
      group by tag_id, tag_value;
alter table tdm_hr.ch_user_tag_info drop partition 'A3001';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
        select  userid,
               'A3001',
                 '��'
          from tdm_hr.ch_user_tag_info
         where tag_id = 'A1001'
       order by  rand() limit 949305;
alter table tdm_hr.ch_tag_user_str drop partition 'A3001';
insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value,  user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A3001'
          group by tag_id, tag_value;
alter table tdm_hr.ch_user_tag_info drop partition 'A3002';
insert into tdm_hr.ch_user_tag_info(userid,    tag_id,  tag_value)
        select  userid,
               'A3002',
               '��'
          from tdm_hr.ch_user_tag_info
         where tag_id = 'A1001'
       order by  rand() limit 5949305;
alter table tdm_hr.ch_tag_user_str drop partition 'A3002';
insert into tdm_hr.ch_tag_user_str(tag_id,  tag_value, user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A3002'
          group by tag_id, tag_value;
alter table tdm_hr.ch_user_tag_info drop partition 'A3003';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
        select  userid,
               'A3003',
               toDate(tag_value)+ modulo(rand64(), 365)
          from tdm_hr.ch_user_tag_info
         where tag_id = 'A1001'
       order by  rand() limit 6949305;
alter table tdm_hr.ch_tag_user_date drop partition 'A3003';
insert into tdm_hr.ch_tag_user_date(tag_id, tag_value,  user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A3003'
          group by tag_id, tag_value;

alter table tdm_hr.ch_user_tag_info drop partition 'A3004';
insert into tdm_hr.ch_user_tag_info(userid,  tag_id, tag_value)
        select  userid,
               'A3004',
               toDate(tag_value)+ modulo(rand64(), 365)
          from tdm_hr.ch_user_tag_info
         where tag_id = 'A3003';

alter table tdm_hr.ch_tag_user_date drop partition 'A3004';
insert into tdm_hr.ch_tag_user_date(tag_id,  tag_value,  user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A3004'
          group by tag_id, tag_value;


alter table tdm_hr.ch_user_tag_info drop partition 'A3005';
insert into tdm_hr.ch_user_tag_info(userid, tag_id,  tag_value)
        select  userid,
               'A3005',
               toDate(tag_value)+ modulo(rand64(), 365)+ modulo(rand64(), 365)
          from tdm_hr.ch_user_tag_info
         where tag_id = 'A1001';

alter table tdm_hr.ch_tag_user_date drop partition 'A3005';
insert into tdm_hr.ch_tag_user_date(tag_id,  tag_value,  user_ids)
           select  tag_id, tag_value,
                   groupBitmapState(userid)
             from tdm_hr.ch_user_tag_info
            where tag_id = 'A3005'
          group by tag_id, tag_value;