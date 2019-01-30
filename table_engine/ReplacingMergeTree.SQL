---------------------------------------------------- 去重引擎 ----------------------------------------------------
1. 建表语句
	Create Table db.table_name  [on cluster]
	(
	 name1 type1 ,
	 name2 type2
	 ....
	) ENGINE=ReplacingMergeTree([ver])
	PARTITION BY expr
	ORDER BY expr
	SAMPLE BY expr
	SETTINGS name = value
	
	该引擎会根据主键(取版本号字段最大的)自动在后台进程去重. 去重是无定时计划的,因此不能保证时刻唯一性.
	
	一般以插入时间的时间戳作为版本号字段(ver)


2. 实验

  2.1 创建表
   drop table test.t_replacing;
   
	  create table test.t_replacing
		(
			gmt DateTime,
			id UInt64,
			name String,
			ver DateTime
		)
		engine = ReplacingMergeTree(ver) PARTITION BY toYYYYMM(gmt) ORDER BY id SETTINGS index_granularity = 8192;
		
		其中ver 字段类型必须为 日期 或者整数型。
		
  2.2 同一批次 同一分区日期 插入具有相同ID
			insert into test.t_replacing
		    (gmt, id, name, ver)
		values ('2018-01-01 23:00:00', 1, 'a_1', now()),
		       ('2018-01-01 23:00:00', 2, 'a_2', now()),
		       ('2018-01-01 23:00:00', 3, 'a_3', now()),
		       ('2018-01-01 23:00:00', 1, 'a_1', now() + 1);
		       
		 
		结论: 插入后不会立马去重. 执行命令 'optimize table test.t_replacing final;'后. 数据根据主键id取最新(ver最大)的一条
		
		
	
 2.3 不同批次再次插入
	insert into test.t_replacing
	    (gmt, id, name, ver)
	values ('2018-01-04 23:00:00', 1, 'a_12', now()),
	       ('2018-01-04 23:00:00', 2, 'a_22', now());
	       
	 结论: 插入后不会立马去重. 执行命令 'optimize table test.t_replacing final;'后. 数据根据主键id取最新(ver最大)的一条
	
	

 2.4 不同批次,  不同分区日期再次插入
   insert into test.t_replacing
	    (gmt, id, name, ver)
	values ('2018-02-04 23:00:00', 1, 'a_12', now()),
	       ('2018-02-04 23:00:00', 2, 'a_22', now());
	       
	 
    	 
	 结论: 插入后不会立马去重. 执行命令 'optimize table test.t_replacing final;'后. 数据不会根据主键id取最新(ver最大)的一条去重.
	   因此 判断数据去重是 依据分区 + 主键
	 
	 
	 
	 
3. ⚠️

  3.1 不要直接 count 求数量
  
  3.2 使用之前 最好排一下重
  
  
  


4. 生产应用

  4.1 
	
	 
	 
	 
	 


