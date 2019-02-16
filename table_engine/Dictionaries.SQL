---------------------------------------------------- 字典表 ----------------------------------------------------

用途：
   
   字典表主要用于维度表。 特别是小维度表和数量大的事实表 若用Join关联,性能比不上直接用字典表使用.



配置： 
   在配置文件 config.xml  元素
    <dictionaries_config>*_dictionary.xml</dictionaries_config>
    
    

配置文件样式:

		<dictionary>
		    <!-- 维度表名字 -->
		    <name>dw_category_info</name>
		    
		    <!-- 维度表数据来源 包括localfile, HTTPS, mysql, clickhouse, mongodb, odbc -->
		    <source>
		       <clickhouse>
				    <host>192.168.89.102</host>
				    <port>9000</port>
				    <user>default</user>
				    <password>sMNl+f/n</password>
				    <db>dw_hr</db>
				    <table>dw_category_info</table>
			     </clickhouse>
		    </source>
		
		    <!-- 定义维度表 主键id的类型 包括  
		       flat：单字段整型。 最大值为 500000
		       hashed：单字段整型。 无限制
		       cache：
		       range_hashed：单字段整型。
		       complex_key_hashed：多字段
		       complex_key_cache：多字段
		       ip_trie
		     -->
		    <layout>
		      <complex_key_hashed/>
		    </layout>
		
		    <structure>
		    
		      <key>
				     <attribute>
				       <name>categorytreeid</name>
				        <type>UInt8</type>
				      </attribute>
				     <attribute>
				       <name>categoryid</name>
				        <type>UInt32</type>
				      </attribute>
				   </key>
   
				   <attribute>
				     <name>categoryname</name>
				     <type>String</type>
				     <null_value>未知</null_value>
				   </attribute>
				   
				   <attribute>
				     <name>mid_categoryid</name>
				     <type>UInt32</type>
				     <null_value>0</null_value>
				   </attribute>
				   
				   <attribute>
				     <name>mid_categoryname</name>
				     <type>String</type>
				     <null_value>未知</null_value>
				   </attribute>
		  </structure>
		    
		    <!-- 数据刷新间隔 -->
		    <lifetime>
		      <min>300</min>
          <max>360</max>
		    </lifetime>
		    
		</dictionary>
		
		
		
样例：
  
	select dictGetString('dw_buinfo', 'buname', tuple(frdssvd.out_buid)) as buname
	 from dw_hr.fct_rt_dc_shop_sku_vender_day frdssvd limit  100;
	 
	 
	其中: 'dw_buinfo' 维度表名
	      ‘buname’   维度表列名
      
      

