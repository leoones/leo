https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00945_bloom_filter_index.sql










SET allow_experimental_data_skipping_indices = 1;

-- 测试对 null 影响
CREATE TABLE default.test_bloomfilter
(
    `key` LowCardinality(Nullable(String)),
    `key_no_idx` LowCardinality(Nullable(String)),
    `id` String,
    `dt` DateTime,
    INDEX key_bf key TYPE bloom_filter(0.01) GRANULARITY 8192
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;
                                
SELECT * from test_bloomfilter where key = NULL;
SELECT * from test_bloomfilter where isNull(key);                                
                                
---测试 Group by operation on LowCardinality 影响
CREATE TABLE default.perf_bloomfilter                       
(                                                           
    `d1` LowCardinality(Nullable(String)),                  
    `d2` String,                                            
    `id` String,                                            
    `dt` DateTime,                                          
    INDEX idx_bf d1 TYPE bloom_filter(0.01) GRANULARITY 8192
)                                                           
ENGINE = MergeTree()                                        
PARTITION BY toYYYYMMDD(dt)                                 
ORDER BY id;
                        
SELECT               
    d2,              
    count()          
FROM perf_bloomfilter
GROUP BY d2;
                        
SELECT               
    d1,              
    count()          
FROM perf_bloomfilter
GROUP BY d1   
