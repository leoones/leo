--耗时: 1 m 3 s 596 ms
-- 耗时: 3 s 752 ms
select sssd.venderid,
       sum(sssd.saleqty) as act_sale_qty,
       sum(sssd.salevalue) as act_sale_cost
 from ods_hr.sale_shop_sku_day sssd
group by sssd.venderid;

--耗时:  16 s 271 ms
--耗时: 12 s 338 ms
select sssd.venderid,
       sssd.brandid,
       sum(sssd.saleqty) as act_sale_qty,
       sum(sssd.salevalue) as act_sale_cost
 from ods_hr.sale_shop_sku_day sssd
group by sssd.venderid, sssd.brandid;

--耗时: 5 s 350 ms
--耗时: 5 s 93 ms
select count(distinct venderid) as act_venders
  from ods_hr.sale_shop_sku_day;


--耗时: 5 s 54 ms
--耗时: 4 s 764 ms
select count(distinct brandid) as act_brands
  from ods_hr.sale_shop_sku_day;

--耗时：10 s 45 ms
--耗时： 9 s 658 ms
select count(distinct brandid) as act_brands
       ,count(distinct venderid) as act_venders
  from ods_hr.sale_shop_sku_day;

--耗时: 30s
alter table ods_hr.sale_shop_sku_day modify COLUMN venderid LowCardinality(String);
--耗时: 31s
alter table ods_hr.sale_shop_sku_day modify COLUMN brandid LowCardinality(String);

-- 查看各个字段大小
SELECT column,
        any(type) as column_type,
        round(sum(column_data_compressed_bytes) / 1024 / 1024)  as compressed_size,
        round(sum(column_data_uncompressed_bytes) / 1024 / 1024) as uncompressed_size,
        round(compressed_size / (compressed_size +  uncompressed_size), 2) as uncompressed_percent,
        sum(rows) as act_rows
    FROM system.parts_columns
    WHERE (table = 'sale_shop_sku_day')
      AND active
      --AND (column LIKE '%CityName')
    GROUP BY column
    ORDER BY column ASC
