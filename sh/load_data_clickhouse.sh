#!/bin/bash
data_path='/data/'
startDate=20150101
endDate=20151231
while [ $startDate -le $endDate ];
do
stat_mon=$(date -d "$startDate+0days" +%Y%m)
sql="insert into ods_hr.$1 FORMAT CSV"
clickhouse-client -h 192.168.89.102 --user=default --password=sMNl+f/n --format_csv_delimiter='|' --input_format_allow_errors_num=10 --query="$sql" < $data_path$1_$stat_mon.txt
echo "----------------- $stat_mon Complete---------------------"
startDate=$(date -d "$startDate+1months" +%Y%m%d)
done
