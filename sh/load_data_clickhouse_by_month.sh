#!/bin/bash
data_path='/oradata/output/'
startDate=20160101
endDate=20190228
while [ $startDate -le $endDate ];
do
stat_mon=$(date -d "$startDate+0days" +%Y%m)
clickhouse-client -h 10.239.33.43 --user=default --password=ycuPocDi --query="alter table ods_hr.$1 drop partition($stat_mon);"
sql="insert into ods_hr.$1 FORMAT CSV"
clickhouse-client -h 10.239.33.43 --user=default --password=ycuPocDi --format_csv_delimiter='|' --input_format_allow_errors_num=10 --query="$sql" < $data_path$1_$stat_mon.txt
if [ $? -eq 0 ]; then
 rm -rf $data_path$1_$stat_mon.txt
fi
echo "----------------- $stat_mon Complete---------------------"
echo $?
startDate=$(date -d "$startDate+1months" +%Y%m%d)
done
