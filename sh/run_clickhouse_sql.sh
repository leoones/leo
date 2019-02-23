#!/bin/bash

startDate=$1
endDate=$2
startSec=`date -d "$startDate" "+%s"`
endSec=`date -d "$endDate" "+%s"`
for((i=$startSec;i<=$endSec;i+=86400))
do
    stat_day=`date -d "@$i" "+%Y-%m-%d"`
    echo "$stat_day"
done