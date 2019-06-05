#!/bin/bash
soft_path=/soft
base_path='/data/clickhouse'
data_path="$base_path/data"
tmp_pth="$base_path/tmp"
log_path="$base_path/log"
file_path="$base_path/user_file"
schema_path="$base_path/format_schema"
version="$1"

createdir() {
 if [ ! -d "$1" ]; then
  mkdir -p $1
 fi
}

stopdb() {
 if [ -f "/etc/init.d/clickhouse-server" ]; then
  /etc/init.d/clickhouse-server stop
 fi
}

startdb() {
 /etc/init.d/clickhouse-server start
}


config() {
 #set confi elemet <log> </log> , <errorlog> </errorlog>
 s='/var/log/clickhouse-server'
 sed -i "s#$s#$log_path#g" /etc/clickhouse-server/config.xml 
 
 #set config.xml data paht <path></path>
 s='/var/lib/clickhouse'
 sed -i "s#$s#$data_path#g" /etc/clickhouse-server/config.xml

 #set config.xml tmp_path <tmp_path></tmp_path>
 s='/var/lib/clickhouse/tmp/'
 sed -i "s#$s#$tmp_path/#g" /etc/clickhouse-server/config.xml

 #set config.xml element user_files_path
 s='/var/lib/clickhouse/user_files'
 sed -i "s#$s#$file_path#g" /etc/clickhouse-server/config.xml

 #set config.xml schema_path
 s='/var/lib/clickhouse/format_schemas'
 sed -i "s#$s#$schema_path#g" /etc/clickhouse-server/config.xml
 
 #set config.xml listen_host
 s='<!-- <listen_host>0.0.0.0</listen_host> -->'
 t='<listen_host>0.0.0.0</listen_host>'
 sed -i "s#$s#$t#g" /etc/clickhouse-server/config.xml
 
 #set /etc/init.d/clickhouse-server
 s='CLICKHOUSE_LOGDIR=/var/log/clickhouse-server'
 t='CLICKHOUSE_LOGDIR=/data/clickhouse/log'
 sed -i "s#$s#$t#g" /etc/init.d/clickhouse-server
 
 s='CLICKHOUSE_DATADIR=/var/lib/clickhouse'
 t='CLICKHOUSE_DATADIR=/data/clickhouse/data'
 sed -i "s#$s#$t#g" /etc/init.d/clickhouse-server
 
 s='CLICKHOUSE_DATADIR_OLD=/opt/clickhouse'
 t='CLICKHOUSE_DATADIR_OLD=/data/clickhouse/data'
 sed -i "s#$s#$t#g" /etc/init.d/clickhouse-server

} 

installdb() {
 pkg="clickhouse-server-common-$1-1.el6.x86_64"
 flag=`yum list | grep "$pkg" | wc -l`
 
 if [ "$flag" -eq "1" ]; then
  echo "CLICKHOUSE-$1 have aready installed"
 else
  total=`yum list | grep 'clickhouse' | wc -l`

  if [ $total -ne '0' ]; then
  yum list | grep 'clickhouse' | xargs yum -y remove
  fi

  yum -y install $soft_path/$pkg

  pkg="clickhouse-common-static-$1-1.el6.x86_64.rpm"
  yum -y install $soft_path/$pkg

  pkg="clickhouse-server-$1-1.el6.x86_64.rpm"
  yum -y install $soft_path/$pkg

  pkg="clickhouse-client-$1-1.el6.x86_64.rpm"
  yum -y install $soft_path/$pkg
  
 fi

}
echo '-------------------------- Starting -----------------------'
echo '   ----------------------- 服务停止 -------------------    '
stopdb
echo '   ----------------------- 创建相关目录 -------------------    '
createdir $data_path
createdir $tmp_pth
createdir $log_path
createdir $file_path
createdir $schema_path
echo '   ----------------------- CLICKHOUSE 安装 -------------------    '

installdb $version

echo '   ----------------------- CLICKHOUSE 配置 -------------------    '
config

chown -R clickhouse:clickhouse $base_path
echo '   ----------------------- CLICKHOUSE 启动 -------------------    '

startdb
echo '-------------------------- Complete -----------------------'
