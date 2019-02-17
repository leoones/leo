#!/bin/bash
version="19.1.8"
config_path='/data/clickhouse/config'
data_path='/data/clickhouse/data'
tmp_path='/data/clickhouse/tmp'
log_path='/data/clickhouse/log'
file_path='/data/clickhouse/user_file'
schema_path='/data/clickhouse/format_schemas'

createdir() {
 if [ ! -d "$1" ]; then
  mkdir -p $1 
 fi
}

ckown() {
chown -R clickhouse:clickhouse $1
}

ckinstall() {
     path="/opt/"
     file="clickhouse-server-base_$1_amd64.deb"
		 if [ -f "$path$file" ]; then
		  dpkg -i $path$file
		 else
      echo "$path$file not exist.You must upload"
      exit 99
     fi 

		 file="clickhouse-server-common_$1_all.deb"
		 if [ -f "$path$file" ]; then
		  dpkg -i $path$file
		 else
		   echo "$path$file not exist.You must upload"
		   exit 99
		 fi
		
		file="clickhouse-client_$1_all.deb"
		 if [ -f "$path$file" ]; then
		  dpkg -i $path$file
		 else
		   echo "  $path$file not exist.You must upload"
		   exit 99
		 fi 
}

sed_ckserver() {
 source='/etc/$PROGRAM'
 sed -i "s#$source#/data/clickhouse/config#g" /etc/init.d/clickhouse-server
 
 sed -i "s#/var/log/clickhouse-server#/data/clickhouse/log#g" /etc/init.d/clickhouse-server
 
 sed -i "s#/var/lib/clickhouse#/data/clickhouse/data#g" /etc/init.d/clickhouse-server 
}

stop() {
 /etc/init.d/clickhouse-server stop
}

start() {
 /etc/init.d/clickhouse-server start
}

sed_config() {

     s='/var/log/clickhouse-server/clickhouse-server.log'
     sed -i "s#$s#$log_path/clickhouse-server.log#g" /etc/clickhouse-server/config.xml.dpkg-dist

     s='/var/log/clickhouse-server/clickhouse-server.err.log'
     sed -i "s#$s#$log_path/clickhouse-server.err.log#g" /etc/clickhouse-server/config.xml.dpkg-dist

     s='/var/lib/clickhouse/'
     sed -i "s#$s#$data_path/#g" /etc/clickhouse-server/config.xml.dpkg-dist

     s='/var/lib/clickhouse/tmp/'
     sed -i "s#$s#$tmp_path/#g" /etc/clickhouse-server/config.xml.dpkg-dist

     s='/var/lib/clickhouse/user_files/'
     sed -i "s#$s#$file_path/#g" /etc/clickhouse-server/config.xml.dpkg-dist

     s='/var/lib/clickhouse/format_schemas/'
     sed -i "s#$s#$schema_path/#g" /etc/clickhouse-server/config.xml.dpkg-dist

     s='<!-- <listen_host>0.0.0.0</listen_host> -->'
     t='<listen_host>0.0.0.0</listen_host>'
     sed -i "s#$s#$t/#g" /etc/clickhouse-server/config.xml.dpkg-dist
     cp /etc/clickhouse-server/config.xml.dpkg-dist $config_path/config.xml
     cp /etc/clickhouse-server/users.xml.dpkg-dist $config_path/users.xml
}
echo '------------------------Start Install ----------------------------'

echo '  ----------------1/7 暂停clickhouse服务-----------------------'
stop

echo '  ----------------2/7 创建相关目录-----------------------'
createdir $config_path
createdir $data_path
createdir $tmp_path
createdir $log_path
createdir $file_path
createdir $schema_path

echo '  ----------------3/7 修改目录权限-----------------------'
ckown /data/clickhouse


echo '  ----------------4/7 安装clickhouse-version:$version 相关包-----------------------'
ckinstall $version

echo '  ----------------5/7 替换clickhouse启动脚本-----------------------'
sed_ckserver

echo '  ----------------6/7 替换clickhouse配置文件-----------------------'
if [ ! -f "$config_path/config.xml" ]; then
 sed_config
fi

echo '  ----------------7/7 替换clickhouse启动脚本-----------------------'
start

echo '------------------------End Install ----------------------------'
