#!/bin/sh
machines=master,slave1,slave2
soft_path=/soft
version=$1

hosts=(${machines//,/ })


#上传安装文件至指定服务器上
for host in ${hosts[@]}
do
 # ssh  $host "ls -alt /soft/clickhouse-*${version}_*.deb"
 #if [ $? -ne 0  ];then
  echo '>>>>>>>>>>>>> 开始上传安装文件至机器:$host <<<<<<<<<<<<<<<'
  scp -r $soft_path/clickhouse-common-static_${version}_amd64.deb $host:$soft_path
  scp -r $soft_path/clickhouse-client_${version}_all.deb $host:$soft_path
  scp -r $soft_path/clickhouse-server_${version}_all.deb $host:$soft_path
  echo '>>>>>>>>>>>>> 完成上传安装文件至机器:$host <<<<<<<<<<<<<<<<<<<'
 #fi
done


for host in ${hosts[@]}
do
  echo ">>>>>>>>>>>>机器：$host 升级clickhouse 版本:$version"
  ssh $host "service clickhouse-server stop && \
             dpkg -i $soft_path/clickhouse-common-static_${version}_amd64.deb &&\
             dpkg -i $soft_path/clickhouse-server_${version}_all.deb &&\
             dpkg -i $soft_path/clickhouse-client_${version}_all.deb &&\
             service clickhouse-server start
             "
done
