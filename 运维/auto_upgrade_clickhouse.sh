#!/bin/sh
machines=master,slave1,slave2
soft_path=/soft
version=$1

hosts=(${machines//,/ })


#上传安装文件至指定服务器上
for host in ${hosts[@]}
do
 echo $host
 ssh  $host "ls -alt /soft/clickhouse-*${version}_*.deb"
 if [ $? -ne 0  ];then
  echo '>>>>>>>>>>>>> 版本:$version 上传安装文件开始 <<<<<<<<<<<<<<<'
  scp -r $soft_path/clickhouse-common-static_${version}_amd64.deb $host:$soft_path
  scp -r $soft_path/clickhouse-client_${version}_all.deb $host:$soft_path
  scp -r $soft_path/clickhouse-server_${version}_all.deb $host:$soft_path
  echo '>>>>>>>>>>>>>版本:$version 上传安装完成 <<<<<<<<<<<<<<<<<<<'
 fi
done



for host in ${hosts[@]}
do
  echo $host
  echo ">>>>>>>>>>>> 升级clickhouse 版本:$version"
  ssh $host "service clickhouse-server stop"

  ssh $host "dpkg -i $soft_path/clickhouse-common-static_${version}_amd64.deb &&\
             dpkg -i $soft_path/clickhouse-server_${version}_all.deb &&\
             dpkg -i $soft_path/clickhouse-client_${version}_all.deb
             "
  ssh $host "service clickhouse-server start"
done
