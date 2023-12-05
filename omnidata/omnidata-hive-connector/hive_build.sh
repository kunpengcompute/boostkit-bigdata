#!/bin/bash
mvn clean package
jar_name=`ls -n connector/target/*.jar | grep hive-exec | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}'`
dir_name=`ls -n connector/target/*.jar | grep hive-exec | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}' | awk -F '.jar' '{print$1}'`
if [ -d "${dir_name}" ];then rm -rf ${dir_name}; fi
if [ -d "${dir_name}.zip" ];then rm -rf ${dir_name}.zip; fi
mkdir -p $dir_name
cp connector/target/$jar_name $dir_name
cd omnidata-hive-connector-lib/
mvn clean package
cd ..
cd $dir_name
cp ../omnidata-hive-connector-lib/target/boostkit-omnidata-hive-connector-lib/boostkit-omnidata-hive-connector-lib/* .
cd ..
zip -r -o $dir_name.zip $dir_name