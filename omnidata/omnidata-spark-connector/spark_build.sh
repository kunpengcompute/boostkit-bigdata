#!/bin/bash
mvn clean package
jar_name=`ls -n connector/target/*.jar | grep omnidata-spark | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}'`
dir_name=`ls -n connector/target/*.jar | grep omnidata-spark | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}' | awk -F '.jar' '{print$1}'`
echo "  aaa: $dir_name"
if [ -d "${dir_name}-aarch64" ];then rm -rf ${dir_name}-aarch64; fi
if [ -d "${dir_name}-aarch64.zip" ];then rm -rf ${dir_name}-aarch64.zip; fi
mkdir -p $dir_name-aarch64
cp connector/target/$jar_name $dir_name-aarch64
cd omnidata-spark-connector-lib/
mvn clean package
cd ..
echo "  cd $dir_name-aarch64"
cd $dir_name-aarch64
cp ../omnidata-spark-connector-lib/target/boostkit-omnidata-spark-connector-lib/boostkit-omnidata-spark-connector-lib/* .
cd ..
zip -r -o "${dir_name}-aarch64.zip" "${dir_name}-aarch64"