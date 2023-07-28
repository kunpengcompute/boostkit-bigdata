#!/bin/bash
mvn clean package
jar_name=`ls -n connector/target/*.jar | grep hive-exec | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}'`
dir_name=`ls -n connector/target/*.jar | grep hive-exec | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}' | awk -F '.jar' '{print$1}'`
if [ -d "${dir_name}" ];then rm -rf ${dir_name}; fi
if [ -d "${dir_name}.zip" ];then rm -rf ${dir_name}.zip; fi
mkdir -p $dir_name
cp connector/target/$jar_name $dir_name
cd $dir_name
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/bcpkix-jdk15on/1.68/package/bcpkix-jdk15on-1.68.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/guava/31.1-jre/package/guava-31.1-jre.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/hetu-transport/1.6.1/package/hetu-transport-1.6.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jackson-annotations/2.12.4/package/jackson-annotations-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jackson-core/2.12.4/package/jackson-core-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jackson-databind/2.12.4/package/jackson-databind-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jackson-datatype-guava/2.12.4/package/jackson-datatype-guava-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/jackson-datatype-jdk8/2.12.4/package/jackson-datatype-jdk8-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/Jackson-datatype-Joda/2.12.4/package/jackson-datatype-joda-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/jackson-datatype-jsr310/2.12.4/package/jackson-datatype-jsr310-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/jackson-module-parameter-names/2.12.4/package/jackson-module-parameter-names-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jasypt/1.9.3/package/jasypt-1.9.3.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jol-core/0.2/package/jol-core-0.2.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/joni/2.1.5.3/package/joni-2.1.5.3.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/kryo-shaded/4.0.2/package/kryo-shaded-4.0.2.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/log/0.193/package/log-0.193.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/perfmark-api/0.23.0/package/perfmark-api-0.23.0.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/presto-main/1.6.1/package/presto-main-1.6.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/presto-spi/1.6.1/package/presto-spi-1.6.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/protobuf-java/3.12.0/package/protobuf-java-3.12.0.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/slice/0.38/package/slice-0.38.jar
cd ..
zip -r -o $dir_name.zip $dir_name