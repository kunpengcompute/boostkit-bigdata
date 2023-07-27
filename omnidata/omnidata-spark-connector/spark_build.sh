#!/bin/bash
mvn clean package
jar_name=`ls -n connector/target/*.jar | grep omnidata-spark | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}'`
dir_name=`ls -n connector/target/*.jar | grep omnidata-spark | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}' | awk -F '.jar' '{print$1}'`
if [ -d "${dir_name}-aarch64" ];then rm -rf ${dir_name}-aarch64; fi
if [ -d "${dir_name}-aarch64.zip" ];then rm -rf ${dir_name}-aarch64.zip; fi
mkdir -p $dir_name-aarch64
cp connector/target/$jar_name $dir_name-aarch64
cd $dir_name-aarch64
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/bcpkix-jdk15on/1.68/package/bcpkix-jdk15on-1.68.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/curator-client/2.12.0/package/curator-client-2.12.0.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/curator-framework/2.12.0/package/curator-framework-2.12.0.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/curator-recipes/2.12.0/package/curator-recipes-2.12.0.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/fastjson/1.2.76/package/fastjson-1.2.76.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/fst/2.57/package/fst-2.57.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/guava/26.0-jre/package/guava-26.0-jre.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/hetu-transport/1.6.1/package/hetu-transport-1.6.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jackson-datatype-guava/2.12.4/package/jackson-datatype-guava-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/jackson-datatype-jdk8/2.12.4/package/jackson-datatype-jdk8-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/Jackson-datatype-Joda/2.12.4/package/jackson-datatype-joda-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/jackson-datatype-jsr310/2.12.4/package/jackson-datatype-jsr310-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc-hgh-artifactory.cmc.tools.huawei.com/artifactory/opensource_general/jackson-module-parameter-names/2.12.4/package/jackson-module-parameter-names-2.12.4.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jasypt/1.9.3/package/jasypt-1.9.3.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/jol-core/0.2/package/jol-core-0.2.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/joni/2.1.5.3/package/joni-2.1.5.3.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/log/0.193/package/log-0.193.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/perfmark-api/0.23.0/package/perfmark-api-0.23.0.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/presto-main/1.6.1/package/presto-main-1.6.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/presto-spi/1.6.1/package/presto-spi-1.6.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/protobuf-java/3.12.0/package/protobuf-java-3.12.0.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/slice/0.38/package/slice-0.38.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/bytecode/1.2/package/bytecode-1.2.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/fastutil/6.5.9/package/fastutil-6.5.9.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/json/206/package/json-206.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/lucene-analyzers-common/7.2.1/package/lucene-analyzers-common-7.2.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/presto-parser/1.6.1/package/presto-parser-1.6.1.jar
wget --proxy=off --no-check-certificate https://cmc.cloudartifact.szv.dragon.tools.huawei.com/artifactory/opensource_general/units/1.3/package/units-1.3.jar
wget --proxy=off --no-check-certificate https://cmc.centralrepo.rnd.huawei.com/artifactory/maven-central-repo/io/airlift/stats/0.193/stats-0.193.jar
wget --proxy=off --no-check-certificate https://cmc.centralrepo.rnd.huawei.com/artifactory/maven-central-repo/io/hetu/core/presto-expressions/1.6.1/presto-expressions-1.6.1.jar
cd ..
zip -r -o $dir_name-aarch64.zip $dir_name-aarch64