#!/bin/bash
mvn clean package
jar_name=`ls -n connector/target/*.jar | grep hive-exec | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}'`
dir_name=`ls -n connector/target/*.jar | grep hive-exec | awk -F ' ' '{print$9}' | awk -F '/' '{print$3}' | awk -F '.jar' '{print$1}'`
rm -r $dir_name
rm -r $dir_name.zip
mkdir -p $dir_name
cp connector/target/$jar_name $dir_name
cd $dir_name
wget https://mirrors.huaweicloud.com/repository/maven/org/bouncycastle/bcpkix-jdk15on/1.68/bcpkix-jdk15on-1.68.jar
wget https://mirrors.huaweicloud.com/repository/maven/org/bouncycastle/bcprov-jdk15on/1.68/bcprov-jdk15on-1.68.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar
wget https://mirrors.huaweicloud.com/repository/maven/io/hetu/core/hetu-transport/1.6.1/hetu-transport-1.6.1.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/core/jackson-annotations/2.13.2/jackson-annotations-2.13.2.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/core/jackson-core/2.13.2/jackson-core-2.13.2.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/core/jackson-databind/2.13.2.1/jackson-databind-2.13.2.1.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/datatype/jackson-datatype-guava/2.12.4/jackson-datatype-guava-2.12.4.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.12.4/jackson-datatype-jdk8-2.12.4.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/datatype/jackson-datatype-joda/2.13.3/jackson-datatype-joda-2.13.3.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/datatype/jackson-datatype-jsr310/2.12.4/jackson-datatype-jsr310-2.12.4.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/fasterxml/jackson/module/jackson-module-parameter-names/2.12.4/jackson-module-parameter-names-2.12.4.jar
wget https://mirrors.huaweicloud.com/repository/maven/org/jasypt/jasypt/1.9.3/jasypt-1.9.3.jar
wget https://mirrors.huaweicloud.com/repository/maven/org/openjdk/jol/jol-core/0.2/jol-core-0.2.jar
wget https://repo1.maven.org/maven2/io/airlift/joni/2.1.5.3/joni-2.1.5.3.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/esotericsoftware/kryo-shaded/4.0.2/kryo-shaded-4.0.2.jar
wget https://mirrors.huaweicloud.com/repository/maven/io/airlift/log/0.193/log-0.193.jar
wget https://mirrors.huaweicloud.com/repository/maven/io/perfmark/perfmark-api/0.23.0/perfmark-api-0.23.0.jar
wget https://mirrors.huaweicloud.com/repository/maven/io/hetu/core/presto-main/1.6.1/presto-main-1.6.1.jar
wget https://mirrors.huaweicloud.com/repository/maven/io/hetu/core/presto-spi/1.6.1/presto-spi-1.6.1.jar
wget https://mirrors.huaweicloud.com/repository/maven/com/google/protobuf/protobuf-java/3.12.0/protobuf-java-3.12.0.jar
wget https://mirrors.huaweicloud.com/repository/maven/io/airlift/slice/0.38/slice-0.38.jar
cd ..
zip -r -o $dir_name.zip $dir_name
rm -r $dir_name