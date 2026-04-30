# 快速入门<a name="ZH-CN_TOPIC_0000002520774918"></a>


**简介<a name="section20251624133510"></a>**

本文档通过一个简单的样例，帮助用户快速了解OmniShield各个特性的使用方法。


**环境准备<a name="section176201755124016"></a>**

请参见 《[安装指南](./docs/zh/installation_guide.md)》 进行环境部署安装。

**使用OmniShield特性<a name="section1819738555"></a>**

**使用Spark-SQL行式数据源加解密特性对数据源进行加解密**
1. 部署相应KMS服务，创建好所需的Primary Key。Hadoop 提供了KMS服务，下面以Hadoop KMS为例，可以通过以下指令在Hadoop KMS中创建Primary Key。
      ```
      hadoop key create key2
      ```
2. 在`/opt/omnishield`目录下执行如下命令启动Spark-SQL。
      ```
      spark-sql --master local --conf spark.sql.extensions=com.huawei.analytics.shield.sql.DataSourceEncryptPlugin --conf spark.hadoop.io.compression.codecs=com.huawei.analytics.shield.crypto.CryptoCodec --jars omnishield-1.0-SNAPSHOT.jar,kms.jar  --conf spark.executor.extraClassPath=omnishield-1.0-SNAPSHOT.jar:kms.jar --driver-class-path omnishield-1.0-SNAPSHOT.jar:kms.jar
      ```
3. 在Spark-SQL命令行执行如下SQL创建加密数据表。 
      ```
     drop table if exists test1;
     create table test1 (id int) using csv options (`encrypt` 'true',`keyname` 'key2',`kmstype` 'test.HadoopKeyManagementService',`cryptomode` 'aes/gcm/nopadding',`keylength` '128');
      ```
4. 在Spark-SQL命令行执行如下SQL检测数据表是否为加密数据表。
      ```
     show create table test1;
      ```
5. 在数据表中先写入数据，在读出数据验证加解密效果。