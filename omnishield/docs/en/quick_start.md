# Quick Start<a name="EN-US_TOPIC_0000002520774918"></a>


**Introduction<a name="section20251624133510"></a>**

This document uses a simple example to help you quickly understand how to use the OmniShield feature.


**Environment Preparation<a name="section176201755124016"></a>**

Set up the environment following instructions in [Installation Guide](./docs/en/installation_guide.md).

**Using OmniShield<a name="section1819738555"></a>**

**Using the Spark SQL Row-based Data Source Encryption and Decryption Feature to Encrypt and Decrypt Data Sources**
1. Deploy the KMS service and create a primary key. (Hadoop provides a KMS service. Using Hadoop KMS as an example, you can create a primary key by running the following command.)
      ```
      hadoop key create key2
      ```
2. In the **/opt/omnishield** directory, run the following command to start Spark SQL:
      ```
      spark-sql --master local --conf spark.sql.extensions=com.huawei.analytics.shield.sql.DataSourceEncryptPlugin --conf spark.hadoop.io.compression.codecs=com.huawei.analytics.shield.crypto.CryptoCodec --jars omnishield-1.0-SNAPSHOT.jar,kms.jar  --conf spark.executor.extraClassPath=omnishield-1.0-SNAPSHOT.jar:kms.jar --driver-class-path omnishield-1.0-SNAPSHOT.jar:kms.jar
      ```
3. In the Spark SQL CLI, run the following SQL statements to create an encrypted data table:
      ```
     drop table if exists test1;
     create table test1 (id int) using csv options (`encrypt` 'true',`keyname` 'key2',`kmstype` 'test.HadoopKeyManagementService',`cryptomode` 'aes/gcm/nopadding',`keylength` '128');
      ```
4. Run the following command in the Spark SQL CLI to check whether the data table is encrypted:
      ```
     show create table test1;
      ```
5. Write data to the table and then read it to verify the encryption and decryption functionality.
