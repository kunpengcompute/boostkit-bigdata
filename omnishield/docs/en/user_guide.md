# User Guide<a name="EN-US_TOPIC_0000002520471488"></a>

## Using the Feature<a name="EN-US_TOPIC_0000002520471492"></a>

### Encrypting and Decrypting DataFrame Row-based Data Sources<a name="EN-US_TOPIC_0000002551631467"></a>

When using the OmniShield feature to execute Spark services, start Spark to submit tasks.

1. Deploy the KMS service and create the primary key. For example, you can run the following command to start the Hadoop KMS and create a primary key:
    1. Open the Hadoop configuration file.

        ```
        cd $HADOOP_HOME/etc/hadoop
        vi core-site.xml
        ```

    2. Press  **i**  to enter the insert mode and add the following content to the  **<configuration\>**  tag:

        ```
        <property>
          <name>hadoop.security.key.provider.path</name>
          <value>kms://http@x.x.x.x:9600/kms</value>
        </property>
        ```

    3. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.
    4. Copy the  **core-site.xml**  file to Spark.

        ```
        cp $HADOOP_HOME/etc/hadoop/core-site.xml $SPARK_HOME/conf/
        ```

    5. Start the Hadoop KMS.

        ```
        cd $HADOOP_HOME/sbin
        sh kms.sh start
        ```

    6. Create a key.

        ```
        hadoop key create key1
        ```

        >![](public_sys-resources/icon-note.gif) **NOTE:** 
        >-   When modifying the  **core-site.xml**  file, replace  **x.x.x.x**  in the second step with the actual IP address.
        >-   If  **key1**  already exists, run the  **hadoop key delete key1**  command to delete  **key1**  before creating it.

2. Generate the CSV data source file to be encrypted in the  **/home**  directory.
    1. Open the  **simple.csv**  file.

        ```
        vi simple.csv
        ```

    2. Press  **i**  to enter the insert mode and add the following content to the file:

        ```
        name,age,job
        user1,12,Engineer
        user2,13,Engineer
        user3,14,Developer
        user4,15,Engineer
        user5,16,Engineer
        ```

    3. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.

3. In the  **/opt/omnishield**  directory, run the following command to encrypt the data source. After the command is executed, the  **/home/en**  directory is generated, and the encrypted  **simple.csv**  file is stored in this directory.

    ```
    spark-submit --class com.huawei.analytics.shield.utils.Encrypt --master local --conf spark.hadoop.io.compression.codecs=com.huawei.analytics.shield.crypto.CryptoCodec  --conf spark.shield.primaryKey.name=key1 --conf spark.shield.primaryKey.key1.kms.type=test.example.HadoopKeyManagementService --jars omnishield-1.0-SNAPSHOT.jar,kms.jar omnishield-1.0-SNAPSHOT.jar  -i file:///home/simple.csv -o file:///home/en -a AES/GCM/NOPadding -t csv -e encrypt
    ```

4. In the  **/opt/omnishield**  directory, run the following command to decrypt the data source. After the command is executed, the  **/home/de**  directory is generated, and the decrypted file is stored in this directory.

    ```
    spark-submit --class com.huawei.analytics.shield.utils.Encrypt --master local --conf spark.hadoop.io.compression.codecs=com.huawei.analytics.shield.crypto.CryptoCodec  --conf spark.shield.primaryKey.name=key1 --conf spark.shield.primaryKey.key1.kms.type=test.example.HadoopKeyManagementService --jars omnishield-1.0-SNAPSHOT.jar,kms.jar omnishield-1.0-SNAPSHOT.jar  -i file:///home/en -o file:///home/de -a AES/GCM/NOPadding -t csv -e decrypt
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >-   The  **spark.shield.primaryKey**  parameter specifies the primary key in use. This key must be created in the KMS in advance.
    >-   The  **spark.shield.primaryKey.key1.kms.type**  parameter specifies the KMS type of the primary key. Implement the KMS by yourself based on the APIs provided by OmniShield. The value of this parameter must include the specific class path of the KMS in the  **kms.jar**  package.
    >-   The  **-i**  and  **-o**  options are used to specify the input and output paths, respectively. Both HDFS and local file systems are supported.
    >    -   In encryption mode,  **-i**  specifies the input data file and  **-o**  specifies the output folder for the encrypted data.
    >    -   In decryption mode,  **-i**  specifies the input folder containing encrypted data and  **-o**  specifies the output folder for the decrypted data.
    >-   **-a**  specifies the encryption algorithm, which must be AES/GCM/NOPadding.
    >-   **-t**  specifies the data source file format, which is CSV, JSON, or TXT.
    >-   **-e**  specifies the working mode, which is  **encrypt**  or  **decrypt**.

### Encrypting and Decrypting Spark SQL Row-based Data Sources<a name="EN-US_TOPIC_0000002520631472"></a>

When using the OmniShield feature to execute Spark services, start Spark to submit tasks.

1. Deploy the KMS service and create the primary key. For example, you can run the following command to create a primary key in the Hadoop KMS:

    ```
    hadoop key create key2
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >The procedure for starting the Hadoop KMS is the same as that in  [1](encrypting-and-decrypting-dataframe-row-based-data-sources.md#en-us_topic_0000001984634968_en-us_topic_0000001842867824_en-us_topic_0000001519205317_li39371321142320). If the Hadoop KMS has been started, you do not need to start the KMS again when creating a key.

2. In the  **/opt/omnishield**  directory, run the following command to start Spark SQL:

    ```
    spark-sql --master local --conf spark.sql.extensions=com.huawei.analytics.shield.sql.DataSourceEncryptPlugin --conf spark.hadoop.io.compression.codecs=com.huawei.analytics.shield.crypto.CryptoCodec --jars omnishield-1.0-SNAPSHOT.jar,kms.jar  --conf spark.executor.extraClassPath=omnishield-1.0-SNAPSHOT.jar:kms.jar --driver-class-path omnishield-1.0-SNAPSHOT.jar:kms.jar
    ```

3. <a name="en-us_topic_0000002021075065_li1973462017298"></a>In the Spark SQL CLI, run the following SQL statement to create an encrypted data table:

    ```
    drop table if exists test1;
    create table test1 (id int) using csv options (`encrypt` 'true',`keyname` 'key2',`kmstype` 'test.HadoopKeyManagementService',`cryptomode` 'aes/gcm/nopadding',`keylength` '128');
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >-   The  **encrypt**  parameter indicates whether to use the encrypted table. If the value is  **true**, the encrypted table is used.
    >-   The  **keyname**  parameter specifies the primary key in use. This key must be created in the KMS in advance.
    >-   The  **kmstype**  parameter specifies the KMS type of the primary key. Implement the KMS by yourself based on the APIs provided by OmniShield. The value of this parameter must include the specific class path of the KMS in the  **kms.jar**  package.
    >-   The  **cryptomode**  parameter specifies the encryption algorithm, which is AES/GCM/NOPadding or SM4/GCM/NoPadding.
    >-   The  **keylength**  parameter specifies the length of the encrypted key. For AES/GCM/NOPadding, the key length is  **128**  or  **256**; for SM4/GCM/NoPadding, the key length is  **128**.
    >-   You can create different types of data tables by specifying the data source format. For example, to create a data table in JSON format, replace  **using json**  with  **using csv**  in  [3](#en-us_topic_0000002021075065_li1973462017298). The table type is CSV, JSON, or TXT.

4. Run the following command in the Spark SQL CLI to check whether the data table is encrypted:

    ```
    show create table test1;
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >If the command output contains  **encryptdatakey**  in  **OPTIONS**, the table is encrypted.

### Encrypting and Decrypting Spark SQL ORC Data Sources Using SM Algorithms<a name="EN-US_TOPIC_0000002551631473"></a>

When using the OmniShield feature to execute Spark services, start Spark to submit tasks.

1. Deploy the Hadoop KMS service and create the primary key.

    ```
    hadoop key create key3 -cipher 'SM4/GCM/NoPadding'
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >The procedure for starting the Hadoop KMS is the same as that in  [1](encrypting-and-decrypting-dataframe-row-based-data-sources.md#en-us_topic_0000001984634968_en-us_topic_0000001842867824_en-us_topic_0000001519205317_li39371321142320). If the Hadoop KMS has been started, you do not need to start the KMS again when creating a key.

2. In the  **/opt/omnishield**  directory, run the following command to start Spark SQL:

    ```
    spark-sql --master local --conf spark.sql.extensions=com.huawei.analytics.shield.sql.DataSourceEncryptPlugin --conf spark.hadoop.io.compression.codecs=com.huawei.analytics.shield.crypto.CryptoCodec --conf spark.sql.orc.filterPushdown=false --jars omnishield-1.0-SNAPSHOT.jar,kms.jar  --conf spark.executor.extraClassPath=omnishield-1.0-SNAPSHOT.jar:kms.jar --driver-class-path omnishield-1.0-SNAPSHOT.jar:kms.jar
    ```

3. In the Spark SQL CLI, run the following SQL statement to create an encrypted data table:

    ```
    drop table if exists otest1;
    create table otest1 (name string) options ( hadoop.security.key.provider.path "kms://http@IP:PORT/kms", orc.key.provider "hadoop", orc.encrypt "key3:name") stored as orc;
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >-   The  **hadoop.security.key.provider.path**  parameter specifies the IP address and port of the Hadoop KMS.
    >-   The  **orc.encrypt**  parameter specifies the column to be encrypted.

4. Run the following command in the Spark SQL CLI to check whether the data table is encrypted:

    ```
    describe extended otest1;
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >If the command output contains  **orc.encrypt**  in  **Storage Properties**, the table is encrypted.

### Enabling SM Algorithms for Shuffle Secure I/Os<a name="EN-US_TOPIC_0000002520631464"></a>

When using the OmniShield feature to execute Spark services, start Spark to submit tasks.

1. Run the following SQL statements in the  **/opt/omnishield**  directory and enable shuffle I/O encryption:

    ```
    spark-sql --driver-cores 5 --driver-memory 20g --num-executors 6 --executor-cores 4 --executor-memory 20g --conf spark.executor.memoryOverhead=20g --master yarn --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=18g --conf spark.task.cpus=1 --conf spark.driver.maxResultSize=40g  --conf spark.authenticate.secret=testSecret  --conf spark.authenticate=true --conf spark.io.encryption.enabled=true  --conf spark.io.encryption.commons.config.cipher.classes=com.huawei.boostkit.omnishield.cipher.SM4Cipher --conf spark.io.crypto.cipher.transformation=SM4/GCM/NOPadding  --conf spark.network.crypto.enabled=true  --conf spark.network.crypto.cipher=SM4/GCM/NOPadding  --conf spark.driver.extraClassPath=omnishield-1.0-SNAPSHOT.jar --conf spark.executor.extraClassPath=omnishield-1.0-SNAPSHOT.jar  --jars omnishield-1.0-SNAPSHOT.jar --database tpcds_bin_partitioned_varchar_orc_2 -f /automated/lib/bd/testcase/scripts/omniop/conf/Spark_OmniRuntime_ShuffleEnhance/Spark_OmniRuntime_ShuffleEnhance_002.sql
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >-   To enable shuffle drive I/O encryption, modify the following command parameters:
    >    -   spark.io.encryption.enabled=true
    >    -   spark.io.encryption.commons.config.cipher.classes=_Customized cipher class_
    >    -   spark.io.crypto.cipher.transformation=SM4/GCM/NOPadding
    >-   To enable shuffle network I/O encryption, modify the following command parameters:
    >    -   spark.authenticate.secret=_Customized key_
    >    -   spark.authenticate=true
    >    -   spark.network.crypto.enabled=true
    >    -   spark.network.crypto.cipher=SM4/GCM/NOPadding

2. Check whether the setting of SM algorithms for shuffled I/Os has taken effect.
    1. Obtain the Executor log file.

        ```
        yarn logs --applicationId application_xxx > xxx.log
        ```

    2. Check whether there is SM algorithm information. If  **Shuffle network IO encryption will use SM4/GCM/NOPadding algorithm**  and  **Shuffle disk IO encryption will use SM4/GCM/NOPadding algorithm**  are displayed, the setting has taken effect.

        ![](figures/en-us_image_0000002551631477.png)

### Performing Spark on Yarn Application-Level Remote Attestation<a name="EN-US_TOPIC_0000002520471486"></a>

1. <a name="li6709341143217"></a>Add a user for IMA measurement to the virtCCA confidential VM \(cVM\).

    ```
    useradd attest
    ```

2. Configure the IMA measurement policy on the virtCCA cVM.
    1. Open the  **policy**  file.

        ```
        vi policy
        ```

    2. Press  **i**  to enter the insert mode and add the following content to the file:

        ```
        measure func=FILE_CHECK mask=MAY_READ fowner=1001
        ```

        >![](public_sys-resources/icon-note.gif) **NOTE:** 
        >-   **1001**  is the ID of the  **attest**  user in  [1](#li6709341143217). Replace the ID specified by  **fowner**  in the  **policy**  file with the actual one. You can run  **cat /etc/passwd | grep attest**  to view the user ID.
        >-   **func=FILE\_CHECK**  indicates that the IMA measurement object is a file.
        >-   **mask=MAY\_READ**  indicates that the IMA measurement is triggered by a read operation.
        >-   **fowner=1001**  indicates that the owner ID of the file to be measured is  **1001**.
        >-   When the three parameter rules are combined, IMA measurement is triggered when the file whose owner is  **1001**  is read.

    3. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.

3. Make the IMA measurement policy take effect on the virtCCA cVM.

    ```
    cat policy > /sys/kernel/security/ima/policy
    ```

4. Configure the environment variables of the IMA measurement user on the virtCCA cVM.
    1. Open the  **/etc/profile**  file.

        ```
        vi /etc/profile
        ```

    2. Press  **i**  to enter the insert mode and add the following content to the file:

        ```
        export IMA_USER=attest
        ```

        >![](public_sys-resources/icon-note.gif) **NOTE:** 
        >**IMA\_USER**  is the name of the user you added in  [1](#li6709341143217).

    3. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.

5. Submit a Spark job on the ResourceManager node of Yarn and perform remote attestation. For example, run the following commands to start the job of calculating the Pi value in the Spark example:

    ```
    spark-submit --master yarn  --class org.apache.spark.examples.SparkPi --num-executors 1  --executor-cores 1 --executor-memory 1G --conf spark.remote.attestation.enable=true  --conf spark.remote.attestation.agent.class=rats.RatsTLSRemoteAttestationAgent --conf spark.network.timeout=1200 --conf spark.yarn.max.executor.failures=1 --conf spark.yarn.maxAppAttempts=1  --conf spark.executor.extraClassPath=omnishield-1.0-SNAPSHOT.jar:rats-tls-demo-1.0.jar --driver-class-path omnishield-1.0-SNAPSHOT.jar:rats-tls-demo-1.0.jar  --jars "omnishield-1.0-SNAPSHOT.jar,rats-tls-demo-1.0.jar" /usr/local/spark/examples/jars/spark-examples_2.12-3.3.1.jar
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >-   **rats-tls-demo-1.0.jar**  is the JAR package where the class specified by  **spark.remote.attestation.agent.class**  resides, which needs to be implemented by yourself. This step uses virtcca rats-tls as an example. For details, see  [Connecting OmniShield to virtcca rats-tls](https://gitee.com/kunpengcompute/boostkit-bigdata/blob/main/omnishield/src/test/scala/com/huawei/analytics/shield/ra/example/README.md).
    >-   **spark.remote.attestation.enable**  is a new configuration item in OmniShield. It is used to configure whether to perform remote attestation during Executor startup.  **true**  indicates that remote attestation is required when starting the Executor, whereas  **false**  indicates that remote attestation is not required.
    >-   **spark.remote.attestation.agent.class**  is a new configuration item in OmniShield. It is used to configure the class name of remote attestation performed by the Executor. You need to implement the interconnection by yourself.
    >-   The Spark configuration items  **spark.yarn.max.executor.failures=1**  and  **spark.yarn.maxAppAttempts=1**  are used to set the number of retry times after Spark on Yarn Executor fails to be started. If remote attestation fails, the Spark Executor fails to be started. You can use the preceding two parameters to control the number of retry times.



## Maintaining the Feature<a name="EN-US_TOPIC_0000002551511475"></a>

Follow the operating instructions when upgrading or uninstalling OmniShield.

**Upgrading OmniShield<a name="section018614194301"></a>**

>![](public_sys-resources/icon-notice.gif) **NOTICE:** 
>The upgrade cannot be performed using the tool. Therefore, you need to download the installation package and reinstall the software.

Contact Huawei technical support to download the OmniShield software installation package for the upgrade.

**Uninstalling OmniShield<a name="section1397016019314"></a>**

>![](public_sys-resources/icon-notice.gif) **NOTICE:** 
>-   These steps are not mandatory for deploying OmniShield.
>-   Before uninstalling OmniShield, stop all services and perform the uninstallation operations on every node where OmniShield is installed.

Go to the  **/opt/omnishield**  directory and delete the OmniShield JAR package.

```
cd /opt/omnishield
rm -rf omnishield-1.0-SNAPSHOT.jar
```
