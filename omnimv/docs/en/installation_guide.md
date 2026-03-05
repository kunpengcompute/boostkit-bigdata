# Installation Guide<a name="EN-US_TOPIC_0000002551410827"></a>

## Installation Overview<a name="EN-US_TOPIC_0000002519970740"></a>

### Network Planning<a name="EN-US_TOPIC_0000002551410855"></a>

You are advised to use a storage and compute coupled network, that is, use nodes as both storage and compute nodes.

The environment planned in this document for OmniMV consists of four servers, which are one management node and three compute nodes. A storage and compute coupled network is used. In the following, we will be using HDFS on the storage nodes for illustration:

- The management node is server1, which is used to manage tasks.
- The compute nodes are agent1, agent2, and agent3, which are used to run OmniMV query engine services and store datasets.

A server can function as a management node and a compute node at the same time. In single-node mode, operations performed on the management node or compute node mentioned in the following sections are performed on the same node.  [Figure 1](#en-us_topic_0000001713019717_fig195021730155013)  shows the networking diagram.

**Figure  1**  Networking diagram<a name="en-us_topic_0000001713019717_fig195021730155013"></a>  
![](figures/networking-diagram.png "networking-diagram")

You are advised to use a storage and compute coupled network, that is, use nodes as both storage and compute nodes.
### Environment Requirements<a name="EN-US_TOPIC_0000002551410733"></a>

Before installing OmniMV, prepare the hardware and software environments to facilitate subsequent installation operations.

**Hardware Requirements<a name="en-us_topic_0000001665139878_section197116445713"></a>**

[Table 1](#en-us_topic_0000001713099729___d0e1171)  lists the hardware requirements for each node in the cluster.

**Table  1**  Hardware requirements

|Item|Management/Compute/Storage Node|
|--|--|
|Processor|Kunpeng 920 5250 processor/New Kunpeng 920 processor model|
|Memory size|384 GB (12 x 32 GB)|
|Memory frequency|2666 MHz|
|Network|Service network: 10GEManagement network: 1GE|
|Drive|System drive: 1 x RAID 0 (1 x 1.2 TB SAS HDD)Data drive: 12 x RAID 0 (12 x 8 TB SATA HDD)|
|RAID controller card|LSI SAS3508|


**OS and Software Requirements<a name="en-us_topic_0000001665139878_section112781416185717"></a>**

[Table 2](#en-us_topic_0000001665139878_table97421444205)  lists the OS and software requirements for each node in the cluster.

**Table  2**  OS and software requirements

|Item|Version|Description|Management Node (Server)|Compute/Storage Node|
|--|--|--|--|--|
|OS|openEuler 22.03 LTS SP1|None|√|√|
|JDK|BiSheng JDK 1.8.0_342|For details about how to install the BiSheng JDK, see BiSheng JDK 8 Installation Guide.|√|√|
|Hadoop|3.2.0|See Hadoop Deployment Guide (CentOS 7.6 & openEuler 20.03).|√|√|
|Spark|3.1.1/3.4.3|See Spark Deployment Guide (CentOS 7.6 & openEuler 20.03).|√|-|
|ZooKeeper|3.6.2 or later|See ZooKeeper Deployment Guide (CentOS 7.6 & openEuler 20.03).|-|√|
|Python|3.10.2 or later|None|√|√|
|√: indicates that the item is required on the node.-: indicates that the item is not required on the node.|


>![](public_sys-resources/icon-note.gif) **NOTE:** 
>-   √: indicates that the item is required on the node.
>-   -: indicates that the item is not required on the node.
>-   If the preceding third-party software has vulnerabilities, fix the vulnerabilities based on official instructions.
>-   The preceding component versions may be different from those in the  _Deployment Guide_. The  _Deployment Guide_  is for reference only.

**Obtaining Software<a name="en-us_topic_0000001665139878_section73341541215"></a>**

[Table 3](#en-us_topic_0000001665139878_table1238345073817)  describes the OmniMV software packages and how to obtain them.

**Table  3**  Software required for OmniMV

|Software Name|Package Name|Release Type|Description|Download URL|
|--|--|--|--|--|
|OmniMV software installation package|BoostKit-omnimv_1.2.0.zip|Open source|-|Link|
|OmniMV plugin JAR package|boostkit-omnimv-spark-3.1.1-1.2.0-aarch64.jar|Open source|Spark extension package for using the OmniMV feature.|Link|
|boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar|Open source|Spark extension package for using the OmniMV feature.|Link|
|OmniMV log parser JAR package|boostkit-omnimv-logparser-spark-3.1.1-1.2.0-aarch64.jar|Open source|Function package for OmniMV to parse Spark logs.|Link|
|boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar|Open source|Function package for OmniMV to parse Spark logs.|Link|
|hive-testbench|hive-testbench-hdp3.zip|Open source|TestBench source package used for generating TPC-DS/TPC-H data.|Link|


**Verifying the Software Package Integrity<a name="en-us_topic_0000001665139878_section16501429204018"></a>**

After downloading a software package from the Kunpeng community, verify the software package to ensure that it is consistent with the original one on the website.

Verify a software package as follows:

1. Obtain the digital certificate and software.
2. Obtain the  [verification tool and guide](https://support.huawei.com/enterprise/en/tool/pgp-verify-TL1000000054).
3. Verify the package integrity by following the procedure described in the  _OpenPGP Signature Verification Guide_  obtained from the URL.

Before installing OmniMV, prepare the hardware and software environments to facilitate subsequent installation operations.


## Installing the Feature<a name="EN-US_TOPIC_0000002551130865"></a>

### Installation Node Requirements<a name="EN-US_TOPIC_0000002551130815"></a>

Check that the configuration of each node meets the OmniMV installation requirements. Before compiling the source code, install GCC/G++, make, CMake, and glibc on each node and ensure that they meet the version requirements. Before installing OmniMV, prepare the cluster environment and those components required for installing the Spark engine based on the OS and software requirements.

- Before using the source code for compilation, install GCC/G++, Make, CMake, and glibc on each node. The version requirements are listed in  [Table 1](#en-us_topic_0000001664980146_en-us_topic_0000001519207349_table98249491204).

    **Table  1**  Software required for source code compilation

|Software Name|Software Version|
|--|--|
|GCC/G++|7.3.0|
|make|4.2.1 or later|
|CMake|3.13.4 or later|
|glibc|2.25 or later|


- Before installing OmniMV, install related components in the cluster environment based on  [OS and Software Requirements](environment-requirements.md#en-us_topic_0000001665139878_section112781416185717).  [Table 2](#en-us_topic_0000001664980146_table673mcpsimp)  lists the components required by the Spark engine.

    **Table  2**  Required components

|Engine Using OmniMV|Required Components|
|--|--|
|Spark|OS, JDK, Hadoop, Spark, Hive, and HDFS|


    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >You can select a cluster mode based on the application scenario requirements. The secure mode \(Kerberos\) is recommended if the storage mode is HDFS.

Check that the configuration of each node meets the OmniMV installation requirements. Before compiling the source code, install GCC/G++, make, CMake, and glibc on each node and ensure that they meet the version requirements. Before installing OmniMV, prepare the cluster environment and those components required for installing the Spark engine based on the OS and software requirements.
### Installing OmniMV<a name="EN-US_TOPIC_0000002551410763"></a>

To install OmniMV on openEuler 22.03, install Conda and create an OmniMV virtual environment. Then use  **pip**  to download the Python dependency library, including the dependency packages of the Spark engine. After that, download and extract the OmniMV recommendation algorithm software package, and remove other user groups' read and execute permissions.

1. Download the Conda software package and activate the Conda virtual environment.
    1. Download Conda.

        ```
        cd /opt
        wget -k https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-aarch64.sh
        ```

    2. Install Conda, for example, in the  **/opt/anaconda3**  directory.

        ```
        bash Anaconda3-2022.05-Linux-aarch64.sh
        ```

    3. Initialize Conda.

        ```
        source /opt/anaconda3/etc/profile.d/conda.sh
        ```

    4. Create a Conda virtual environment named  **omnimv**.

        ```
        conda create -n omnimv python=3.11
        ```

    5. Activate the Conda virtual environment.

        ```
        conda activate omnimv
        ```

2. Use pip to download the Python dependencies.
    - Spark 3.1.1 dependency packages

        ```
        pip install Cython==0.29.24
        pip install pandas==1.3.5
        pip install PyYAML==6.0
        pip install mo-sql-parsing
        pip install sql-metadata==2.6.0
        pip install scikit-learn==1.5.2
        pip install xgboost==1.7.3
        pip install pyspark==3.1.1
        pip install pydoop==2.0.0
        pip install numpy==1.23.3
        ```

    - Spark 3.4.3 dependency packages

        ```
        pip install Cython==0.29.24
        pip install pandas==1.3.5
        pip install PyYAML==6.0
        pip install mo-sql-parsing
        pip install sql-metadata==2.6.0
        pip install scikit-learn==1.5.2
        pip install xgboost==1.7.3
        pip install pyspark==3.4.3
        pip install pydoop==2.0.0
        pip install numpy==1.23.3
        ```

3. Download the OmniMV recommendation algorithm software package.

    Spark engine: Download the OmniMV recommendation algorithm software package, and save the JAR package of the Spark SQL plugin and log parser JAR package to the server node of the Spark cluster. For details about how to obtain the software packages, see  [Obtaining Software](environment-requirements.md#en-us_topic_0000001665139878_section73341541215). There is no special requirement on the software package paths. For example, you can save them in  **/opt/omnimv**.

    - Spark 3.1.1

        ```
        mkdir -p /opt/omnimv
        mv boostkit-omnimv-spark-3.1.1-1.2.0-aarch64.jar /opt/omnimv
        mv boostkit-omnimv-logparser-spark-3.1.1-1.2.0-aarch64.jar /opt/omnimv
        mv BoostKit-omnimv_1.2.0.zip /opt/omnimv
        ```

    - Spark 3.4.3

        ```
        mkdir -p /opt/omnimv
        mv boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar /opt/omnimv
        mv boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar /opt/omnimv
        mv BoostKit-omnimv_1.2.0.zip /opt/omnimv
        ```

4. Extract the OmniMV component package and remove the read and execute permissions of other user groups on the generated folder.

    ```
    cd /opt/omnimv
    unzip BoostKit-omnimv_1.2.0.zip
    chmod -R o-r BoostKit-omnimv_1.2.0
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >You are advised to extract the package as a non-root user so that the generated directory has the minimum permission to prevent files from being replaced.

To install OmniMV on openEuler 22.03, install Conda and create an OmniMV virtual environment. Then use  **pip**  to download the Python dependency library, including the dependency packages of the Spark engine. After that, download and extract the OmniMV recommendation algorithm software package, and remove other user groups' read and execute permissions.


