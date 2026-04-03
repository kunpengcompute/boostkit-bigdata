# 用户指南<a name="ZH-CN_TOPIC_0000002520333564"></a>

## 使用特性<a name="ZH-CN_TOPIC_0000002520013624"></a>

### LoadsMetric基本使用<a name="ZH-CN_TOPIC_0000002520333568"></a>

LoadsMetric为集群负载采集与分析工具，支持安装、卸载、启动、停止等操作，同时，支持一键启停Prometheus、Node Exporter、Grafana其他相关软件。

**查看使用命令<a name="zh-cn_topic_0000002161905900_section151109571067"></a>**

1. 进入server1节点的`/home/hadoop/loadsmetric-software`目录。

    ```
    cd /home/hadoop/loadsmetric-software
    ```

2. 执行**sh loadsmetric\_deploy.sh**查看脚本运行方法。

    ```
    sh loadsmetric_deploy.sh
    ```

    ![](figures/zh-cn_image_0000002520013734.png)

**LoadsMetric服务基本运维方法<a name="zh-cn_topic_0000002161905900_section188892817419"></a>**

脚本执行建议进入到脚本放置目录中执行。

```
cd /home/hadoop/loadsmetric-software/
```

- 安装LoadsMetric。

    ```
    sh loadsmetric_deploy.sh install
    ```

- 卸载LoadsMetric。

    ```
    sh loadsmetric_deploy.sh uninstall
    ```

- 启动LoadsMetric。

    ```
    sh loadsmetric_deploy.sh start
    ```

- 停止LoadsMetric。

    ```
    sh loadsmetric_deploy.sh stop
    ```

- 重启LoadsMetric。

    ```
    sh loadsmetric_deploy.sh restart
    ```

**LoadsMetric日志配置方法<a name="zh-cn_topic_0000002161905900_section77551892051"></a>**

1. 打开日志配置文件。

    ```
    vi /home/hadoop/loadsmetric-install/loadsmetric-server/etc/logback.xml
    ```

2. 按`i`进入编辑模式，按需修改如下参数。

    ```
    <!--LOG FILE RETENTION DAYS -->   #设置保留的历史日志文件天数
    <MaxHistory>30</MaxHistory>
    <!-- LOG FILE MAX SIZE -->        #设置单个日志文件的最大大小
    <MaxFileSize>2000MB</MaxFileSize>
    <!-- LOG FILE TOTAL SIZE CAP -->    #设置所有日志文件占用磁盘空间的总上限
    <TotalSizeCap>20GB</TotalSizeCap>
    ```

3. 按`Esc`键，输入:**wq!**，按`Enter`保存并退出编辑。

**LoadsMetric一键启停Prometheus、Node\_exporter、Grafana<a name="zh-cn_topic_0000002161905900_section11313142953"></a>**

一键启停Prometheus、Node\_exporter、Grafana命令请参见[**表 1** LoadsMetric服务一键启停Prometheus、Node\_exporter、Grafana方法](#LoadsMetric服务一键启停Prometheus、Node\_exporter、Grafana方法)。

**表 1** LoadsMetric服务一键启停Prometheus、Node\_exporter、Grafana方法<a id="LoadsMetric服务一键启停Prometheus、Node\_exporter、Grafana方法"></a>

|**功能**|**命令**|**说明**|
|--|--|--|
|一键启动node_exporter|**sh loadsmetric_deploy.sh start_exporter**|node_exporter安装目录为默认路径/opt/prometheus/node_exporter-1.7.0.linux-arm64/时，一键启动所有agent节点的node_exporter。|
|一键启动node_exporter|**sh loadsmetric_deploy.sh start_exporter "安装路径"**如：**sh loadsmetric_deploy.sh start_exporter `/opt/prometheus/node_exporter-1.7.0.linux-arm64”**|node_exporter安装目录为其他路径时，一键启动所有agent节点的node_exporter。|
|一键停止node_exporter|**sh loadsmetric_deploy.sh stop_exporter**|一键停止所有agent节点的node_exporter。|
|一键启动Prometheus|**sh loadsmetric_deploy.sh start_prometheus**|Prometheus安装目录为默认路径/opt/prometheus/prometheus-2.45.4.linux-arm64/时，一键启动server节点的Prometheus。|
|一键启动Prometheus|**sh loadsmetric_deploy.sh start_prometheus "安装路径"**如：**sh loadsmetric_deploy.sh start_prometheus `/opt/prometheus/prometheus-2.45.4.linux-arm64”**|Prometheus安装目录为其他路径时，一键启动server节点的Prometheus。|
|一键停止Prometheus|**sh loadsmetric_deploy.sh stop_prometheus**|一键停止server节点的Prometheus。|
|一键启动Grafana|**sh loadsmetric_deploy.sh start_grafana**|Grafana安装目录为默认路径/opt/prometheus/grafana-v10.4.2/bin/时，一键启动server节点的Grafana。|
|一键启动Grafana|**sh loadsmetric_deploy.sh start_grafana "安装路径"**如：**sh loadsmetric_deploy.sh start_grafana `/opt/prometheus/grafana-v10.4.2/bin”**|Grafana安装目录为其他路径时，一键启动server节点的Grafana。|
|一键停止Grafana|**sh loadsmetric_deploy.sh stop_grafana**|一键停止server节点的Grafana。|



### 使用OmniScheduler Yarn负载调度算法<a name="ZH-CN_TOPIC_0000002551413723"></a>

1. 进入`/home/hadoop/loadsmetric-install/loadsmetric-server/etc`目录。

    ```
    cd /home/hadoop/loadsmetric-install/loadsmetric-server/etc
    ```

2. 按需修改LoadsMetric配置项（不需要则忽略）。
    1. 打开配置文件。

        ```
        vim application.properties
        ```

    2. 按`i`进入编辑模式，按需修改利用率阈值配置项和利用率权重配置项，前者影响集群超载节点的判断，后者影响节点负载排序结果，参数说明请参见[zh-cn\_topic\_0000002551533573.md](zh-cn_topic_0000002551533573.md)。

        ```
        # limits for multi resource usage
        load.limit.cpu=80
        load.limit.mem=80
        load.limit.diskio=80
        load.limit.netio=80
        
        # weights for multi resource usage
        load.weight.cpu=0.3
        load.weight.mem=0.3
        load.weight.diskio=0.2
        load.weight.netio=0.2
        ```

    3. 按`Esc`键，输入:**wq!**，按`Enter`保存并退出编辑。

3. 重启LoadsMetric服务，使配置项生效。

    ```
    cd /home/hadoop/loadsmetric-software
    sh loadsmetric_deploy.sh restart
    ```

4. 执行计算任务（如Spark任务），开始Yarn资源调度，参考命令如下。

    ```
    spark-sql --deploy-mode client --driver-cores 8 --driver-memory 20g --num-executors 3 --executor-cores 7 --executor-memory 26g --master yarn  --conf spark.task.cpus=1 --conf spark.sql.orc.impl=native --conf spark.sql.shuffle.partitions=1000 --conf spark.network.timeout=600  --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.skewedJoin.enabled=true --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.autoBroadcastJoinThreshold=100M --properties-file /home/spark.conf --database tpcds_bin_partitioned_decimal_orc_3000
    ```



## 维护特性<a name="ZH-CN_TOPIC_0000002520013630"></a>

升级或卸载OmniScheduler时请满足操作规范。

**升级OmniScheduler<a name="section1773319019218"></a>**

>![](public_sys-resources/icon-notice.gif) **须知：** 
>当前暂不支持通过工具进行升级，因此如需要升级，请下载安装包重新安装。

请通过[鲲鹏社区](https://www.hikunpeng.com/zh/developer/download?title=%E5%A4%A7%E6%95%B0%E6%8D%AE&subTitle=OmniRuntime)下载要升级的OmniScheduler Yarn负载调度算法软件安装包。

**卸载OmniScheduler<a name="section14513115042212"></a>**

>![](public_sys-resources/icon-notice.gif) **须知：** 
>当前步骤仅供需要卸载OmniScheduler时参考，不属于部署OmniScheduler的必要操作步骤。

1. 在相关节点上停止Prometheus、Grafana和Node Exporter服务，删除所有节点`/opt/prometheus`目录的安装包。

    ```
    cd /opt/prometheus
    rm -rf *
    ```

2. 卸载LoadsMetric。

    停止LoadsMetricApplication进程，然后执行卸载脚本。完成卸载后，删除`/home/hadoop`目录所有内容。

    ```
    cd /home/hadoop/loadsmetric-software
    sh loadsmetric_deploy.sh stop
    sh loadsmetric_deploy.sh uninstall
    cd ..
    rm -rf *
    ```

3. 卸载OmniScheduler。
    1. 在server1节点删除OmniScheduler的JAR包。

        ```
        cd ${HADOOP_HOME}/share/hadoop/yarn/lib
        rm -rf boostkit-yarn-schedule-load-evolution-1.0.0.jar
        ```

    2. 注释掉或删除server1节点yarn-site.xml和capacity-scheduler.xml中新增配置内容，具体新增内容见[zh-cn\_topic\_0000002520013706.md](zh-cn_topic_0000002520013706.md)。
    3. 将server1节点两个配置项内容同步到其他节点。
    4. 重启Yarn集群以使配置生效。

        ```
        ${HADOOP_HOME}/sbin/stop-yarn.sh
        ${HADOOP_HOME}/sbin/start-yarn.sh
        ```



