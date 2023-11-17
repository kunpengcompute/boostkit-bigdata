Quick Start

1. 使用前的准备工作
   
   1.宿主机teeOS 环境已经部署完成,且升级为CCOS
   
   2.tee os 的java环境部署完成
   
   3.准备好teeos + spark 的镜像
   
       注意镜像中需要包含以下内容: 
         1.镜像的/usr/lib64 目录下需要有libboundscheck.so
         2.镜像的/usr/bin 的目录下需要有 tlogcat agentd tee_teleport
         3.镜像的 SPARK_HOME 路径下的bin 和 sbin 目录中需要添加并覆盖当前代码仓中的脚本
         4.镜像的 SPARK_HOME 路径中需要有 trustzone/master trustzone/worker trustzone/sec 文件夹
         5.镜像的 SPARK_HOME 的文件夹 trustzone/sec 中需要包含 spark 运行所需的sec文件 spconf.sec,jars.sec

1. 启动Standalone master
   
    ```shell
    docker run -it \
        -e SPARK_HOME=/usr/local/spark \
        -e JAVA_HOME=/usr/local/bisheng-jdk1.8.0_262 \
        -e PATH=/usr/local/bisheng-jdk1.8.0_262/bin:/usr/local/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
        --name spark-master \
        --mount type=bind,source=/var/itrustee/teecd,target=/var/itrustee/teecd,readonly \
        --device /dev/teelog:/dev/teelog:wr \
        --device /dev/tc_ns_cvm:/dev/tc_ns_cvm:wr \
        sparktee:test \
        sh -c "./usr/local/spark/sbin/start-master.sh;sleep 10;tail -f /usr/local/spark/trustzone/master/ree.log;"
    ```
   其中 --mount 和 --device 是使用机密容器的必要选项, 其他内容按照实际环境修改
   
3. 启动Standalone worker
    ```shell
    docker run -it \
        -e SPARK_HOME=/usr/local/spark \
        -e JAVA_HOME=/usr/local/bisheng-jdk1.8.0_262 \
        -e PATH=/usr/local/bisheng-jdk1.8.0_262/bin:/usr/local/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
        --name spark-master \
        --mount type=bind,source=/var/itrustee/teecd,target=/var/itrustee/teecd,readonly \
        --device /dev/teelog:/dev/teelog:wr \
        --device /dev/tc_ns_cvm:/dev/tc_ns_cvm:wr \
        sparktee:test \
        sh -c "./usr/local/spark/sbin/start-worker.sh $master_ip:7077;sleep 10;tail -f /usr/local/spark/trustzone/worker/ree.log;"
    ```
4. 提交作业
    ```shell
    docker run -it \
        -v /home/txdemo/runjob.sh:/opt/spark/runjob.sh  \
        sparktee:test \                             \
        bash -x /opt/spark/runjob.sh
    ```

