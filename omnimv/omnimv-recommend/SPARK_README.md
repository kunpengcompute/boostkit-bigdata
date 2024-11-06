# OmniMV

**software下面包含3个部分：**

- BoostKit-omnimv_1.2.0.zip 闭源二进制软件包
- boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar 视图改写工具jar包
- boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar 日志解析工具jar包

### 配置Anaconda环境

前提：环境中有gcc, cmake，yum等基础环境配置

```text
# 下载conda软件包
cd /opt
wget -k https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-aarch64.sh 
# 按照conda，安装路径可以选择在/opt/anaconda3路径下面
bash Anaconda3-2022.05-Linux-aarch64.sh
# 初始化conda
source /opt/anaconda3/etc/profile.d/conda.sh
# 创建conda虚拟环境，名字是spark，运行环境是python3.10.2及以上
conda create -n spark python=3.10
# 激活conda虚拟环境
conda activate spark
```

**requirements.txt** (将下面的依赖包写到requirements.txt里面，然后进行安装)

```text
pip install Cython==0.29.24
pip install numpy==1.22.3
pip install pandas==1.3.5
pip install PyYAML==6.0
pip install mo-sql-parsing
pip install sql-metadata==2.6.0
pip install pyspark==3.4.3
pip install pydoop==2.0.0
```

如果pydoop按照遇到问题，可以参考如下按照步骤：

```text
yum install -y libtirpc
find /usr -name "types.h" | grep rpc (如果发现types.h在/usr/include/tirpc目录下面，则采用下面的安装步骤)
pip install --global-option=build_ext --global-option="-I/usr/include/tirpc" pydoop==2.0.0
```

按照完成后，虚拟环境中的软件包如下所示：

### 修改配置文件

`config/omnimv_config_spark.cfg` 程序运行所涉及的所有运行时参数均在此文件中，包括数据存放位置和Spark等参数设置

以下参数配置，请在代码开始执行之前进行设置

- logparser_jar_path和cache_plugin_jar_path务必配置好路径
- hdfs路径需要合理配置，"hdfs://server1:9000"改路径前缀由hadoop core-site.xml中的配置项决定，配置名称是"fs.defaultFS"

```text
# schema sql路径
schema_path = hdfs://server1:9000/omnimv/schema.sql

# 数据库名称
database = tpcds_bin_partitioned_decimal_orc_1000
# 日志解析的jar包路径
logparser_jar_path = /opt/spark/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar
# omnimv plugin的路径
cache_plugin_jar_path = /opt/spark/boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar
# spark-defaults.conf路径下面配置的spark history输出的路径
hdfs_input_path = hdfs://server1:9000/spark2-history
# 通过日志解析之后，把每条日志解析成json格式，所有日志存放的路径
hdfs_output_path = hdfs://server1:9000/omnimv/spark2-history-json
# sql语句的存放路径，这里以tpcds sql为例
sqls_path = hdfs://server1:9000/omnimv/tpcds
# 候选视图的阈值，只选取前面TOP的候选视图
mv_limit = 10

# 候选视图存放的路径，同时会在改目录下面生成子目录：mv_sql(所有候选视图)，top_mv(top N 候选视图)，greedy_recommend_mv(最后推荐的视图)
mv_output_path = hdfs://server1:9000/omnimv/mv

```

`config/logging.yml`日志的配置文件，主要是配置日志的输出形式，建议不用修改

*当前执行目录下面会生成logs目录，可以从这里面获取日志信息*

### 执行步骤(以tpcds sql为例)

首先需要情况当前数据库中残留的候选视图，可以采用如下命令：

```shell
DATABASE=tpcds_bin_partitioned_decimal_orc_1000
views=`spark-sql --master local -e "SHOW MATERIALIZED VIEWS" --conf spark.sql.extensions=com.huawei.boostkit.spark.OmniMV --jars /opt/spark/boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar --database ${DATABASE} | grep -P "^${DATABASE}" |awk -F "\t" '{print $2}' |grep -v "partition"` ; \
drop_mv_cmd=`echo ${views}|tr -s ' ' '\n'|xargs -i echo "DROP MATERIALIZED VIEW IF EXISTS "{}";"` ;\
drop_cmd="${drop_mv_cmd}" ;
echo ${drop_cmd} ;\

spark-sql --master local -e "${drop_cmd}" \
--conf spark.sql.extensions=com.huawei.boostkit.spark.OmniMV \
--jars /opt/spark/boostkit-omnimv-spark-3.4.3-1.2.0-aarch64.jar --database ${DATABASE}
echo ""

```

确保当前数据库中只有tpcds的表

1. 初始化database里面所有表的schema信息

```text
# 首先需要在配置文件中指定database名字，和schema在hdfs上面存放的路径，然后执行初始化操作

cd /opt/spark/BoostKit-omnimv_1.0.0
python main.py spark init_database
# hdfs上可以查看结果
hdfs dfs -ls /omnimv
```

2. 上一步执行完成后，会在hdfs上面生成路径 `hdfs://server1:9000/omnimv`, 然后我们可以sql语句上传到该目录下面，以tpcds sql为例：

```text
# tpcds获取路径：https://github.com/apache/spark/tree/master/sql/core/src/test/resources/tpcds 

# 下载tpcds sql文件，然后将整个目录上传到指定的hdfs目录
hdfs dfs -put /opt/spark/tpcds/ /omnimv
```

3. 上传tpcds sql文件后，先在配置文件中修改spark相应的参数，然后执行这些sql语句：

**修改的spark参数如下**

注意修改spark运行参数和spark运行的超时时间

```text
# spark执行的模式，yarn模式还是local模式
spark_master = yarn
# spark参数中--name后面的参数
app_name = omnimv
# spark参数中--num-executors指定的参数
spark_num_executors = 15
# spark参数中--executor-memory指定的参数
spark_executor_memory = 48g
# spark参数中--driver-memory指定的参数
spark_driver_memory = 48g
# spark参数中--executor-cores指定的参数
executor_cores = 15
# 执行spark-sql session的超时时间，默认单位是s，这里默认指定5000秒超时时间
session_timeout = 5000
```

**修改完参数后，执行命令:**

```text
# 比较耗时，可以在后台执行命令
nohup python main.py spark run_original_sqls &
# 查看当前步骤运行情况
tail -f nohup.out
# 当前目录中会生成logs目录，每条sql执行的结果会记录在logs中，后缀为omnimv_plugin_false_rewrite, 可以在logs中获取执行的时间信息
```

4. 执行完tpcds sql后，在config/omnimv_config.cfg中填入执行tpcds执行的起止时间 (这里执行计划的获取可以重复使用，耗时13个小时)

```text
q_log_start_time = 2023-03-27 15:57
q_log_end_time = 2023-03-27 17:04
```

5. 解析yarn logs解析成json格式的文件，放入指定的目录中

```text
# 配置文件中指定日志文件输入输出目录：
# hdfs_input_path是spark-defaults.conf中默认配置的路径，可以不进行修改，output可以相应修改
hdfs_input_path = hdfs://server1:9000/spark2-history
hdfs_output_path = hdfs://server1:9000/omnimv/spark2-history-json

# 调用解析日志的任务
python main.py spark parse_query_logs
# 执行完成后，将在hdfs_output_path看到解析后的json文件
```

6. 根据上一步解析得到的执行计划，生成候选视图，输出所有的候选视图，TOPN候选视图，以及TOPN中query-mv的映射关系

```text
# 配置文件中指定TOP N候选视图的数量，比如指定生成top 10的候选视图
# 配置参数如下：
mv_limit = 10

# 执行生成候选视图命令
python main.py spark generate_views

# 配置文件中查看候选视图存放的路径，同时会在改目录下面生成子目录：mv_sql(所有候选视图)，greedy_recommend_mv(top N 候选视图)
# 输出路径如下所示：
mv_output_path = hdfs://server1:9000/omnimv/mv
```

7. 创建TOPN的候选视图

```text
# 按照上面的配置，会创建TOPN 视图
nohup python main.py spark create_greedy_views &
# 查看当前步骤运行情况
tail -f nohup.out
# 创建mv的日志在当前目录下面的logs目录中查看
```

### 物化视图Spark引擎命令总结

1. python main.py spark init_database 表示初始化指定数据库中所有表的schema信息
2. python main.py spark run_original_sqls 表示使用Spark原生命令执行配置文件中sqls_path目录下所有的sql文件
3. python main.py spark parse_query_logs 表示从yarn logs中解析并提取原生sql执行相关的信息，包括执行计划，计算耗时等
4. python main.py spark generate_views 表示从解析的sql相关信息中，提取公共子查询，作为候选的物化视图
5. python main.py spark create_greedy_views 表示创建贪心算法推荐的物化视图，个数由配置文件中mv_limit指定
6. python main.py spark --version 表示查看当前OmniRuntime物化视图的版本信息