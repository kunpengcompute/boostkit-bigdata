# OmniMV

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
# 创建conda虚拟环境，名字是clickhouse，运行环境是python3.10.2及以上
conda create -n clickhouse_aarch64 python=3.10
# 激活conda虚拟环境
conda activate clickhouse_aarch64
```

**requirements.txt** (将下面的依赖包写到requirements.txt里面，然后进行安装)

```text
pip install Cython==0.29.24
pip install numpy==1.22.3
pip install pandas==1.3.5
pip install PyYAML==6.0
pip install mo-sql-parsing
pip install sql-metadata==2.6.0
pip install clickhouse-driver==0.2.4
pip install scikit-learn==1.1.2
pip install xgboost==1.7.3
```

### 场景一（从query_log中获取sql，解析得到projection）

**确保运行的环境上能够连接clickhouse客户端**

1. 初始化database里面所有表的schema信息

```text
# 首先需要在配置文件中指定database名字，和文件存放的路径
# database = default
# work_path = ./clickhouse
# 执行命令：
python main.py clickhouse init_database
```

2. 从query_log中提取包含指定表信息的sql

```text
# 配置表工作模式
is_distributed_table = False
# 配置表的信息
clickhouse_tables = lineorder_flat
# 从query_log中对每张表提取query信息，需要配置运行时间阈值，和每张表提取的query数量
sql_running_time_threshold = 100
query_limit_each_table = 15
# 执行命令
python main.py clickhouse extract_sqls
```

3. 解析提取的所有query，推荐projection，会提取出所有可能的projection

```text
python main.py clickhouse parse_sql_text
```

4. （可选）创建projection
```text
# 配置文件中可以控制每张表创建projection的数量，比如每张表只创建3个projection
# projection_limit_each_table = 3
# 是否需要对新建的projection物化历史的数据，默认不物化历史数据，如果需要物化历史数据，需要修改为True
# allows_materialize_projection = True

python main.py clickhouse create_projections
```

### 场景二（可以与clickhouse进行交互，使用策略评估）

1. 首先需要清除当前数据库中目标表现存的Projection：

```text
# 指定database名字
# database = default
# 配置表工作模式
is_distributed_table = False
# 配置目标表
clickhouse_tables = lineorder_flat
# 指定要删除的Projection，如没有可配置为None或跳过步骤1.
drop_projections = lineorder_flat.lineorder_flat_cube0
# 执行删除命令
python main.py clickhouse drop_projections
```

2. 初始化database里面目标表的schema信息

```text
# 配置文件存放路径，要求此目录为空目录
work_path = ./clickhouse 
# 执行初始化数据库schema信息命令
python main.py clickhouse init_database
```

3. 从query_log中提取包含指定表信息的sql

```text
# 从query_log中对每张表提取query信息，需要配置运行时间阈值，和每张表提取的query数量
sql_running_time_threshold = 100
query_limit_each_table = 15
# 执行提取query的命令
python main.py clickhouse extract_sqls
```

4. 生成projection候选视图

```text
python main.py clickhouse generate_views
```

5. 在clickhouse中创建projection

```text
# 配置文件中可以控制每张表创建projection的数量，比如每张表只创建3个projection
# projection_limit_each_table = 3
# 是否需要对新建的projection物化历史的数据，默认不物化历史数据，如果需要物化历史数据，需要修改为True
# allows_materialize_projection = True

python main.py clickhouse create_projections
```

6. 创建projection之后，可以使用命令查看sql真实的改写情况

```text
# 查看query是否可以被创建的projection改写，最后输出一个query mapping映射关系，key是query的id，value是cube的id
python main.py clickhouse parse_query_mv
```

### 场景三 多表场景推荐物化视图（直接从sql解析得到物化视图，无策略筛选）

1. 初始化database里面所有表的schema信息

```text
# 首先需要在配置文件中指定database
# database = imdbload
# 指定工作路径，要求此路径为文件夹
# work_path = ./clickhouse
# 配置目标表为all
# clickhouse_tables = all
python main.py clickhouse init_database
```

2. 从query_log中提取包含指定表信息的sql，并提取物化视图

```text
# 配置从日志中捞取多少条sql
query_limit_mutil_table = 15
# 执行物化视图推荐命令
python main.py clickhouse generate_materialize_view
```

### 物化视图Clickhouse引擎命令总结

1. python main.py clickhouse drop_projections 表示删除指定的projection
2. python main.py clickhouse init_database 表示初始化指定表的schema信息
3. python main.py clickhouse extract_sqls 表示从system.query_log或者配置的日志表中提取sql相关的信息
4. python main.py clickhouse generate_views 表示生成候选的projection，提供创建projection的语句和物化该projection的语句
5. python main.py clickhouse create_projections 表示创建贪心推荐后的projection
6. python main.py clickhouse parse_query_mv 表示得到sql可以被哪个projection成功改写的映射关系
7. python main.py clickhouse parse_sql_text 表示仅从sql文本来解析，并生成首选的projection
8. python main.py clickhouse extract_sqls_incremental 表示从system.query_log或者配置的日志表中增量提取sql相关的信息
9. python main.py clickhouse parse_sql_text_incremental 表示对增量提取的sql进行解析，因此可以得到新增的候选projection集
10. python main.py clickhouse generate_materialize_view 表示对含有多表的sql语句进行物化视图的提取，提供创建物化视图的语句
11. python main.py clickhouse --version 表示查看当前OmniRuntime物化视图的版本信息

