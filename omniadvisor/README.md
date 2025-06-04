## 项目结构

```
# 项目结构
root
├── config/                     # 配置目录
│   ├── common_config.cfg       # 通用配置文件
│   └── spark_repo.json         # 调优参数空间文件
├── docs/                       # 文档目录
├── logs/                       # log文件生成目录
├── script/                     # 脚本目录
│   └── spark-submit            # Spark submit脚本，用于替换实现任务劫持
├── src/                        # 源代码目录
│   ├── init.py                 # 初始化入口
│   ├── hijack.py               # 任务劫持入口
│   └── tuning.py               # 负载调优入口
├── tests/                      # 测试目录
├── pyproject.toml              # 构建文件
└── READEME                     # README说明
```

## 使用说明

### 1 历史数据库初始化

初始化数据库，数据库用于存储调优过程与负载执行过程。若之前执行过初始化过程，则此处可以跳过。

- ① 进入`omniadvisor/src/`目录下。
- ② 顺序执行以下命令：`python init.py makemigrations`和`python init.py migrate`，初始化数据库
- ③ 执行命令`python init.py createsuperuser`创建超级用户，并按指示设置用户名和密码。

### 2 启动历史数据库服务

完成数据库初始化后，即可启动历史数据库，并通过Web端查看数据库内数据

- ① 执行命令`python init.py runserver 0.0.0.0:8000`启动数据库，其中`0.0.0.0`应替换为实际节点IP。
- ② 登录Web管理页面：`localhost:8000/admin/`，`localhost`替换实际的IP地址，并通过初始化过程中的用户名和密码登录。

### 3 负载劫持

首先要对用户负载进行劫持，获取用户负载及相关信息，以便后续能够调优。使能负载劫持，步骤如下：

- ① 修改 spark-submit 脚本（位于`omniadvisor/script/`目录下），将 `hijack_path` 值修改为 `hijack.py` 文件（位于`omniadvisor/src/`目录下）绝对路径
- ② 替换正在使用的 spark-submit 脚本（可通过 `which spark-submit` 查看）为 ① 中的 spark-submit 文件
- ③ 启用环境变量：`export enable_omniadvisor=true`
说明：完成以上步骤之后，用户下发相同的负载，会劫持用户的命令，并替换用户配置为系统推荐的最优的配置（初始状态下保持用户的默认配置）。推荐配置若执行失败，则退化为用户的默认配置

### 4 查询负载信息

执行负载并劫持之后，在后台管理页面查看到相关信息

- ① 登录后台管理页面：`localhost:8000/admin/` 
- ② 切到Loads页面，即可查询看到负载信息

### 5 启用调优

通过在根目录下，执行 `python tuning.py --help` 查看各参数说明

- ① 各参数的详细介绍
  - --load-id，即步骤2中loads表查询到的负载的id
  - --retest-way，复测方式（复测是为了保证结果的可靠），可选前台或者后台复测。若是前台复测，当用户提交任务时，跟随任务下发；若后台复测，则是由
  系统触发
  - --tuning-method，调优方法，不同的调优方法原理不同，iterative，即迭代调优，本质上使用贝叶斯优化方法；expert，即专家调优，通过诊断资源瓶颈，
  给出调优建议；transfer，迁移调优，迁移相似负载的调优经验到陌生负载；native 算子加速，将Spark原生算子替换为C++ Native算子，同时使能CPU向量化执行，实现性能加速
- ② 命令示例 
  - `python tuning.py -l 1 -r backend -t expert`。说明：使用专家调优，对id为1的负载，进行调优，期间在后台复测，复测次数为 `common_config.cfg` 中 `tuning.retest.times` 的值
  - `python tuning.py -l 1 -r backend`。说明：使用系统中默认的调优策略以及历史调优记录，共同决定当前的调优方式
  说明：调优结束后，会更新负载的最优配置信息，以便步骤1能够使用最优配置



