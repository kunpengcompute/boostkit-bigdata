1 负载劫持
首先要对用户负载进行劫持，获取用户负载及相关信息，以便后续能够调优。使能负载劫持，步骤如下：
- ① 修改 omniadvisor/script/spark-submit 脚本中 hijack_path 值，需要填实际的 hijack.py 文件的路径
- ② 替换正在使用的 spark-submit 脚本（可通过 `which spark-submit` 查看）为 ① 中的 spark-submit 文件
- ③ 启用环境变量：`export enable_omniadvisor=true`
说明：完成以上步骤之后，用户下发相同的负载，会劫持用户的命令，并替换用户配置为系统推荐的最优的配置（初始状态下保持用户的默认配置）。推荐配置若执行失败，则退化为用户的默认配置

2 查询负载等信息
执行负载并劫持之后，在后台管理页面查看到相关信息
- ① 若之前从未创建过用户，使用命令：`python init.py createsuperuser`，按指示设置用户名和密码。
- ② `python init.py runserver`（可选 绑定 0.0.0.0:8000）
- ③ 修改settings.py中的 ALLOWED_HOSTS（可选）
- ④ 登录后台管理页面：`localhost:8000/admin/` （替换实际的ip，若访问不通，须执行步骤 ②③）
说明：进入页面后，可以看到APP下有若干数据表，其中 loads 就是负载表

3 启用调优
通过在根目录下，执行 `python tuning.py --help` 查看各参数说明
- ① 各参数的详细介绍
  - --load-id，即步骤2中loads表查询到的负载的id
  - --retest-way，复测方式（复测是为了保证结果的可靠），可选前台或者后台复测。若是前台复测，当用户提交任务时，跟随任务下发；若后台复测，则是由
  系统触发
  - --tuning-method，调优方法，不同的调优方法原理不同，iterative，即迭代调优，本质上使用贝叶斯优化方法；expert，即专家调优，通过诊断资源瓶颈，
  给出调优建议；transfer，迁移调优，迁移相似负载的调优经验到陌生负载；native 算子加速，将Spark原生算子替换为C++ Native算子，同时使能CPU向量化执行，实现性能加速
- ② 命令示例 
  - `python tuning.py -l 1 -r backend -t expert`。说明：使用专家调优，对id为1的负载，进行调优，期间在后台复测，复测次数为 
  common_config.cfg 中 tuning.retest.times 的值
  - `python tuning.py -l 1 -r backend`。说明：使用系统中默认的调优策略以及历史调优记录，共同决定当前的调优方式
说明：调优结束后，会更新负载的最优配置信息，以便步骤1能够使用最优配置




