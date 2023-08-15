单机 hook 使用方式
1. 在环境中创建日志文件 (如 touch /home/posix.log ), 修改 bin 目录下的 hook.c 文件的LOG_PATH, 指定创建好的日志文件(如  #define LOG_PATH "/home/posix.log")
2. 修改 bin 目录下的 build.sh 文件, 添加需要测试(抓取)的方法 (可以参考其中的 run_example 方法,在注释处添加自定义的测试方法)
3. 执行 sh build.sh , 脚本会生成libhook.so文件, 抓取的执行过程中系统调用的全部接口，并将结果记录在 hook.c 中指定的日志文件中

spark分布式作业 hook 方式
前置条件：环境中不能有已经启动的spark集群（如果有已经启动的spark集群需要先关闭它）
1. 修改 bin 目录下的 hook.c 文件的16行的LOG_PATH, 指定日志文件路径(如  #define LOG_PATH "/home/posix.log")
2. 修改 build.sh，注释35行的run_example (如修改 run_example 为 #run_example)
3. 执行 sh build.sh，脚本执行完成后会在当前目录生成 libhook.so 文件
4. 把生成的libhook.so拷贝到各个节点（如 拷贝到 主、从节点的 /home 目录下）
5. 修改主、从节点spark安装目录下的 conf/spark-env.sh 文件， 在文件末尾追加 export LD_PRELOAD=/home/libhook.so （注意这里填写的libhook.so路径需要和步骤4中填写的路径保持一致）
6. 主节点启动spark集群 （在 spark 安装目录的sbin文件夹下执行 ./start-all.sh）
7. 主节点执行spark分布式作业
8. 在分布式作业的执行过程中在主、从节点的 /home 目录下都会生成 posix.log ， 该文件即为 hook 到的接口文件

ps:如果想停止hook, 修改主从节点spark安装目录下的 conf/spark-env.sh 文件删除export LD_PRELOAD=/home/libhook.so,而后停止spark（在 spark 安装目录的sbin文件夹下执行 ./stop-all.sh）集群即可