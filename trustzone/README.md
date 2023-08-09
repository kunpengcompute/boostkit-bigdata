hook 使用方式
1. 在环境中创建日志文件 (如 touch /home/posix.log ), 修改 bin 目录下的 hook.c 文件的LOG_PATH, 指定创建好的日志文件(如  #define LOG_PATH "/home/posix.log")
2. 修改 bin 目录下的 build.sh 文件, 添加需要测试(抓取)的方法 (可以参考其中的 run_example 方法,在注释处添加自定义的测试方法)
3. 执行 sh build.sh , 脚本会生成libhook.so文件, 抓取的执行过程中系统调用的全部接口，并将结果记录在 hook.c 中指定的日志文件中