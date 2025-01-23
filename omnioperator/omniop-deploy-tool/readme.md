# OmniOperator自动部署工具使用说明：

## 1. 修改配置文件./conf/config
1.1. `expect_spark_version`：需要部署OmniOperator包对应的spark版本，而不是填系统当前的spark版本
例如：当前系统的spark版本为3.4.1, 但OmniOperator的包对应spark版本是3.4.3，则expect_spark_version应填3.4.3，否则会报错找不到对应的包

1.2. `omnioperator_version`：OmniOperator的版本

1.3. `target_path`：OmniOperator的部署路径

1.4. `sve_flag`：是否选择sve版本（centos系统暂不支持，cpu为鲲鹏920的机器暂不支持）
若机器不支持OmniOperator SVE版本，而选择SVE版本安装，将安装失败

## 2. 获取OmniOperator安装包
获取：

BoostKit-omniop_{omni_version}.zip
boostkit-omniop-spark-{spark_version}-{omni_version}-aarch64.zip
Dependency_library_{os_type}.zip

三个包，并放置在脚本的根目录/omnioperator文件夹下

## 3. 脚本执行
在脚本的根目录下，执行：
```shell
bash deploy.sh
```

## 4. 获取命令行
在脚本的根目录下，执行：
```shell
vim ./command/command_line
```
并复制文件中的内容，粘贴到机器上，执行即可