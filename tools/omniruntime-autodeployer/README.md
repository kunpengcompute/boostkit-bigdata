# 1 前置条件
需要一台跳转jumper节点，用于将omnioperator的软件包传送到各个server/agent节点上进行部署，该jumper节点需要具备如下条件：

1、jumper节点与server/agent节点网络相同，可以用server节点作为jumper节点。

2、jumper节点需要安装python 3.5以上版本，且安装了yaml库。如果没有安装，需要执行一下命令进行安装：
```shell
pip3 install pyyaml
```
3、server和agent用于ssh登录的端口、用户名、密码都需要保持一致。

如果是通过非root用户部署，但部署目录需要有root权限，则需要将部署用户加到`/etc/sudoers`配置文件上。比如，部署用户为hadoop，则需要再`/etc/sudoers`文件上加入如下配置：

```shell
hadoop  ALL=(ALL)   ALL
```

4、当前只支持OmniOperator 1.4.0（也即Kunpeng BoostKit 24.0.RC1）以上版本部署

# 2 部署前准备
## 2.1 下载部署脚本
执行如下命令下载一键部署脚本工程：
```shell
git clone https://gitee.com/kunpengcompute/boostkit-bigdata.git
```
下载后`cd boostkit-bigdata/tools`进入`tools`目录，将`omniruntime-autodeployer`拷贝到服务器上。
> `omniruntime-autodeployer`建议上传至omniruntime.yaml配置的jumper节点的deploy_home下，详细见下述2.2章节

## 2.2 omniruntime.yaml配置
根据以下配置说明完成`omniruntime-autodeployer`目录下omniruntime.yaml配置
```yaml
env:
  username: "hadoop" # ssh登录用户名
  password: "123456" # ssh登录密码
  port: 22 # 需要各个节点ssh登录端口一致
  jumper_host: "server1" # 通过jumper传送软件包到其他节点上
  server_hosts: # server节点ip，支持server1这种hostname形式、192.168.1.1的ip格式、192.168.1.10-210网段形式
    - "server1"
  agent_hosts: # agent节点ip，支持agent1这种hostname形式、192.168.1.1的ip格式、192.168.1.10-210网段形式
    - "agent1"
    - "192.168.1.1"
    - "192.168.1.10-210"

omnioperator:
  omni_version: "1.4.0" # omnioperator版本，在闭源软件包名中可找到，BoostKit_omniop_1.4.0.zip
  spark_version: "3.3.1" # Spark版本，当前支持3.1.1和3.3.1两个版本
  os_type: "openeuler" # 操作系统版本，当前支持openeuler和centos两个版本
  omni_home: "/opt/omni-operator" # 部署omnioperator的目录，通常固定为/opt/omni-operator
  deploy_home: "/home/hadoop" # 存放部署安装包和部署脚本的目录，其中安装包需要再deploy_home上新建一个packages目录存放
  is_need_sudo: true # 部署到omni_home是否需要sudo权限，true/false，如果为true，需要提前将username配置到/etc/sudoers文件上
```

## 2.3 omni.conf配置
根据[OmniOperator用户指南](https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0065.html)完成`omniruntime-autodeployer`目录下的omni.conf的配置，参考下面的连接：
> https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0065.html

## 2.3 软件包准备
1、在omniruntime.yaml上配置的`omnioperator.deploy_home`目录上，上传以下三个软件包，具体获取方法参考[OmniRuntime特性指南](https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0019.html)的"OmniOperator-安装简介-环境要求-软件获取"章节：
> https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_0019.html

包含下述 3 个安装包：
```shell
BoostKit-omniop_{omni_version}.zip # OmniOperator闭源软件包
boostkit-omniop-spark-{spark_version}-{omni_version}-aarch64.zip # OmniOperator开源软件包
Dependency_library_{os_type}.zip # 需要从Dependency_library.zip上解压出来
```

# 3 软件部署
在jumper节点上进入`omniruntime-autodeployer`目录，执行如下命令：
```shell
python main.py all,omniop,deps,extension,conf
```
其中，
- **all**表示部署三个软件包+omni.conf,
- **omniop**表示部署BoostKit-omniop_{omni_version}.zip
- **deps**表示部署Dependency_library_{os_type}.zip
- **extension**表示部署boostkit-omniop-spark-{spark_version}-{omni_version}-aarch64.zip
- **conf**表示部署omni.conf

可通过`,`分隔符对部署包进行组合：
```shell
# 部署所有包时执行
python main.py all
# 部署BoostKit_omniop_{omni_version}.zip和omni.conf时，执行
python main.py omniop,conf
```