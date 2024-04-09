# 1 部署脚本使用前置条件
需要一台跳转jumper节点，用于将omnioperator的软件包传送到各个server/agent节点上进行部署，该jumper节点需要具备如下条件：

1、jumper节点与server/agent节点网络相同，可以用server节点作为jumper节点。

2、jumper节点需要安装python 3.5以上版本，且安装了yaml和paramiko两个库。如果没有安装，需要执行一下命令进行安装：
```shell
pip3 install pyyaml
pip3 install paramiko
```
3、server和agent用于ssh登录的端口、用户名、密码都需要保持一致

4、当前只支持OmniOperator 1.4.0以上版本部署

# 2 部署前配置
## 2.1 omniruntime.yaml配置
根据以下配置说明完成omniruntime.yaml配置
```yaml
env: # 环境配置
  username: root # ssh登录用户名
  password: 123456 # ssh登录密码
  port: 22 # 需要各个节点ssh登录端口一致
  jumper_host: "192.168.2.1" # 通过jumper传送软件包到其他节点上
  server_hosts: # server节点ip，支持server1这种hostname形式、192.168.1.1的ip格式、192.168.1.10-210网段形式
      - "server1"
  agent_hosts: # agent节点ip，支持agent1这种hostname形式、192.168.1.1的ip格式、192.168.1.10-210网段形式
      - "agent1"
      - "192.168.2.3"
      - "192.168.2.4-10"

omnioperator:
  omni_version: 1.4.0 # omnioperator版本，在闭源软件包名中可找到，BoostKit_omniop_1.4.0.zip
  spark_version: 3.3.1 # Spark版本，当前支持3.1.1和3.3.1两个版本
  os_type: "centos" # 操作系统版本，当前支持openeuler和centos两个版本
  omni_home: "/opt/omni-operator" # 部署omnioperator的目录，通常固定为/opt/omni-operator
  deploy_home: "/home/omniop" # 存放部署安装包和部署脚本的目录
```

## 2.2 omni.conf配置
根据OmniOperator用户指南完成omni.conf的配置

# 3 部署前准备
1、在omniruntime.yaml上配置的omnioperator.deploy_home目录上，上传以下三个软件包，具体获取方法参考OmniRuntime特性指南：
```shell
BoostKit-omniop_{omni_version}.zip # OmniOperator闭源软件包
boostkit-omniop-spark-{spark_version}-{omni_version}-aarch64.zip # OmniOperator开源软件包
Dependency_library_{os_type}.zip # 从Dependency_library.zip上解压出来
```

2、将本部署脚本工程omniruntime-autodeployer上传至omnioperator.deploy_home目录上

# 4 特性部署
进入{omnioperator.deploy_home}/omniruntime-autodeployer目录，执行如下命令：
```shell
python main.py all|omniop|deps|extension|conf
```
其中，
- **all**表示部署三个软件包+omni.conf,
- **omniop**表示部署BoostKit-omniop_{omni_version}.zip
- **deps**表示部署Dependency_library_{os_type}.zip
- **extension**表示部署boostkit-omniop-spark-{spark_version}-{omni_version}-aarch64.zip
- **conf**表示部署omni.conf

可通过 | 对部署包进行组合：
```shell
# 部署所有包时执行
python main.py all
# 部署BoostKit_omniop_{omni_version}.zip和omni.conf时，执行
python main.py omniop|conf
```