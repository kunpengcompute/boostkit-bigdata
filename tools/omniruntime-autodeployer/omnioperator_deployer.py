import os
import shutil

import paramiko

import utils

from omniruntime_conf import OmniRuntimeConf
from ssh import SSH

class OmniOperatorDeployer(object):
    """
    OmniOperator软件包部署，其中
    BoostKit-omniop_{omniop_version}-.zip的结构如下：
    |- BoostKit-omniop_{omniop_version}-.zip
      |- boostkit-omniop-operator-{omniop_version}-aarch64-centos.tar.gz
      |- boostkit-omniop-operator-{omniop_version}-aarch64-openeuler.tar.gz
      |- Kunpeng BoostKit User License Agreement 2.0.txt
      |- version.txt
      |_ 鲲鹏应用使能套件BoostKit用户许可协议 2.0.txt

    boostkit-omniop-spark-{spark_version}-{omniop_version}-aarch64.zip的结构如下
    |- boostkit-omniop-spark-{spark_version}-{omniop_version}-aarch64.zip
      |- boostkit-omniop-spark-{spark_version}-{omniop_version}-aarch64-centos.zip
        |- boostkit-omniop-spark-{spark_version}-{omniop_version}-aarch64.jar
        |- dependencies.tar.gz
      |- boostkit-omniop-spark-{spark_version}-{omniop_version}-aarch64-openeuler.zip
        |- boostkit-omniop-spark-{spark_version}-{omniop_version}-aarch64.jar
        |_ dependencies.tar.gz

    Dependency_library.zip的结构如下：
    |- Dependency_library.zip
      |- Dependency_library_centos.zip
      |_ Dependency_library_openeuler.zip
    """
    def __init__(self):
        self.omni_conf = OmniRuntimeConf()
        # 解析omniruntime.yaml配置文件
        self.omni_conf.parse("omniruntime.yaml")
        print('parse omniruntime.yaml done!')
        self.operator_dir = f"{self.omni_conf.omnioperator.deploy_home}/omni-operator"

        self.omniop_pkg = f"BoostKit-omniop_{self.omni_conf.omnioperator.omni_version}.zip"
        self.omniop_inner_pkg = (f"boostkit-omniop-operator-{self.omni_conf.omnioperator.omni_version}-aarch64-"
                                 f"{self.omni_conf.omnioperator.os_type}.tar.gz")
        self.omniop_inner_dir = f"boostkit-omniop-operator-{self.omni_conf.omnioperator.omni_version}-aarch64"
        self.dependency_pkg = f"Dependency_library_{self.omni_conf.omnioperator.os_type}.zip"

        self.spark_extension_pkg = (f"boostkit-omniop-spark-{self.omni_conf.omnioperator.spark_version}-"
                                    f"{self.omni_conf.omnioperator.omni_version}-aarch64.zip")
        self.spark_extension_inner_pkg = (f"boostkit-omniop-spark-{self.omni_conf.omnioperator.spark_version}-"
                                          f"{self.omni_conf.omnioperator.omni_version}-aarch64-{self.omni_conf.omnioperator.os_type}.zip")
        self.spark_extension_jar = (f"boostkit-omniop-spark-{self.omni_conf.omnioperator.spark_version}-"
                                    f"{self.omni_conf.omnioperator.omni_version}-aarch64.jar")
        self.spark_dependency_pkg = (f"dependencies.tar.gz")



    # 部署黄区软件包
    def omniop_deploy(self):
        # 创建部署目录{deploy_home}/omni-operator
        if not os.path.exists(self.operator_dir):
            os.mkdir(self.operator_dir)
            os.mkdir(f"{self.operator_dir}/lib")
        # 解压BoostKit-omniop_{omniop_version}-.zip到{deploy_home}目录，得到tar包
        shutil.unpack_archive(f"{self.omni_conf.omnioperator.deploy_home}/{self.omniop_pkg}", self.omni_conf.omnioperator.deploy_home)
        # 解压tar包至{deploy_home}/omni-operator/lib目录下
        shutil.unpack_archive(f"{self.omni_conf.omnioperator.deploy_home}/{self.omniop_inner_pkg}", f"{self.omni_conf.omnioperator.deploy_home}")
        utils.copy_files(f"{self.omni_conf.omnioperator.deploy_home}/{self.omniop_inner_dir}", f"{self.operator_dir}/lib")

    # 部署依赖包
    def dependencies_lib_deploy(self):
        # 解压Dependency_library_{os_type}.zip到{deploy_home}/omni-operator/lib目录下
        shutil.unpack_archive(f"{self.omni_conf.omnioperator.deploy_home}/{self.dependency_pkg}",f"{self.operator_dir}/lib")

    # 部署spark-extension蓝区软件包
    def spark_extension_deploy(self):
        shutil.unpack_archive(f"{self.omni_conf.omnioperator.deploy_home}/{self.spark_extension_pkg}", self.omni_conf.omnioperator.deploy_home)
        shutil.unpack_archive(f"{self.omni_conf.omnioperator.deploy_home}/{self.spark_extension_inner_pkg}", self.omni_conf.omnioperator.deploy_home)
        shutil.copy2(f"{self.omni_conf.omnioperator.deploy_home}/{self.spark_extension_jar}", f"{self.operator_dir}/lib")
        shutil.unpack_archive(f"{self.omni_conf.omnioperator.deploy_home}/{self.spark_dependency_pkg}", f"{self.operator_dir}/lib")

    # 部署omni.conf文件
    def omni_conf_deploy(self):
        # 创建部署目录{deploy_home}/omni-operator/conf目录
        os.mkdir(f"{self.operator_dir}/conf")
        shutil.copy2(f"omni.conf", f"{self.operator_dir}/conf")


    def trans2node(self, host):
        new_ssh = SSH()
        new_ssh.open(host, self.omni_conf.env.port, self.omni_conf.env.username, self.omni_conf.env.password)
        # 部署软件包
        transport = paramiko.Transport(host, self.omni_conf.env.port)
        transport.connect(username=self.omni_conf.env.username, password=self.omni_conf.env.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(f"{self.omni_conf.omnioperator.deploy_home}/omni-operator.tar.gz", f"/opt/omni-operator.tar.gz")
        new_ssh.send_shell_cmd(f"tar -zxPf /opt/omni-operator.tar.gz -C /opt --touch && rm /opt/omni-operator.tar.gz -rf")
        transport.close()
        # 文件权限修改
        new_ssh.send_shell_cmd(f"chmod -R 550 {self.omni_conf.omnioperator.omni_home}/lib/*")
        new_ssh.send_shell_cmd(f"chmod 640 {self.omni_conf.omnioperator.omni_home}/conf/omni.conf")
        # omni环境变量清理
        new_ssh.send_shell_cmd(f"sed -i '/$LD_LIBRARY_PATH:\/opt\/omni-operator\/lib/d' ~/.bashrc;")
        new_ssh.send_shell_cmd(f"sed -i '/export OMNI_HOME/d' ~/.bashrc;")
        # omni环境变量配置
        new_ssh.send_shell_cmd(f"sed -i '$a export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{self.omni_conf.omnioperator.omni_home}' ~/.bashrc;")
        new_ssh.send_shell_cmd(f"sed -i '$a export OMNI_HOME={self.omni_conf.omnioperator.omni_home}' ~/.bashrc;")
        new_ssh.close()

    def trans2all(self):
        # scp前先把软件包打包好
        shutil.make_archive(self.operator_dir, "gztar", self.omni_conf.omnioperator.deploy_home, "omni-operator")
        print('make archive omni-operator.tar.gz done!')
        for host in self.omni_conf.env.server_hosts:
            self.trans2node(host)
            print(f'deploy {host} done!')

        for host in self.omni_conf.env.agent_hosts:
            self.trans2node(host)
            print(f'deploy {host} done!')

        print(f'deploy {len(self.omni_conf.env.server_hosts) + len(self.omni_conf.env.agent_hosts)}')