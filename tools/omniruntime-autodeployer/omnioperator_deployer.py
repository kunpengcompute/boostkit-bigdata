import os
import shutil
import subprocess

from omniruntime_conf import OmniRuntimeConf

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
        self.omniop_pkg = f"BoostKit-omniop_{self.omni_conf.omnioperator.omni_version}.zip"
        self.omniop_inner_pkg = (f"boostkit-omniop-operator-{self.omni_conf.omnioperator.omni_version}-aarch64-"
                                 f"{self.omni_conf.omnioperator.os_type}.tar.gz")
        self.spark_extension_pkg = (f"boostkit-omniop-spark-{self.omni_conf.omnioperator.spark_version}-"
                                    f"{self.omni_conf.omnioperator.omniop_version}-aarch64.zip")
        self.spark_extension_inner_pkg = (f"boostkit-omniop-spark-{self.omni_conf.omnioperator.spark_version}-"
                                          f"{self.omni_conf.omnioperator.omniop_version}-aarch64-{self.omni_conf.omnioperator.os_type}.zip")
        self.operator_dir = f"{self.omni_conf.omnioperator.deploy_home}/omni-operator"


    def omniop_deploy(self):
        # 创建部署目录{deploy_home}/omni-operator
        os.mkdir(self.operator_dir)
        # 解压BoostKit-omniop_{omniop_version}-.zip到{deploy_home}目录，得到tar包
        shutil.unpack_archive(f"{self.omni_conf.omnioperator.deploy_home}/{self.omniop_pkg}", self.omni_conf.omnioperator.deploy_home)
        # 解压tar包至{deploy_home}/omni-operator/lib目录下
        cmd = ['tar', '-zxPf', f"{self.omni_conf.omnioperator.deploy_home}/{self.omniop_inner_pkg}", '--strip-components',
               '1', '-C', f"{self.operator_dir}/lib", '--touch']
        subprocess.run(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        # 解压Dependency_library.zip到{deploy_home}目录，得到tar包