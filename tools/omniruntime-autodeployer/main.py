from omnioperator_deployer import OmniOperatorDeployer

if __name__ == '__main__':
    omniop_deployer = OmniOperatorDeployer()
    omniop_deployer.omniop_deploy()
    omniop_deployer.spark_extension_deploy()
    omniop_deployer.omni_conf_deploy()
    omniop_deployer.trans2all()
