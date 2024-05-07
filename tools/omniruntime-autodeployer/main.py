import sys
from omnioperator_deployer import OmniOperatorDeployer

valid_params = {"all", "omniop", "deps", "extension", "conf"}

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python main.py all,omniop,deps,extension,conf')
        sys.exit(1)
    argvs = sys.argv[1].split(",")
    params = set()
    for argv in argvs:
        if argv not in valid_params:
            print('Usage: python main.py all,omniop,deps,extension,conf')
            sys.exit(1)
        params.add(argv)
    omniop_deployer = OmniOperatorDeployer()
    if "all" in params or "omniop" in params:
        omniop_deployer.omniop_deploy()
    if "all" in params or "deps" in params:
        omniop_deployer.dependencies_lib_deploy()
    if "all" in params or "extension" in params:
        omniop_deployer.spark_extension_deploy()
    if "all" in params or "conf" in params:
        omniop_deployer.omni_conf_deploy()
    omniop_deployer.trans2all()
