from omniadvisor.utils.utils import load_common_config
"""
定义的常数
"""
CODING_TYPE = "utf-8"
COMMON_CONFIG = load_common_config()
SPARK_NON_EXEC_ATTR_LIST = ["master", "deploy_mode", "class", "name", "jar", "packages", "exclude_packages",
                            "repositories", "py_files", "files", "archives", "d", "database", "e", "execute",
                            "f", "file", "i", "conf"]
