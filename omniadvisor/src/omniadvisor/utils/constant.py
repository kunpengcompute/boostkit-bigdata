import configparser
import os
from enum import Enum, auto


def _load_common_config():
    """
    使用configparser库加载common_config
    :return: 返回common_config的内容
    """
    cur_dir_path = os.path.dirname(os.path.abspath(__file__))
    common_config = configparser.ConfigParser()
    config_path = f"{os.path.dirname(os.path.dirname(os.path.dirname(cur_dir_path)))}/" \
                  f"config/common_config.cfg"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"{config_path} does not exist")
    common_config.read(config_path, encoding='utf-8')
    return common_config


# 任务状态，供django使用
class TaskStatus:
    success = 'Success'
    fail = 'Fail'
    running = 'Running'
    all = [success, fail, running]


# 存几种调优算法，目前暂无赋值
class TunerType:
    iterative = 'iterative'
    expert = 'expert'
    transfer = 'transfer'
    all = [iterative, expert, transfer]


class RetestWay:
    hijacking = 'hijacking'
    backend = 'backend'
    all = [hijacking, backend]


# 项目通用配置
COMMON_CONFIG = _load_common_config()
CODING_TYPE = "utf-8"
SPARK_NON_EXEC_ATTR_LIST = {"name", "conf"}

# 根目录
PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # 获取当前脚本所在的目录

# 数据存储路径
DATA_DIR = "".join([PROJECT_DIR, "/data"])

# spark trace的存储路径
SPARK_TRACE_DATA_DIR = "".join([DATA_DIR, "/spark_trace"])
