import os
import configparser

from ConfigSpace import ConfigurationSpace, UniformIntegerHyperparameter, Constant, CategoricalHyperparameter, \
    UniformFloatHyperparameter

from omniadvisor.utils.utils import read_json_file


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


def _load_config_space():
    """
    将knob_space 转为 configuration_space
    """
    repo_path = os.path.join(PROJECT_DIR, 'config', 'spark_repo.json')
    knob_space = read_json_file(repo_path)
    config_space = ConfigurationSpace()
    for knob_name, knob_value in knob_space.items():
        if not knob_value['enable']:
            continue
        knob_type = knob_value.get('type')

        if knob_type == 'uniform_int':
            hyperparameter = UniformIntegerHyperparameter(
                name=knob_name,
                lower=knob_value.get('lower'),
                upper=knob_value.get('upper'),
                default_value=knob_value.get('default'),
            )
        elif knob_type == 'constant':
            hyperparameter = Constant(
                name=knob_name,
                value=knob_value.get('default')
            )
        elif knob_type == 'categorical':
            hyperparameter = CategoricalHyperparameter(
                name=knob_name,
                choices=knob_value.get('choices'),
                default_value=knob_value.get('default'),
                weights=knob_value.get('weights'),
            )
        elif knob_type == 'uniform_float':
            hyperparameter = UniformFloatHyperparameter(
                name=knob_name,
                lower=knob_value.get('lower'),
                upper=knob_value.get('upper'),
                default_value=knob_value.get('default'),
            )
        else:
            raise RuntimeError(f'Not supported config type: {knob_type}')
        config_space.add(hyperparameter)
    return config_space


# 任务状态，供django使用
class TaskStatus:
    success = 'Success'
    fail = 'Fail'
    running = 'Running'
    all = [success, fail, running]


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

# 全局参数空间变量
CONFIG_SPACE = _load_config_space()
