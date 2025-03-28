import os
import json
import uuid
import subprocess
from typing import Tuple, List, Dict

from ConfigSpace import ConfigurationSpace, UniformIntegerHyperparameter, Constant, CategoricalHyperparameter, \
    UniformFloatHyperparameter

from common.constant import OA_CONF
from omniadvisor.utils.logger import logger


def run_cmd(args_list) -> Tuple[int, str]:
    """
    在shell终端执行命令，并返回执行结果。

    :param args_list: 需要执行的命令组成的列表。
    :type args_list: list
    :return: 返回一个包含退出状态码和命令输出的元组。
             第一个元素是整数类型的退出状态码（0表示成功），
             第二个元素是一个字符串
    :rtype: Tuple[int, str]
    """
    logger.debug(f"Executor system command: {' '.join(args_list)}")
    exitcode, data = subprocess.getstatusoutput(' '.join(args_list))
    logger.debug(data)
    return exitcode, data


def trace_data_saver(data: List[Dict[str, str]], data_dir):
    """
    用于把各类trace信息以文件的形式保存，并返回文件的绝对路径
    :param data:通过RESTAPI 获取的list[dict]形式文件
    :param data_dir: 数据
    :return:
    """
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    file_path = "".join([data_dir, "/", str(uuid.uuid4())])
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"数据已成功保存到 {file_path}")
    except IOError as e:
        raise IOError(f"出现IO错误: {e}")
    except Exception as e:
        raise Exception(f"保存过程中出现错误: {e}")
    return file_path


def read_json_file(file_path: str):
    """
    读取json文件
    :param file_path: json文件路径
    :return:
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            # 将 JSON 文件内容解析为 Python 对象
            data = json.load(file)
    except FileNotFoundError:
        logger.error(f"Error: The file '{file_path}' does not exist.")
    except json.JSONDecodeError:
        logger.error(f"Error: The file '{file_path}' is not a valid JSON file.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

    return data


def load_config_space() -> ConfigurationSpace:
    """
    将knob_space 转为 configuration_space
    :return: ConfigurationSpace 对象
    """
    repo_path = os.path.join(OA_CONF.PROJECT_DIR, 'config', 'spark_repo.json')
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
