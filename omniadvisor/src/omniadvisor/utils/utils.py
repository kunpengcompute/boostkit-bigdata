import os
import json
import uuid
import subprocess
from typing import Tuple, List, Dict

from ConfigSpace import ConfigurationSpace, UniformIntegerHyperparameter, Constant, CategoricalHyperparameter, \
    UniformFloatHyperparameter

from common.constant import OA_CONF
from omniadvisor.utils.logger import logger


def run_cmd(submit_cmd) -> Tuple[int, str]:
    """
    在shell终端执行命令，并返回执行结果。

    :param submit_cmd: 需要执行的命令。
    :type submit_cmd: str
    :return: 返回一个包含退出状态码和命令输出的元组。
             第一个元素是整数类型的退出状态码（0表示成功），
             第二个元素是一个保存在列表中的字符串 ["xxxx"]
    :rtype: Tuple[int, str]
    """
    logger.debug(f"Executor system command: {submit_cmd}")
    exitcode, data = subprocess.getstatusoutput(submit_cmd)
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

