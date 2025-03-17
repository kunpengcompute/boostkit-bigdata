import os
import configparser
import subprocess
from typing import Tuple, List
from omniadvisor.utils.logger import logger


def load_common_config():
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


def run_cmd(args_list) -> Tuple[int, List[str]]:
    """
    在shell终端执行命令，并返回执行结果。

    :param args_list: 需要执行的命令组成的列表。
    :type args_list: list
    :return: 返回一个包含退出状态码和命令输出的元组。
             第一个元素是整数类型的退出状态码（0表示成功），
             第二个元素是一个字符串列表，每个字符串代表输出中的一行。
    :rtype: Tuple[int, List[str]]
    """
    logger.debug(f"Executor system command: {' '.join(args_list)}")
    exitcode, data = subprocess.getstatusoutput(' '.join(args_list))
    data = data.split("\n")
    logger.info(data)
    return exitcode, data


