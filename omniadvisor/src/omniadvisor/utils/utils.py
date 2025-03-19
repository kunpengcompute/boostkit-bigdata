import os
import json
import uuid
import subprocess
from typing import Tuple, List, Dict
from omniadvisor.utils.logger import logger


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
        print(f"出现IO错误: {e}")
    except Exception as e:
        print(f"保存过程中出现错误: {e}")
    return file_path

