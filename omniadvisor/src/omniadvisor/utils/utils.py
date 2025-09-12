import json
import os
import subprocess
import uuid
from typing import Tuple, List, Dict

from common.constant import OmniAdvisorConf
from common.exceptions import UnknownEncodingError
from omniadvisor.utils.logger import global_logger


def run_cmd(cmd_fields: list) -> Tuple[int, str]:
    """
    在shell终端执行命令，并返回执行结果。

    :param cmd_fields: 需要执行的命令字段List
    :return: 返回一个包含退出状态码和命令输出的元组。
             第一个元素是整数类型的退出状态码（0表示成功），
             第二个元素是一个保存在列表中的字符串 ["xxxx"]
    """
    global_logger.debug(f"Executor system command: {cmd_fields}")
    kwargs = {
        'stdout': subprocess.PIPE,
        'stderr': subprocess.STDOUT,
        'shell': False,
        'text': True
    }
    result = subprocess.run(cmd_fields, **kwargs)
    return result.returncode, result.stdout


def save_trace_data(data: List[Dict[str, str]], data_dir):
    """
    用于把各类trace信息以文件的形式保存，并返回文件的绝对路径
    :param data:通过RESTAPI 获取的list[dict]形式文件
    :param data_dir: 数据
    :return:
    """
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        # 数据目录权限设置为750
        os.chmod(data_dir, 0o750)
    file_path = "".join([data_dir, "/", str(uuid.uuid4())])
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        # 数据文件权限设置为640
        os.chmod(file_path, 0o640)
        global_logger.debug(f"数据已成功保存到 {file_path}")
    except IOError as e:
        raise IOError(f"出现IO错误: {e}") from e
    except Exception as e:
        raise Exception(f"保存过程中出现错误: {e}") from e
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
        global_logger.error(f"Error: The file '{file_path}' does not exist.")
    except json.JSONDecodeError:
        global_logger.error(f"Error: The file '{file_path}' is not a valid JSON file.")
    except Exception as e:
        global_logger.error(f"An unexpected error occurred: {e}")

    return data


def float_format(to_format: float) -> str:
    """
    按 decimal_digits 保留小数位数
    :param to_format: 待格式化的的小数
    :return:
    """
    return "{:.{}f}".format(to_format, OmniAdvisorConf.decimal_digits)


def safe_read_file(filepath: str):
    """
    以多种不同的编码形式，读取文件
    """
    encoding_types = ['utf-8', 'utf-8-sig', 'gb18030', 'latin1']
    for enc in encoding_types:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise UnknownEncodingError(
        f'Only encoding in {encoding_types} is supported, encoding of current file {filepath} is unknown'
    )


def file_exists(filepath: str):
    """
    检查文件是否存在
    """
    return os.path.isfile(filepath)
