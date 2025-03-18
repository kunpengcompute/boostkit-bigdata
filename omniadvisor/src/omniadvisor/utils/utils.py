import json
import subprocess

from omniadvisor.utils.logger import logger


def call_binary(command, *args, binary_path):
    """
    调用二进制文件并传递参数。
    :param command: 要执行的命令
    :param args: 命令的参数
    :param binary_path: 二进制文件的路径
    """
    # 构造完整的命令
    full_command = [binary_path, command] + list(args)

    # 调用二进制文件
    result = subprocess.run(full_command, check=True, capture_output=True, text=True)
    return result


def read_json_file(file_path: str):
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
    else:
        logger.info(data)
    return data
