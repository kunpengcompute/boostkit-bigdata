import os
import subprocess



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

