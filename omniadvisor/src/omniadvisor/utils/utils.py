
import subprocess
from typing import Tuple, List
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


