import shlex
import subprocess

from omniadvisor.utils.exception import ParameterException
from constants import CODING_TYPE
from omniadvisor.utils.logger import logger


def run_cmd(args_list, verbose=True):
    """
    在shell终端执行的命令
    :param args_list: 需要执行的命令
    :param verbose: 是否开启verbose, 开启, 则会打印命令
    :return: 返回执行结果
    """
    if verbose:
        logger.info(f"Excutor system command: {' '.join(args_list)}")
    proc = subprocess.Popen(' '.join(args_list), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    session_timeout = 3600
    s_output, s_err = proc.communicate(timeout=session_timeout)
    s_return = proc.returncode
    if s_output:
        s_output = s_output.decode('utf-8').split('\n')
    return s_return, s_output, s_err


def execute_submit_cmd(execute_cmd):
    s_return_code, s_output, s_err = run_cmd(execute_cmd)
    if s_return_code != 0:
        logger.error(s_err.decode(CODING_TYPE))
        raise ParameterException(' '.join(execute_cmd))
    result = "\n".join(s_output)
    logger.info(s_err.decode(CODING_TYPE))
    logger.info(f"\n{result}")


class SparkExecutor:
    def submit_spark_task(self, submit_config_str):
        """
        在shell终端提交spark命令
        :param submit_config_str: spark的提交命令
        :return:
        """
        execute_cmd = shlex.split(submit_config_str)
        execute_submit_cmd(execute_cmd)
