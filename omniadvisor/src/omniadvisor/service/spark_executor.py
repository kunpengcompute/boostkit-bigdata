import shlex
import subprocess
import re
from typing import Tuple, List
from omniadvisor.utils.exception import ParameterException
from omniadvisor.utils.logger import logger


def run_cmd(args_list, verbose=True) -> Tuple[int, List[str]]:
    """
    在shell终端执行命令，并返回执行结果。

    :param args_list: 需要执行的命令组成的列表。
    :type args_list: list
    :param verbose: 是否开启详细模式。如果开启，则会打印出即将执行的命令。
    :type verbose: bool, optional
    :return: 返回一个包含退出状态码和命令输出的元组。
             第一个元素是整数类型的退出状态码（0表示成功），
             第二个元素是一个字符串列表，每个字符串代表输出中的一行。
    :rtype: Tuple[int, List[str]]
    """
    if verbose:
        logger.info(f"Executor system command: {' '.join(args_list)}")
    exitcode, data = subprocess.getstatusoutput(' '.join(args_list))
    data = data.split("\n")
    return exitcode, data


def execute_submit_cmd(execute_cmd):
    exitcode, data = run_cmd(execute_cmd)
    if exitcode != 0:
        raise ParameterException(' '.join(execute_cmd))
    logger.debug(data)
    return exitcode, data


class SparkExecutor:
    def submit_spark_task(self, submit_config_str):
        """
        在shell终端提交spark命令
        :param submit_config_str: spark的提交命令
        :return:
        """
        execute_cmd = shlex.split(submit_config_str)
        exitcode, spark_output = execute_submit_cmd(execute_cmd)
        return self._response_parser(spark_output)

    def _response_parser(self, spark_output):
        return self._reg_match_application_id_and_time_taken(spark_output)

    def _reg_match_application_id_and_time_taken(self, spark_output):
        result_dict = {}
        # TODO 看看是否需要增加对于spark-class这条命令的捕获和解析
        for item in reversed(spark_output):
            if "Application Id" in item:
                pattern = r"Application Id:\s*(.*)"
                match = re.search(pattern, item)
                if match:
                    result_dict["application_id"] = match.group(1).strip()
            elif "Time taken" in item:
                pattern = r"Time taken:\s*(.*)seconds"
                match = re.search(pattern, item)
                if match:
                    result_dict["time_taken"] = match.group(1).strip()
            if "application_id" in item and "time_taken" in item:
                break
        return result_dict


if __name__ == "__main__":
    spark_executor = SparkExecutor()
    # NOTE 在当前的实现模式下 sub_cmd命令本身不应该包含有双引号或者单引号 否则被shlex.split分隔后无法拼接回去
    sub_cmd = 'source /etc/profile && spark-sql --master yarn -f /home/x30041342/testsql/test.sql'
    result_dict = spark_executor.submit_spark_task(sub_cmd)
    logger.info(result_dict)