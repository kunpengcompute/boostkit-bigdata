import shlex
import subprocess
import re
from typing import Tuple, List
from omniadvisor.utils.logger import logger
from omniadvisor.service.spark_fetcher import SparkFetcher
from constants import COMMON_CONFIG
from omniadvisor.repository.task_repository import TaskRepository


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


class SparkExecutor:
    def submit_spark_task(self, submit_config_str):
        """
        在shell终端提交spark命令
        :param submit_config_str: spark的提交命令
        :return:
        """
        execute_cmd = shlex.split(submit_config_str)
        exitcode, spark_output = run_cmd(execute_cmd)

        # 获取ApplicantID 以及 TimeTaken
        result_dict = self._response_parser(spark_output)

        # 根据ApplicantID获取Trace
        history_server_url = COMMON_CONFIG.get("sparkfetcher", "spark.history.rest.url")
        trace_dict = self._query_trace_from_history_server(history_server_url, result_dict['application_id'])

        # TODO result_dict和trace_dict存入task_repositoryTO
        # TODO 这个部分是只要创建了Task类就可以了吗 Trace要怎么保存 直接赋值吗

        return exitcode, result_dict

    def _query_trace_from_history_server(self, history_server_url, application_id):
        trace_dict = {}
        spark_fetcher = SparkFetcher(history_server_url)
        trace_dict['sql'] = spark_fetcher.get_spark_sql_by_app(application_id)
        trace_dict['stages'] = spark_fetcher.get_spark_stages_by_app(application_id)
        trace_dict['executor'] = spark_fetcher.get_spark_executor_by_app(application_id)
        return trace_dict

    def _response_parser(self, spark_output) -> dict:
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
    exitcode, result_dict = spark_executor.submit_spark_task(sub_cmd)
    logger.info(result_dict)
