import re

from common.constant import OA_CONF
from omniadvisor.utils.utils import run_cmd


class SparkExecutor:

    @classmethod
    def submit_spark_task(cls, execute_cmd: str):
        """
        在shell终端提交spark命令

        :param execute_cmd: Spark的提交命令
        :return:
        """
        exitcode, spark_output = run_cmd(execute_cmd)

        return exitcode, spark_output

    @classmethod
    def parser_spark_output(cls, spark_output: str):
        """
        解析Spark输出，获得spark提交命令、Application ID和执行用时

        :param spark_output: Spark执行输出
        :return:
        """
        spark_output = spark_output.split("\n")
        spark_submit_cmd = ""
        application_id = ""
        time_taken_list = []
        for item in spark_output:
            if "deploy.SparkSubmit" in item:
                spark_submit_cmd = item.strip()
                break
        for item in reversed(spark_output):
            if "Application Id" in item:
                pattern = r"Application Id:\s*(.*)"
                match = re.search(pattern, item)
                if match:
                    application_id = match.group(1).strip()
            elif "Time taken" in item:
                pattern = r"Time taken:\s*(.*)seconds"
                match = re.search(pattern, item)
                if match:
                    time_taken_list.append(float(match.group(1).strip()))

        if time_taken_list:
            total_time_taken = sum(time_taken_list)
        else:
            total_time_taken = OA_CONF.exec_fail_return_runtime

        return spark_submit_cmd, application_id, total_time_taken
