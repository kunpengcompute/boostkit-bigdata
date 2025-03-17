import shlex
import re
from omniadvisor.utils.utils import run_cmd


class SparkExecutor:
    def submit_spark_task(self, execute_cmd):
        """
        在shell终端提交spark命令
        :param submit_config_str: spark的提交命令
        :return:
        """
        execute_cmd = shlex.split(execute_cmd)
        exitcode, spark_output = run_cmd(execute_cmd)

        # 获取ApplicantID 以及 TimeTaken
        result_dict = self._response_parser(spark_output)
        return exitcode, result_dict

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
