import re
from omniadvisor.utils.utils import run_cmd


class SparkExecutor:
    def submit_spark_task(self, execute_cmd):
        """
        在shell终端提交spark命令
        :param submit_config_str: spark的提交命令
        :return:
        """
        exitcode, spark_output = run_cmd(execute_cmd)

        return exitcode, spark_output

    def parser_spark_output(self, spark_output) -> dict:
        return self._reg_match_application_id_and_time_taken(spark_output)

    def _reg_match_application_id_and_time_taken(self, spark_output):
        spark_output = spark_output.split("\n")
        spark_submit_cmd = ""
        application_id = ""
        total_time_taken = -1.0
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
        return spark_submit_cmd, application_id, float(total_time_taken)
