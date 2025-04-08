from unittest.mock import patch
from omniadvisor.service.spark_service.spark_executor import SparkExecutor


class TestSparkExecutor:
    def setup_class(self):
        """在每个测试用例之前运行，初始化 SparkExecutor 实例"""
        self.spark_executor = SparkExecutor()

    @patch('omniadvisor.service.spark_service.spark_executor.run_cmd')
    def test_submit_spark_task(self, mock_run_cmd):
        """
        测试 submit_spark_task 方法
        """
        # 模拟输入的 execute_cmd 和返回值
        execute_cmd = "spark-submit --class MyClass my_app.jar"
        mock_run_cmd.return_value = (0, ["Output line 1 \nOutput line 2"])

        # 调用方法
        exitcode, spark_output = self.spark_executor.submit_spark_task(execute_cmd)

        # 验证结果
        assert exitcode == 0
        assert spark_output == ["Output line 1 \nOutput line 2"]
        mock_run_cmd.assert_called_once_with("spark-submit --class MyClass my_app.jar")

    def test_parser_spark_output(self):
        """
        测试 parser_spark_output 方法
        """
        # 模拟 spark_output 数据
        spark_output ="Some log line\n" \
                      "INFO deploy.SparkSubmit: Starting Spark job\n" \
                      "Another log line\n" \
                      "Application Id: application_1234567890\n" \
                      "Time taken: 12.34 seconds"

        # 调用方法
        spark_submit_cmd, application_id, time_taken = self.spark_executor.parser_spark_output(spark_output)

        # 验证结果
        expected_result = {
            "spark_submit_cmd": "INFO deploy.SparkSubmit: Starting Spark job",
            "application_id": "application_1234567890",
            "time_taken": 12.34
        }
        assert spark_submit_cmd == expected_result["spark_submit_cmd"]
        assert application_id == expected_result["application_id"]
        assert time_taken == expected_result["time_taken"]

    def test_reg_match_application_id_and_time_taken(self):
        """
        测试 _reg_match_application_id_and_time_taken 方法
        """
        # 模拟 spark_output 数据
        spark_output ="Some log line\n" \
                      "INFO deploy.SparkSubmit: Starting Spark job\n" \
                      "Another log line\n" \
                      "Application Id: application_1234567890\n" \
                      "Time taken: 12.34 seconds"

        # 调用方法
        spark_submit_cmd, application_id, time_taken = self.spark_executor._reg_match_application_id_and_time_taken(
            spark_output)

        # 验证结果
        assert spark_submit_cmd == "INFO deploy.SparkSubmit: Starting Spark job"
        assert application_id == "application_1234567890"
        assert time_taken == 12.34

    def test_parser_spark_output_with_empty_output(self):
        """
        测试 parser_spark_output 方法处理空输出的情况
        """
        # 模拟空的 spark_output 数据
        spark_output = ""

        # 调用方法
        spark_submit_cmd, application_id, time_taken = self.spark_executor.parser_spark_output(spark_output)

        # 验证结果
        assert spark_submit_cmd == ""
        assert application_id == ""
        assert time_taken == -1.0
