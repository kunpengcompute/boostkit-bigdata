import pytest
from common.constant import OA_CONF
from unittest.mock import patch, MagicMock
from datetime import datetime

from omniadvisor.service.spark_service.spark_executor import SparkExecutor


class TestSparkExecutor:
    """
    SparkExecutor测试类
    """

    def setup_class(self):
        """
        在每个测试用例之前运行，初始化 SparkExecutor 实例
        """
        # 构建executor
        self.spark_executor = SparkExecutor()

        # 构造测试数据
        self.cmd_fields = ['spark-submit', '--class', 'MyClass', 'my_app.jar']
        self.timeout = 10
        self.exitcode = 0
        self.application_id = 'application_123456_303209'
        self.time_taken_list = [23.42, 25.83]
        self.output = MagicMock(stdout=None,
                                stderr=f'Client: Submitting application {self.application_id} to ResourceManager\n'
                                       f'XXXXXX\n'
                                       f'Time taken: {self.time_taken_list[0]} seconds\n'
                                       f'Time taken: {self.time_taken_list[1]} seconds\n'
                                       f'Spark Log [INFO]: Time taken: {self.time_taken_list[1]} seconds\n')

    @patch('omniadvisor.service.spark_service.spark_executor.run_cmd')
    def test_submit_spark_task(self, mock_run_cmd):
        """
        测试 submit_spark_task 方法
        """
        mock_run_cmd.return_value = (self.exitcode, self.output)

        # 调用方法
        exec_result = self.spark_executor.submit_spark_task(cmd_fields=self.cmd_fields, timeout=self.timeout)

        # 验证结果
        mock_run_cmd.assert_called_once_with(cmd_fields=['timeout', str(self.timeout)] + self.cmd_fields)
        assert exec_result.exitcode == self.exitcode
        assert exec_result.output == self.output
        assert exec_result.application_id == self.application_id
        assert exec_result.duration == OA_CONF.exec_fail_return_runtime
        assert type(exec_result.start_time) is datetime
        assert type(exec_result.end_time) is datetime

    def test_parser_spark_output(self):
        """
        测试 parser_spark_output 方法
        """
        # 正确解析spark output
        application_id, total_time_taken = self.spark_executor._parser_spark_output(self.output)
        # 验证结果
        assert application_id == self.application_id
        assert total_time_taken == OA_CONF.exec_fail_return_runtime

        # 解析spark output缺失application id
        self.output.stderr = (f'Time taken: {self.time_taken_list[0]} seconds\n'
                              f'Time taken: {self.time_taken_list[1]} seconds\n'
                              f'Spark Log [INFO]: Time taken: {self.time_taken_list[1]} seconds\n')
        # 验证结果
        with pytest.raises(RuntimeError):
            _, _ = self.spark_executor._parser_spark_output(self.output)

        # 解析spark output缺失行首Time taken
        self.output.stderr = f'Spark Log [INFO]: Time taken: {self.time_taken_list[1]} seconds\n'
        # 验证结果
        with pytest.raises(RuntimeError):
            _, _ = self.spark_executor._parser_spark_output(self.output)

        # 解析spark output为空
        self.output.stderr = ''
        # 验证结果
        with pytest.raises(RuntimeError):
            _, _ = self.spark_executor._parser_spark_output(self.output)

