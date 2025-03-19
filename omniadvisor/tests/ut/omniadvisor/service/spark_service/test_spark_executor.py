import pytest
from unittest.mock import patch, MagicMock
from omniadvisor.service.spark_service.spark_executor import run_cmd, SparkExecutor


@patch('subprocess.getstatusoutput')
def test_run_cmd(mock_getstatusoutput):
    # 配置mock对象
    mock_getstatusoutput.return_value = (0, "line1\nline2\nline3")

    # 调用run_cmd函数
    exitcode, output = run_cmd(['echo', 'hello'])

    # 确认getstatusoutput被正确调用
    mock_getstatusoutput.assert_called_once_with("echo hello")


class TestSparkExecutor:
    def setup_class(self):
        self.spark_executor = SparkExecutor()

    def test_submit_spark_task(self):
        # 模拟run_cmd函数返回值
        mock_run_cmd = MagicMock(return_value=(0, ["Application Id: app-12345", "Time taken: 10.5 seconds"]))

        # 使用patch装饰器替换run_cmd的实际实现为我们mock的对象
        with patch('omniadvisor.service.spark_service.spark_executor.run_cmd', mock_run_cmd):
            exitcode, spark_out_put, result_dict = self.spark_executor.submit_spark_task("spark-submit --class MyClass myApp.jar")

            # 验证run_cmd是否被调用
            mock_run_cmd.assert_called_once()

            # 验证返回值
            assert exitcode == 0
            assert result_dict["application_id"] == "app-12345"
            assert result_dict["time_taken"] == "10.5"
