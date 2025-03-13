import pytest
from unittest.mock import patch
from omniadvisor.service.spark_executor import run_cmd, execute_submit_cmd, SparkExecutor, ParameterException


@patch('subprocess.getstatusoutput')
def test_run_cmd_verbose(mock_getstatusoutput):
    # 配置mock对象
    mock_getstatusoutput.return_value = (0, "line1\nline2\nline3")

    # 调用run_cmd函数
    exitcode, output = run_cmd(['echo', 'hello'], verbose=True)

    # 确认getstatusoutput被正确调用
    mock_getstatusoutput.assert_called_once_with("echo hello")


@patch('omniadvisor.service.spark_executor.run_cmd')
def test_execute_submit_cmd_success(mock_run_cmd):
    # 设置mock返回值
    mock_run_cmd.return_value = (0, ['success output'])

    # 调用execute_submit_cmd并验证它不会抛出异常
    try:
        execute_submit_cmd(['some', 'command'])
    except Exception as e:
        pytest.fail(f"execute_submit_cmd raised an exception unexpectedly: {e}")


@patch('omniadvisor.service.spark_executor.run_cmd')
def test_execute_submit_cmd_failure(mock_run_cmd):
    # 设置mock返回值以模拟失败情况
    mock_run_cmd.return_value = (1, ['error message'])

    # 验证execute_submit_cmd在命令失败时抛出ParameterException
    with pytest.raises(ParameterException):
        execute_submit_cmd(['some', 'command'])


class TestSparkExecutor:
    def setup_class(self):
        self.spark_executor = SparkExecutor()

    @patch('omniadvisor.service.spark_executor.execute_submit_cmd')
    @patch('shlex.split')
    def test_submit_spark_task(self, mock_shlex_split, mock_execute_submit_cmd):
        # 设置mock返回值
        mock_execute_submit_cmd.return_value = (0, ['success output'])
        mock_shlex_split.return_value = ['spark-submit', 'config']

        self.spark_executor.submit_spark_task("spark-submit config")

        # 确认execute_submit_cmd被正确调用
        mock_execute_submit_cmd.assert_called_once_with(['spark-submit', 'config'])
