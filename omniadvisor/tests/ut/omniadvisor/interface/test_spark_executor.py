import pytest
from unittest.mock import patch, MagicMock

from omniadvisor.interface.spark_executor import run_cmd, execute_submit_cmd, SparkExecutor, ParameterException


@patch('subprocess.Popen')
def test_run_cmd_verbose(mock_popen):
    # 配置mock对象
    process_mock = MagicMock()
    attrs = {'communicate.return_value': (b'output', b'error'), 'returncode': 0}
    process_mock.configure_mock(**attrs)
    mock_popen.return_value = process_mock

    # 调用run_cmd函数
    result = run_cmd(['echo', 'hello'], verbose=True)

    # 验证结果
    assert result == (0, ['output'], b'error')


@patch('omniadvisor.interface.spark_executor.run_cmd')
def test_execute_submit_cmd_success(mock_run_cmd):
    # 设置mock返回值
    mock_run_cmd.return_value = (0, ['success output'], b'')

    # 调用execute_submit_cmd并验证它不会抛出异常
    try:
        execute_submit_cmd(['some', 'command'])
    except Exception as e:
        pytest.fail(f"execute_submit_cmd raised an exception unexpectedly: {e}")


@patch('omniadvisor.interface.spark_executor.run_cmd')
def test_execute_submit_cmd_failure(mock_run_cmd):
    # 设置mock返回值以模拟失败情况
    mock_run_cmd.return_value = (1, [], b'error message')

    # 验证execute_submit_cmd在命令失败时抛出ParameterException
    with pytest.raises(ParameterException):
        execute_submit_cmd(['some', 'command'])


class TestSparkExecutor:
    def setup_class(self):
        self.spark_executor = SparkExecutor()

    @patch('omniadvisor.interface.spark_executor.execute_submit_cmd')
    @patch('shlex.split')
    def test_submit_spark_task(self, mock_shlex_split, mock_execute_submit_cmd):
        # 设置mock返回值
        mock_shlex_split.return_value = ['spark-submit', 'config']

        self.spark_executor.submit_spark_task("spark-submit config")

        # 确认execute_submit_cmd被正确调用
        mock_execute_submit_cmd.assert_called_once_with(['spark-submit', 'config'])
