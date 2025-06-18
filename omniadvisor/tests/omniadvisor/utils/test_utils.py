import subprocess
from unittest.mock import patch,mock_open
from omniadvisor.utils.utils import run_cmd, save_trace_data, float_format  # 假设你的函数定义在 'your_module' 中


class TestRunCmd:
    @patch('subprocess.run')
    def test_run_cmd_success(self, mock_run):
        # 配置mock的返回值
        mock_run.return_value.returncode = 0,
        mock_run.return_value.stdout = "Success output"

        exitcode, output = run_cmd('echo hello')

        # 验证结果
        assert exitcode[0] == 0
        assert output == "Success output"
        mock_run.assert_called_once()

    @patch('subprocess.run')
    def test_run_cmd_failure(self, mock_run):
        # 配置mock的返回值
        mock_run.return_value.returncode = 1,
        mock_run.return_value.stdout = "Error output"

        exitcode, output = run_cmd('false')

        # 验证结果
        assert exitcode[0] == 1
        assert output == "Error output"


class TestTraceDataSaver:
    @patch('os.makedirs')
    @patch('uuid.uuid4', return_value="test-uuid")
    def test_save_trace_data_success(self, mock_uuid, mock_makedirs):
        data = [{"key": "value"}]
        data_dir = "/tmp"
        m = mock_open()
        with patch('builtins.open', m, create=True):
            file_path = save_trace_data(data, data_dir)

        expected_path = f"{data_dir}/test-uuid"
        assert file_path == expected_path
        m.assert_called_once_with(expected_path, 'w', encoding='utf-8')

    @patch('os.makedirs')
    def test_save_trace_data_ioerror(self, mock_makedirs):
        data = [{"key": "value"}]
        data_dir = "/tmp"
        m = mock_open()
        m.side_effect = IOError("IO Error")

        with patch('os.makedirs'), \
             patch('uuid.uuid4', return_value="test-uuid"), \
             patch('builtins.open', m, create=True):
            try:
                save_trace_data(data, data_dir)
                assert False, "Expected an IOError to be raised."
            except IOError as e:
                assert str(e) == "出现IO错误: IO Error", f"Unexpected error message: {str(e)}"


    @patch('os.makedirs')
    def test_save_trace_data_exception(self, mock_makedirs):
        data = [{"key": "value"}]
        data_dir = "/tmp"
        m = mock_open()
        m.side_effect = Exception("Unexpected error")

        with patch('os.makedirs'), \
             patch('uuid.uuid4', return_value="test-uuid"), \
             patch('builtins.open', m, create=True):
            try:
                save_trace_data(data, data_dir)
                assert False, "Expected an Exception to be raised."
            except Exception as e:
                assert str(e) == "保存过程中出现错误: Unexpected error", f"Unexpected error message: {str(e)}"


class TestFloatFormat:

    def test_basic_formatting(self):
        # 正常四舍五入
        assert float_format(3.14159) == "3.142"
        # 补零
        assert float_format(2.5) == "2.500"
        # 整数输入
        assert float_format(100) == "100.000"

    def test_boundary_values(self):
        # 零值
        assert float_format(0) == "0.000"
        # 负数
        assert float_format(-3.14159) == "-3.142"

    def test_special_floats(self):
        # 无穷大
        assert float_format(float('inf')) == "inf"
        # 负无穷大
        assert float_format(float('-inf')) == "-inf"
