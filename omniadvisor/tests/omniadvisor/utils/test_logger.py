import logging
import io
import pytest

from omniadvisor.utils.logger import global_logger, disable_console_handler


class TestLogger:
    """
    Logger测试类
    """
    def test_logger_output(self):
        """
        测试软件logger输出
        """
        # 创建内存缓冲区
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        # 屏蔽其他handler，使用内存IO作为测试使用
        global_logger.handlers = [handler]

        # log输出
        global_logger.debug("This is a debug message")
        global_logger.info("This is an info message")
        global_logger.warning("This is a warning message")
        global_logger.error("This is an error message")
        global_logger.critical("This is a critical message")

        # 验证日志文件内容
        content = log_stream.getvalue()
        assert global_logger.level == logging.INFO
        assert "This is an debug message" not in content, "Debug message should not found in log file"
        assert "This is an info message" in content, "Info message not found in log file"
        assert "This is a warning message" in content, "Warning message not found in log file"
        assert "This is an error message" in content, "Error message not found in log file"
        assert "This is a critical message" in content, "Critical message not found in log file"

    def test_third_party_logger_suppression(self):
        """
        测试第三方库日志级别是否设置为 ERROR
        """
        # 以smac logger作为示例
        smac_logger = logging.getLogger('smac')
        # 创建内存缓冲区
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        # 屏蔽其他handler，使用内存IO作为测试使用
        smac_logger.handlers = [handler]

        # log输出
        smac_logger.debug("This is a debug message")
        smac_logger.info("This is an info message")
        smac_logger.warning("This is a warning message")
        smac_logger.error("This is an error message")
        smac_logger.critical("This is a critical message")

        # 验证日志文件内容
        content = log_stream.getvalue()
        assert smac_logger.level == logging.ERROR
        assert "This is an debug message" not in content, "Debug message should not found in log file"
        assert "This is an info message" not in content, "Info message should not found in log file"
        assert "This is a warning message" not in content, "Warning message should not found in log file"
        assert "This is an error message" in content, "Error message not found in log file"
        assert "This is a critical message" in content, "Critical message not found in log file"

    def test_disable_console_handler(self):
        """
        测试禁用控制台处理器的功能
        """
        # 创建一个测试记录器并添加控制台处理器
        test_logger = logging.getLogger('test_logger')
        console_handler = logging.StreamHandler()
        console_handler.name = 'consoleHandler'
        test_logger.addHandler(console_handler)

        # 验证处理器存在
        assert console_handler in test_logger.handlers

        # 调用禁用函数
        disable_console_handler(test_logger)

        # 验证处理器已被移除
        assert console_handler not in test_logger.handlers
