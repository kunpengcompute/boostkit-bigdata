import logging
import os

import pytest

from pathlib import Path
from omniadvisor.utils.logger import logger, LOGGING_CONFIG


@pytest.fixture(scope="class", autouse=True)
def class_fixture():
    logging.info("\n初始化日志测试模块")
    yield
    logging.info("\n清理日志测试模块")


class TestLogger:

    def test_log(self):
        log_file_path = LOGGING_CONFIG['handlers']['fileHandler']['filename']
        assert os.path.exists(log_file_path), f"日志文件{log_file_path}不存在"
        logging.info("清空日志文件内容")
        with open(log_file_path, 'w') as file:
            # 打开文件以写入模式，这将自动清空文件内容
            pass  # 不需要写入任何内容
        logger.debug("This is a debug message")
        logger.info("This is an info message")
        logger.warning("This is a warning message")
        logger.error("This is an error message")
        logger.critical("This is a critical message")
        assert Path(log_file_path).stat().st_size > 0, f"文件 {log_file_path} 大小为 0 字节,写入失败"

        # 验证日志文件内容
        with open(log_file_path, 'r') as file:
            content = file.read()

        assert "This is an info message" in content, "Info message not found in log file"
        assert "This is a warning message" in content, "Warning message not found in log file"
        assert "This is an error message" in content, "Error message not found in log file"
        assert "This is a critical message" in content, "Critical message not found in log file"
