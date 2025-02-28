import logging
import os

import pytest
from pathlib import Path

from omniadvisor.utils.logger import logger


@pytest.fixture(scope="class", autouse=True)
def class_fixture(test_logger):
    test_logger.info("初始化日志测试模块")
    global tmplog
    tmplog = "tmp.log"
    # 临时替换FileHandler 用于测试
    orihandler = ""
    orihandler_index = ""
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            orihandler = handler
            new_file_handler = logging.FileHandler(tmplog)
            new_file_handler.setLevel(logging.INFO)
            orihandler_index = logger.handlers.index(handler)
            logger.handlers[orihandler_index] = new_file_handler

    yield
    logger.handlers[orihandler_index] = orihandler
    test_logger.info(f"清理日志测试模块 移除临时文件{tmplog}")
    os.remove(tmplog)


class TestLogger:

    def test_log(self, test_logger):
        assert os.path.exists(tmplog), f"日志文件{tmplog}不存在"
        test_logger.info(f"清空日志文{tmplog}件内容")
        with open(tmplog, 'w') as file:
            # 打开文件以写入模式，这将自动清空文件内容
            pass  # 不需要写入任何内容
        logger.debug("This is a debug message")
        logger.info("This is an info message")
        logger.warning("This is a warning message")
        logger.error("This is an error message")
        logger.critical("This is a critical message")
        assert Path(tmplog).stat().st_size > 0, f"文件 {tmplog} 大小为 0 字节,写入失败"

        # 验证日志文件内容
        with open(tmplog, 'r') as file:
            content = file.read()

        assert "This is an info message" in content, "Info message not found in log file"
        assert "This is a warning message" in content, "Warning message not found in log file"
        assert "This is an error message" in content, "Error message not found in log file"
        assert "This is a critical message" in content, "Critical message not found in log file"
