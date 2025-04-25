import logging
import os

import pytest
from pathlib import Path

from omniadvisor.utils.logger import global_logger


@pytest.fixture(scope="class", autouse=True)
def class_fixture():
    global tmplog
    tmplog = "tmp.log"
    # 如果原有日志文件存在 删除文件
    if os.path.exists(tmplog):
        os.remove(tmplog)
    # 临时替换FileHandler 用于测试
    orihandler = ""
    orihandler_index = ""
    for handler in global_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            orihandler = handler
            new_file_handler = logging.FileHandler(tmplog)
            new_file_handler.setLevel(logging.INFO)
            orihandler_index = global_logger.handlers.index(handler)
            global_logger.handlers[orihandler_index] = new_file_handler

    yield
    global_logger.handlers[orihandler_index] = orihandler
    os.remove(tmplog)


class TestLogger:

    def test_log(self):
        global_logger.debug("This is a debug message")
        global_logger.info("This is an info message")
        global_logger.warning("This is a warning message")
        global_logger.error("This is an error message")
        global_logger.critical("This is a critical message")
        assert Path(tmplog).stat().st_size > 0, f"文件 {tmplog} 大小为 0 字节,写入失败"

        # 验证日志文件内容
        with open(tmplog, 'r') as file:
            content = file.read()

        assert "This is an info message" in content, "Info message not found in log file"
        assert "This is a warning message" in content, "Warning message not found in log file"
        assert "This is an error message" in content, "Error message not found in log file"
        assert "This is a critical message" in content, "Critical message not found in log file"
