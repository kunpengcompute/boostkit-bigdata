import logging
import colorlog
import pytest


@pytest.fixture(scope="session")
def test_logger():
    # 创建logger
    test_logger = logging.getLogger('test_logger')
    test_logger.setLevel(logging.DEBUG)  # 设置最低的日志级别

    # 如果logger已经有handler，避免重复添加handler
    if not test_logger.handlers:
        # 创建控制台处理器
        ch = colorlog.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义输出格式
        ch.setFormatter(colorlog.ColoredFormatter(
            '%(log_color)s%(levelname)s:%(name)s:%(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        ))

        # 将处理器添加到logger
        test_logger.addHandler(ch)

    # 返回logger实例
    yield test_logger