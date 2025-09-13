# !/usr/bin/env python
# -*-coding:utf-8-*-

"""
日志打印类
"""
import logging
import os
import functools
import time
from logging import handlers
from logging.handlers import RotatingFileHandler

base_path = os.path.dirname(__file__)
LOG_PATH = os.path.join(base_path, r"log")
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)


class Logger(object):
    """
    日志类
    """

    # 日志级别关系映射
    level_relations = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    def __init__(
        self,
        filename,
        level="info",
        when="D",
        back_count=10,
        fmt="%(asctime)s- %(process)d-%(thread)d-%(threadName)s-%(pathname)s-%(funcName)s[line:%(lineno)d]"
        " - %(levelname)s: %(message)s",
    ):
        filenames = f"{LOG_PATH}{os.sep}{filename}"
        self.log = logging.getLogger(filenames)
        format_str = logging.Formatter(fmt)  # 设置日志格式
        self.log.setLevel(self.level_relations.get(level))  # 设置日志级别
        stream_handle = logging.StreamHandler()  # 往屏幕上输出
        stream_handle.setFormatter(format_str)  # 设置屏幕上显示的格式
        # TODO 待删除
        # log_handle = handlers.TimedRotatingFileHandler(
        #     filename=filenames, when=when, backupCount=back_count, encoding="utf-8"
        # )
        # 往文件里写入#指定间隔时间自动生成文件的处理器
        # 实例化TimedRotatingFileHandler
        # interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
        # S 秒
        # M 分
        # H 小时、
        # D 天、
        # W 每星期（interval==0时代表星期一）
        # midnight 每天凌晨
        # 设置文件里写入的格式
        # TODO 待删除
        # log_handle.setFormatter(format_str)
        # 把对象加到logger里
        self.log.addHandler(stream_handle)
        # TODO 待删除
        # self.log.addHandler(log_handle)


__other_logs = Logger("server.log", level="info")
logger = __other_logs.log

__kmc_logs = Logger("kmc.log", level="info")
kmc_logger = __kmc_logs.log


def method_metric_log(func):
    """
    利用装饰器打印日志，包括enter、exit和消耗时间
    :param func: 装饰函数
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kw):
        start = time.time()
        try:
            logger.info(f"[method {func.__name__}] enter.")
            return func(*args, **kw)
        finally:
            # 用于统计方法耗时
            logger.info(f"[method {func.__name__}] exit.")
            logger.info(f"[method {func.__name__}] cost: {time.time() - start}s.")

    return wrapper