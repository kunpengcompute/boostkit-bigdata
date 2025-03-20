import argparse
from abc import ABC, abstractmethod


class ParserInterface(ABC):
    """
    Spark, Hive等各类引擎的解析器的接口类
    :param submit_cmd: 提交的命令
    """

    def __init__(self, submit_cmd=""):
        self._parameters = {}
        self._parser = argparse.ArgumentParser()
        self._submit_cmd = submit_cmd

    @abstractmethod
    def process_parameter(self):
        pass
