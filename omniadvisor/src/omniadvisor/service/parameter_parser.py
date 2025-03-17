import argparse
from abc import ABC, abstractmethod


class ParserInterface(ABC):
    """
    引擎的解析器的接口类
    :param submit_cmd: 提交的命令
    """

    def __init__(self, submit_cmd=""):
        self._parameters = {}
        self._parser = argparse.ArgumentParser()
        # NOTE 这个变量用来保存文件内的sql 需要评估目前是否需要这个字段
        self._sql = ""
        self._submit_cmd = submit_cmd

    @abstractmethod
    def process_parameter(self):
        pass
