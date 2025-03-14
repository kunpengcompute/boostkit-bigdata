import shlex
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

    def _get_base_params(self):
        """
        使用argparse来解析命令行中的输入命令，并返回解析后的参数，以字典类型保存
        :return:
        """
        if not self._submit_cmd:
            raise ValueError("Submit command cannot be null")
        param_start_index = self._submit_cmd.strip().index(" ")
        base_params = {}
        args = shlex.split(self._submit_cmd[param_start_index:])
        params, unknown = self._parser.parse_known_args(args=args)
        options_dict = vars(params)
        for key, value in options_dict.items():
            if value is not None:
                base_params[key] = value
        return base_params


class SparkParameterParser(ParserInterface):
    """
    Spark 引擎的解析器
    :param submit_cmd: 提交的命令
    """
    def __init__(self, submit_cmd):
        super().__init__(submit_cmd)
        self._parser.add_argument("--master",
                                  help="spark://host:port, mesos://host:port, yarn, k8s://https://host:port, "
                                       "or local (Default: local[*]).")
        self._parser.add_argument("--deploy-mode",
                                  help="Whether to launch the driver program locally (client) or on one of the worker "
                                       "machines inside the cluster (cluster)(Default: client)")
        self._parser.add_argument('--class', help="Your application's main class(for Java / Scala apps")
        self._parser.add_argument('--name', help="A name of your application")
        self._parser.add_argument('--jar')
        self._parser.add_argument('--packages')
        self._parser.add_argument('--exclude-packages')
        self._parser.add_argument('--repositories')
        self._parser.add_argument('--py-files')
        self._parser.add_argument('--files')
        self._parser.add_argument('--archives')
        self._parser.add_argument('--conf', action='append', help="Arbitrary Spark configuration property")
        self._parser.add_argument('--driver-memory')
        self._parser.add_argument('--driver-java-options')
        self._parser.add_argument('--driver-library-path')
        self._parser.add_argument('--driver-class-path')
        self._parser.add_argument('--executor-memory')
        self._parser.add_argument('--proxy-user')
        self._parser.add_argument('--verbose')
        self._parser.add_argument('--version')
        self._parser.add_argument('--driver-cores')
        self._parser.add_argument('--supervise')
        self._parser.add_argument('--kill')
        self._parser.add_argument('--status')
        self._parser.add_argument('--total-executor-cores')
        self._parser.add_argument('--executor-cores')
        self._parser.add_argument('--num-executors')
        self._parser.add_argument('--principal')
        self._parser.add_argument('--keytab')
        self._parser.add_argument('--queue')
        self._parser.add_argument('-d', "--database", metavar="database")
        self._parser.add_argument('-e', '--execute', type=str, help='SQL statement to execute.')
        self._parser.add_argument('-f', '--file', type=str, help='File containing SQL script.')
        self._parser.add_argument('-i', help='Initialization SQL file')
        self._parser.add_argument('remainder', nargs=argparse.REMAINDER, help="Collect the rest of parameters")

    def process_parameter(self):
        """
        解析提交后的命令，得到解析的参数后，以字典的形式保存
        :return:dict
        返回一个包含所有提取出的基础参数的字典。每个键代表一个参数名，对应的值为该参数的具体设置或值。
        """
        base_params = self._get_base_params()
        return base_params
