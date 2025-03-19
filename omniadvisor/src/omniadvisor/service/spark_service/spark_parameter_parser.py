import argparse
import shlex
from typing import Dict
from omniadvisor.utils.logger import logger
from src.omniadvisor.utils.constant import OA_CONF
from omniadvisor.service.parameter_parser import ParserInterface


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

    def process_parameter(self) -> Dict:
        """
        解析提交后的命令，得到解析的参数后，以字典的形式保存
        :return:dict
        返回一个包含所有提取出的基础参数的字典,每个键代表一个参数名,详见add_argument中的配置的参数。
        对应的值为该参数的具体设置或值。
        base_params = {name:"", conf_params:{}, exec_attr:{}}
        """
        base_params = self._get_base_params()
        return base_params

    def _get_base_params(self):
        """
        使用argparse来解析命令行中的输入命令，并返回解析后的非空参数，以字典类型保存
        :return:
        """
        if not self._submit_cmd:
            raise ValueError("Submit command cannot be null")
        param_start_index = self._submit_cmd.strip().index(" ")
        base_params = {}
        conf_params = {}
        exec_attr = {}
        args = shlex.split(self._submit_cmd[param_start_index:])
        params, unknown = self._parser.parse_known_args(args=args)
        options_dict = vars(params)
        for key, value in options_dict.items():
            # 过滤空值参数
            if value is None:
                continue
            # 提取执行相关参数保存至exec_attr
            if key not in OA_CONF.SPARK_NON_EXEC_ATTR_LIST:
                exec_attr[key] = value
            # 提取conf配置
            elif key == "conf":
                for confitem in value:
                    parts = confitem.split("=")
                    if len(parts) != 2:
                        raise ValueError(f"conf{parts}的格式不符合key=value的形式")
                    confkey, confvalue = parts
                    # 处理重复键问题，例如合并值为列表
                    if confkey in conf_params:
                        if isinstance(conf_params[confkey], list):
                            conf_params[confkey].append(confvalue)
                        else:
                            conf_params[confkey] = [conf_params[key], value]
                        logger.warn(f"{confkey}被重复配置,多次配置值如下{conf_params[confvalue]}")
                    else:
                        conf_params[confkey] = confvalue
            else:
                base_params[key] = value

            # exec_attr 和 conf_params合并进入base_params
            base_params["exec_attr"] = exec_attr
            base_params["conf_params"] = conf_params

        return base_params
