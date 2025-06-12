#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) Huawei Technologies Co, Ltd. 2023-2023. All rights reserved.

import argparse

from omniadvisor.utils.logger import global_logger

# 命令行开头的字段名称
_CMD_PREFIX_KEY = 'omniadvisor-cmd-prefix'
# 命令行开头的字段名称
_CMD_UNKNOWN_KEY = 'unknown'
# 单横杠参数的字段
_SINGLE_HORIZONTAL_BAR_KEYS = ['e', 'f', 'i']
# 布尔类型的字段
_BOOLEAN_TYPE_KEYS = ['verbose', 'supervise', 'kill', 'status']
# 需进行转换的字段
_SPARK_CONF_SUPPLEMENT_MAP = {
    'num-executors': 'spark.executor.instances',
    'executor-cores': 'spark.executor.cores',
    'driver-cores': 'spark.driver.cores',
    'executor-memory': 'spark.executor.memory',
    'driver-memory': 'spark.driver.memory'
}


class SparkCMDParser:
    """
    Spark 引擎的解析器
    """
    _parser = argparse.ArgumentParser()
    # 参数命中包含'-'的参数，设置dest属性，避免参数解析中'-'被转换为'_'
    # 解析Spark CMD开头，需脚本配合
    _parser.add_argument(f'--{_CMD_PREFIX_KEY}', dest=_CMD_PREFIX_KEY,
                         help="The head of Spark CMD.")
    # 解析Spark相关参数
    _parser.add_argument('--master',
                          help="spark://host:port, mesos://host:port, yarn, k8s://https://host:port, "
                               "or local (Default: local[*]).")
    _parser.add_argument('--deploy-mode', dest='deploy-mode',
                          help="Whether to launch the driver program locally (client) or on one of the worker "
                               "machines inside the cluster (cluster)(Default: client)")
    _parser.add_argument('--class', help="Your application's main class(for Java / Scala apps")
    _parser.add_argument('--name', help="A name of your application")
    _parser.add_argument('--jar')
    _parser.add_argument('--packages')
    _parser.add_argument('--exclude-packages', dest='exclude-packages')
    _parser.add_argument('--repositories')
    _parser.add_argument('--py-files', dest='py-files')
    _parser.add_argument('--files')
    _parser.add_argument('--archives')
    _parser.add_argument('--conf', action='append', help="Arbitrary Spark configuration property")
    _parser.add_argument('--driver-memory', dest='driver-memory')
    _parser.add_argument('--driver-java-options', dest='driver-java-options')
    _parser.add_argument('--driver-library-path', dest='driver-library-path')
    _parser.add_argument('--driver-class-path', dest='driver-class-path')
    _parser.add_argument('--executor-memory', dest='executor-memory')
    _parser.add_argument('--proxy-user', dest='proxy-user')
    _parser.add_argument('--version')
    _parser.add_argument('--driver-cores', dest='driver-cores')
    _parser.add_argument('--total-executor-cores', dest='total-executor-cores')
    _parser.add_argument('--executor-cores', dest='executor-cores')
    _parser.add_argument('--num-executors', dest='num-executors')
    _parser.add_argument('--principal')
    _parser.add_argument('--keytab')
    _parser.add_argument('--queue')
    _parser.add_argument('-d', '--database')
    _parser.add_argument('-e', type=str, help='SQL statement to execute.')
    _parser.add_argument('-f', type=str, help='File containing SQL script.')
    _parser.add_argument('-i', help='Initialization SQL file')
    # boolean变量
    _parser.add_argument('--verbose', action='store_true')
    _parser.add_argument('--supervise', action='store_true')
    _parser.add_argument('--kill', action='store_true')
    _parser.add_argument('--status', action='store_true')

    @staticmethod
    def _normalize_value(value):
        """
        对spark-sql命令解析出来的value做标准化处理，将含有空格字符串类型的值用双引号包裹

        :param value: 原始命令解析出的value
        :return: 标准化的value
        """
        if isinstance(value, str) and ' ' in value:
            return f'"{value}"'
        else:
            return value

    @classmethod
    def parse_cmd(cls, argv: list):
        """
        解析提交后的命令，得到解析的参数后，以字典的形式保存
        返回一个包含所有提取出的基础参数的字典,每个键代表一个参数名,详见add_argument中的配置的参数。
        对应的值为该参数的具体设置或值。
        base_params = {conf_params:{}, exec_attr:{}}

        :param argv: 执行命令参数
        :return:dict
        """
        if not argv:
            raise ValueError("Submit command cannot be null")

        # 输出内容初始化
        exec_attr = dict()
        conf_params = dict()

        # 参数解析
        params, unknown = cls._parser.parse_known_args(args=argv)
        for key, value in vars(params).items():
            # 过滤空值参数
            if value is None:
                continue
            # 提取conf配置
            if key == 'conf':
                for conf_item in value:
                    conf_key, conf_value = conf_item.split('=', 1)
                    conf_params[conf_key] = conf_value
            # 将 --num-executors --executors-cores --driver-cores这类的配置归类到conf_params中便于后续重建命令
            elif key in _SPARK_CONF_SUPPLEMENT_MAP.keys():
                conf_params[_SPARK_CONF_SUPPLEMENT_MAP[key]] = value
            # 其余参数保存至exec_attr
            else:
                exec_attr[key] = value

        if unknown:
            global_logger.warning(f"The remainder unknown params {unknown} will be add to exec_attr[unknown]")
            exec_attr[_CMD_UNKNOWN_KEY] = unknown

        return exec_attr, conf_params

    @classmethod
    def reconstruct_cmd(cls, exec_attr: dict, conf_params: dict):
        """
        根据执行属性和参数配置，重组命令

        :param exec_attr: 执行属性
        :param conf_params: 参数配置
        :return:
        """
        cmd_fields = list()

        # 处理命令行开头
        if _CMD_PREFIX_KEY not in exec_attr:
            raise ValueError(f"Cmd prefix key is not in exec_attr!")
        cmd_fields += [exec_attr.get(_CMD_PREFIX_KEY)]

        # 处理执行属性
        for key, value in exec_attr.items():
            if key == _CMD_PREFIX_KEY:
                continue
            # 单独处理 remainder = ['--uk1','xxxx','--uk2','xxxx'] 这部分未被解析的残余参数
            if key == _CMD_UNKNOWN_KEY:
                cmd_fields += [cls._normalize_value(item) for item in value]
                continue
            # 分别处理-开头的参数与--开头的参数
            if key in _SINGLE_HORIZONTAL_BAR_KEYS:
                cmd_fields += [f'-{key}', cls._normalize_value(value)]
            else:
                if key in _BOOLEAN_TYPE_KEYS and value is True:
                    cmd_fields += [f'--{key}']
                else:
                    cmd_fields += [f'--{key}', cls._normalize_value(value)]

        # 处理参数配置
        for key, value in conf_params.items():
            cmd_fields += ['--conf', f'{key}={cls._normalize_value(value)}']

        cmd = ' '.join(cmd_fields)
        return cmd
