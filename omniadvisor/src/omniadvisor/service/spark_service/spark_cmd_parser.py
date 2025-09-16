#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) Huawei Technologies Co, Ltd. 2023-2023. All rights reserved.

import argparse
from typing import List, Any

from omniadvisor.utils.logger import global_logger

# 命令行开头的字段名称
_CMD_PREFIX_KEY = 'omniadvisor-cmd-prefix'
# 命令中的primary_resource部分
_CMD_PRIMARY_RESOURCE = 'primary_resource'
# 命令行中class字段
_CMD_CLASS_KEY = 'class'
# 单横杠参数的字段
_SINGLE_HORIZONTAL_BAR_KEYS = ['e', 'f', 'i']
# 布尔类型的字段
_BOOLEAN_TYPE_KEYS = ['verbose', 'supervise', 'version', 'usage-error', 'help']
# 需进行转换的字段
_SPARK_CONF_SUPPLEMENT_MAP = {
    'driver-memory': 'spark.driver.memory',
    'driver-cores': 'spark.driver.cores',
    'executor-memory': 'spark.executor.memory',
    'executor-cores': 'spark.executor.cores',
    'num-executors': 'spark.executor.instances',
}


class SparkCMDParser:
    """
    Spark 引擎的解析器
    """
    # 当用户提交-h和--help时需要退回原生执行 因此关闭argparse自带的--help/-h参数解析
    _parser = argparse.ArgumentParser(add_help=False)

    # 参数命中包含'-'的参数，设置dest属性，避免参数解析中'-'被转换为'_'
    # 解析Spark CMD开头，需脚本配合
    _parser.add_argument(f'--{_CMD_PREFIX_KEY}', dest=_CMD_PREFIX_KEY,
                         help="The head of Spark CMD.")

    # SparkSubmit Options 3.3.1
    _parser.add_argument('--class', help="Your application's main class(for Java / Scala apps")
    _parser.add_argument('--conf', action='append', help="Arbitrary Spark configuration property")
    _parser.add_argument('--deploy-mode', dest='deploy-mode',
                         help="Whether to launch the driver program locally (client) or on one of the worker "
                              "machines inside the cluster (cluster)(Default: client)")
    _parser.add_argument('--driver-class-path', dest='driver-class-path')
    _parser.add_argument('--driver-cores', dest='driver-cores')
    _parser.add_argument('--driver-java-options', dest='driver-java-options')
    _parser.add_argument('--driver-library-path', dest='driver-library-path')
    _parser.add_argument('--driver-memory', dest='driver-memory')
    _parser.add_argument('--executor-memory', dest='executor-memory')
    _parser.add_argument('--files')
    _parser.add_argument('--jars')
    _parser.add_argument('--kill')
    _parser.add_argument('--master',
                         help="spark://host:port, mesos://host:port, yarn, k8s://https://host:port, "
                              "or local (Default: local[*]).")
    _parser.add_argument('--name', help="A name of your application")
    _parser.add_argument('--packages')
    _parser.add_argument('--exclude-packages', dest='exclude-packages')
    _parser.add_argument('--proxy-user', dest='proxy-user')
    _parser.add_argument('--py-files', dest='py-files')
    _parser.add_argument('--repositories')
    _parser.add_argument('--status')
    _parser.add_argument('--total-executor-cores', dest='total-executor-cores')

    # Options that do not take arguments boolean变量
    _parser.add_argument('-h', '--help', action='store_true')
    _parser.add_argument('--supervise', action='store_true')
    _parser.add_argument('--usage-error', dest="usage-error", action='store_true')
    _parser.add_argument('--verbose', '-v', action='store_true')
    _parser.add_argument('--version', action='store_true')

    # YARN-only options
    _parser.add_argument('--archives')
    _parser.add_argument('--executor-cores', dest='executor-cores')
    _parser.add_argument('--keytab')
    _parser.add_argument('--num-executors', dest='num-executors')
    _parser.add_argument('--principal')
    _parser.add_argument('--queue')

    # Spark sql 相关选项
    # 支持对于--e这种形式的入参解析 但是--e为spark非官方用法 故在命令reconstruct的时候仍然使用-e的方式重建命令
    _parser.add_argument('-e', '--e', type=str, help='SQL statement to execute.')
    _parser.add_argument('-f', type=str, help='File containing SQL script.')
    _parser.add_argument('-i', help='Initialization SQL file')
    _parser.add_argument('-d', '--define')
    _parser.add_argument('--database')


    @classmethod
    def validate_submit_arguments(cls, args: List) -> None:
        """
        检查当前提交的spark-submit命令是否属于SUBMIT动作的spark命令
        该函数与Spark 3.3.1源码中SparkSubmitArguments.scala文件中的validateSubmitArguments逻辑保持一致

        :param args: 从spark-submit文件中劫持的入参列表
        :raises ValueError: 当未提供任何参数时抛出（args 为空）。
        :raises TypeError: 当参数不属于 SUBMIT 动作时抛出。
        """
        if len(args) == 0:
            raise ValueError("No arguments were provided. At least one argument is required.")

        # 待检查参数列表
        params, unknown = cls._parser.parse_known_args(args=args)
        primary_resource = unknown

        if not primary_resource and vars(params)['class'] is None:
            raise TypeError("This is not a SUBMIT type spark task.")

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        """
        对spark-sql命令解析出来的value做标准化处理
        (该函数虽然暂时不对value做任何处理 但考虑到整体代码结构 保留在此)

        :param value: 原始命令解析出的value
        :return: 标准化的value
        """
        return value

    @classmethod
    def _append_double_dash_args(cls, key, value) -> List:
        """
        用于处理spark命令中以 "--" 作为前缀的参数

        :param key: 参数名
        :param value: 参数值
        :return:
        """
        if key in _BOOLEAN_TYPE_KEYS:
            if value is True:
                return [f'--{key}']
            else:
                return []
        else:
            return [f'--{key}', cls._normalize_value(value)]

    @classmethod
    def parse_cmd(cls, argv: list) -> tuple[dict, dict]:
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

        # 原始参数解析
        params, unknown = cls._parser.parse_known_args(args=argv)

        if unknown:
            index = argv.index(unknown[0])
            params, sub_unknown = cls._parser.parse_known_args(args=argv[:index])
            primary_resource = argv[index:]
            global_logger.warning(f"The remainder unknown params %s "
                                  f"will be add to exec_attr[primary_resource]", primary_resource)
            exec_attr[_CMD_PRIMARY_RESOURCE] = primary_resource

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

        return exec_attr, conf_params

    @classmethod
    def reconstruct_cmd(cls, exec_attr: dict, conf_params: dict) -> list:
        """
        根据执行属性和参数配置，重组命令

        :param exec_attr: 执行属性
        :param conf_params: 参数配置
        :return: cmd_fields 参数列表
        """
        cmd_fields = list()

        # 处理命令行开头
        if _CMD_PREFIX_KEY not in exec_attr:
            raise ValueError(f"Cmd prefix key is not in exec_attr!")
        # 把${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.SparkSubmit通过空格拆分成含有两个元素的参数列表
        cmd_fields += exec_attr.get(_CMD_PREFIX_KEY).split(" ")

        # 将命令行中的class属性提前放置，符合Spark正常执行命令
        if _CMD_CLASS_KEY in exec_attr:
            cmd_fields += cls._append_double_dash_args(_CMD_CLASS_KEY, exec_attr[_CMD_CLASS_KEY])

        # 处理执行属性
        for key, value in exec_attr.items():
            if key in [_CMD_PREFIX_KEY, _CMD_CLASS_KEY]:
                continue
            # 单独处理 unknown = ['xxxxxxx.jar', 'xxxxxx', 'xxxxx'] 这部分未被解析的残余参数追加到末尾 不在此处理
            elif key == _CMD_PRIMARY_RESOURCE:
                continue
            # 单独处理 - 作为前缀的参数
            elif key in _SINGLE_HORIZONTAL_BAR_KEYS:
                cmd_fields += [f'-{key}', cls._normalize_value(value)]
            # 处理剩余 -- 作为前缀的参数
            else:
                cmd_fields += cls._append_double_dash_args(key, value)

        # 处理参数配置
        for key, value in conf_params.items():
            cmd_fields += ['--conf', f'{key}={cls._normalize_value(value)}']

        # 追加primary_resource
        primary_resource = exec_attr.get(_CMD_PRIMARY_RESOURCE)
        if primary_resource:
            cmd_fields += [cls._normalize_value(item) for item in primary_resource]

        global_logger.debug("cmd_fields = %s", cmd_fields)
        return cmd_fields
