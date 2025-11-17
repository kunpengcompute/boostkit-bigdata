# -*- coding: utf-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
""" user_configs 供包内模块使用
"""
import configparser
import os

_project_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
_common_config_path = os.path.join(_project_base_dir, 'config', 'common_config.ini')


def _load_ini_config(config_path: str) -> configparser.ConfigParser:
    """
    使用configparser库加载common_config

    :param config_path: 配置文件路径
    :return: 返回common_config的内容
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"{config_path} does not exist")

    common_config = configparser.ConfigParser(interpolation=None)
    common_config.read(config_path, encoding='utf-8')

    return common_config


user_configs = _load_ini_config(config_path=_common_config_path)