#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

# initialize log directory
import logging.config
import os
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")


def get_logger(filename="omnimv"):
    """
    creating a logger if necessary with the specified name
    :param filename: the specified name of a logger
    :return:
    """

    logger = logging.getLogger("omnimv")
    os.makedirs(LOG_DIR, mode=0o750, exist_ok=True)
    logging_yml_path = os.path.join(BASE_DIR, "config/logging.yml")
    if not os.path.exists(logging_yml_path):
        raise FileNotFoundError("Please check logging.yml exists")
    with open(logging_yml_path, 'rt') as file:
        config = yaml.safe_load(file.read())
    # set filename
    log_file = os.path.join(LOG_DIR, filename)
    config['handlers']['file']['filename'] = log_file
    logging.config.dictConfig(config)
    return logger