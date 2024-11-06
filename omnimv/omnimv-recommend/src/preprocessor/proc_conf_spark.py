#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import os
import configparser
from enum import Enum
import ctypes


class DataType(Enum):
    Q = 0
    MV = 1
    Q_MV = 2


def init_config():
    cur_dir_path = os.path.dirname(os.path.abspath(__file__))
    config = configparser.ConfigParser()
    config_path = f"{os.path.dirname(os.path.dirname(cur_dir_path))}/config/omnimv_config_spark.cfg"
    config.read(config_path, encoding="utf-8")
    return config


def get_spark_schema_path():
    g_cfg = init_config()
    return g_cfg.get("schema", "schema_path")


def get_table_bytes_path():
    g_cfg = init_config()
    return g_cfg.get("schema", "table_bytes_path")


def get_spark_database():
    g_cfg = init_config()
    return g_cfg.get("spark", "database")

def get_spark_mock_database():
    g_cfg = init_config()
    return g_cfg.get("spark", "mock_database")

def spark_mock_database_truncate_enable():
    g_cfg = init_config()
    return g_cfg.get("spark", "mock_database_truncate_enable") == "true"

def get_logparser_path():
    g_cfg = init_config()
    return g_cfg.get("spark", "logparser_jar_path")


def get_cache_plugin_path():
    g_cfg = init_config()
    return g_cfg.get("spark", "cache_plugin_jar_path")


def get_hdfs_inputpath():
    g_cfg = init_config()
    return g_cfg.get("spark", "hdfs_input_path")


def get_hdfs_outputpath():
    g_cfg = init_config()
    return g_cfg.get("spark", "hdfs_output_path")


def get_spark_sqls_path():
    g_cfg = init_config()
    return g_cfg.get("spark", "sqls_path")


def get_query_log_timestamp():
    g_cfg = init_config()
    return g_cfg.get("spark", 'q_log_start_time'), g_cfg.get("spark", 'q_log_end_time')


def get_mv_log_timestamp():
    g_cfg = init_config()
    return g_cfg.get("spark", 'mv_log_start_time'), g_cfg.get("spark", 'mv_log_end_time')


def get_large_table_threshold():
    g_cfg = init_config()
    large_table_threshold = g_cfg.get("spark", "large_table_threshold")
    try:
        return int(large_table_threshold)
    except ValueError as e:
        raise ValueError("large_table_threshold should be int type") from e


def get_mv_limit():
    g_cfg = init_config()
    mv_limit = g_cfg.get("spark", "mv_limit")
    try:
        return int(mv_limit)
    except ValueError as e:
        raise ValueError("mv_limit should be int type") from e


def get_mv_output_path():
    g_cfg = init_config()
    return g_cfg.get("result", "mv_output_path")


def get_spark_model_path():
    g_cfg = init_config()
    return g_cfg.get("train", "spark_model_path")


def get_spark_train_max_depth():
    g_cfg = init_config()
    max_depth = g_cfg.get("train", "max_depth")
    try:
        return int(max_depth)
    except ValueError as e:
        raise ValueError("max_depth should be int type") from e


def get_spark_train_learning_rate():
    g_cfg = init_config()
    learning_rate = g_cfg.get("train", "learning_rate")
    try:
        return float(learning_rate)
    except ValueError as e:
        raise ValueError("learning_rate should be float type") from e


def get_spark_master():
    g_cfg = init_config()
    return g_cfg.get("yarn", "spark_master")


def get_spark_app_name():
    g_cfg = init_config()
    return g_cfg.get("yarn", "app_name")


def get_spark_num_executors():
    g_cfg = init_config()
    return g_cfg.get("yarn", "spark_num_executors")


def get_spark_executor_memory():
    g_cfg = init_config()
    return g_cfg.get("yarn", "spark_executor_memory")


def get_driver_memory():
    g_cfg = init_config()
    return g_cfg.get("yarn", "spark_driver_memory")


def get_executor_cores():
    g_cfg = init_config()
    return g_cfg.get("yarn", "executor_cores")


def get_session_timeout():
    g_cfg = init_config()
    session_timeout = g_cfg.get("yarn", "session_timeout")
    try:
        return int(session_timeout)
    except ValueError as e:
        raise ValueError("session_timeout should be int type") from e


def get_mv_metadata_path():
    g_cfg = init_config()
    return g_cfg.get("yarn", "mv_metadata_path")


def get_omnimv_version():
    return f"\nProduct Name: Kunpeng BoostKit\nProduct Version: 24.0.T15\nComponent Name: "\
           f"BoostKit-omnimv\nComponent Version: 1.2.0"