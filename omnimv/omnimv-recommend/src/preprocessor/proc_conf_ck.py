#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import configparser
import ctypes
import getpass
import os
import socket


def init_config():
    cur_dir_path = os.path.dirname(os.path.abspath(__file__))
    config = configparser.ConfigParser()
    config_path = f"{os.path.dirname(os.path.dirname(cur_dir_path))}/" \
                  f"config/omnimv_config_clickhouse.cfg"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"{config_path} does not exist")
    config.read(config_path, encoding="utf-8")
    return config


def get_ck_database():
    g_cfg = init_config()
    return g_cfg.get("raw_file", "database")


def get_ck_work_path():
    g_cfg = init_config()
    work_path = os.path.realpath(g_cfg.get("raw_file", "work_path"))
    return work_path


def get_ck_mv_path():
    work_path = get_ck_work_path()
    mv_path = os.path.join(work_path, 'mv')
    return mv_path


def get_ck_q_path():
    work_path = get_ck_work_path()
    q_path = os.path.join(work_path, 'q')
    return q_path


def get_ck_sqls_path():
    work_path = get_ck_work_path()
    sqls_path = os.path.join(work_path, 'q/sql')
    return sqls_path


def get_ck_schema_path():
    work_path = get_ck_work_path()
    schema_path = os.path.join(work_path, 'schema.sql')
    return schema_path


def check_ip_address(ck_host):
    try:
        socket.inet_aton(ck_host)
        return True
    except socket.error:
        return False


def get_ck_host():
    g_cfg = init_config()
    ck_host = g_cfg.get('clickhouse', 'ck_host')
    if check_ip_address(ck_host):
        return ck_host
    else:
        raise ValueError("ck_host is not a valid ip address")


def get_ck_port():
    g_cfg = init_config()
    return g_cfg.get('clickhouse', 'ck_port')


def get_ck_cluster_name():
    g_cfg = init_config()
    return g_cfg.get("clickhouse", "cluster_name")


def get_ck_user():
    user_name = input('please input your clickhouse user name:\n')
    return user_name


def get_ck_password():
    ck_password = getpass.getpass('please input your clickhouse password:\n')
    return ck_password


def get_ck_num_round():
    g_cfg = init_config()
    num_round = g_cfg.get('clickhouse', 'num_round')
    try:
        return int(num_round)
    except ValueError as e:
        raise ValueError("num_round should be int type") from e


def get_ck_enable_projection():
    g_cfg = init_config()
    enable_projection = g_cfg.get('clickhouse', 'enable_projection')
    if enable_projection.lower() == "false":
        return False
    else:
        return True


def get_ck_projection_wait_time():
    g_cfg = init_config()
    projection_wait_time = g_cfg.get('clickhouse', 'projection_wait_time')
    try:
        return int(projection_wait_time)
    except ValueError as e:
        raise ValueError("projection_wait_time should be int type") from e


def get_ck_projection_compress_ratio():
    g_cfg = init_config()
    projection_compress_ratio = g_cfg.get('clickhouse', 'projection_compress_ratio')
    try:
        return float(projection_compress_ratio)
    except ValueError as e:
        raise ValueError("projection_compress_ratio should be float type") from e


def get_ck_sql_running_time_threshold():
    g_cfg = init_config()
    sql_running_time_threshold = g_cfg.get('clickhouse', 'sql_running_time_threshold')
    try:
        return float(sql_running_time_threshold)
    except ValueError as e:
        raise ValueError("sql_running_time_threshold should be float type") from e


def get_ck_projection_limit_each_table():
    g_cfg = init_config()
    projection_limit_each_table = g_cfg.get('clickhouse', 'projection_limit_each_table')
    try:
        return int(projection_limit_each_table)
    except ValueError as e:
        raise ValueError("projection_limit_each_table should be int type") from e


def get_ck_is_distributed_table():
    g_cfg = init_config()
    is_distributed_table = g_cfg.get('clickhouse', 'is_distributed_table')
    if is_distributed_table.lower() == 'false':
        return False
    return True


def get_clickhouse_tables():
    g_cfg = init_config()
    tables = g_cfg.get('clickhouse', 'clickhouse_tables')
    clickhouse_tables = tables.split(",")
    return [table.strip() for table in clickhouse_tables]


def get_ck_query_log_table():
    g_cfg = init_config()
    query_log_table = g_cfg.get('clickhouse', 'query_log_table')
    return query_log_table


def get_ck_query_limit_each_table():
    g_cfg = init_config()
    query_limit_each_table = g_cfg.get('clickhouse', 'query_limit_each_table')
    try:
        return int(query_limit_each_table)
    except ValueError as e:
        raise ValueError("query_limit_each_table should be int type") from e


def get_ck_query_limit_mutil_table():
    g_cfg = init_config()
    query_limit_mutil_table = g_cfg.get('clickhouse', 'query_limit_mutil_table')
    try:
        return int(query_limit_mutil_table)
    except ValueError as e:
        raise ValueError("query_limit_mutil_table should be int type") from e


def get_ck_projection_with_conditional_functions():
    g_cfg = init_config()
    enable_conditional_func = g_cfg.get('clickhouse', 'projection_with_conditional_functions')
    if enable_conditional_func.lower() == "false":
        return False
    else:
        return True


def get_ck_allows_materialize_projection():
    g_cfg = init_config()
    allows_materialize_projection = g_cfg.get('clickhouse', 'allows_materialize_projection')
    if allows_materialize_projection.lower() == "false":
        return False
    else:
        return True


def get_ck_drop_projections():
    g_cfg = init_config()
    drop_projections = g_cfg.get('clickhouse', 'drop_projections')
    return drop_projections


def get_ck_candidate_recommend_strategy():
    g_cfg = init_config()
    return g_cfg.get("recommend", "candidate_strategy")


def get_ck_max_data_count():
    g_cfg = init_config()
    max_data_count = g_cfg.get("train", "max_data_count")
    try:
        return int(max_data_count)
    except ValueError as e:
        raise ValueError("max_data_count should be int type") from e


def get_ck_train_epochs():
    g_cfg = init_config()
    epochs = g_cfg.get("train", "epochs")
    try:
        return int(epochs)
    except ValueError as e:
        raise ValueError("epochs should be int type") from e


def get_ck_train_estimators():
    g_cfg = init_config()
    estimators = g_cfg.get("train", "estimators")
    try:
        return int(estimators)
    except ValueError as e:
        raise ValueError("estimators should be int type") from e


def get_ck_train_max_depth():
    g_cfg = init_config()
    max_depth = g_cfg.get("train", "max_depth")
    try:
        return int(max_depth)
    except ValueError as e:
        raise ValueError("max_depth should be int type") from e


def get_ck_train_learning_rate():
    g_cfg = init_config()
    learning_rate = g_cfg.get("train", "learning_rate")
    try:
        return float(learning_rate)
    except ValueError as e:
        raise ValueError("learning_rate should be float type") from e


def get_ck_model_path():
    work_path = get_ck_work_path()
    model_path = os.path.join(work_path, 'model')
    return model_path

def get_ck_omnimv_version():
    return f"\nProduct Name: Kunpeng BoostKit\nProduct Version: 23.0.RC2\nComponent Name: "\
           f"BoostKit-omnimv\nComponent Version: 1.1.0"
