#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import pydoop.hdfs as hdfs

from src.candidate_generation.spark.generate_mv_spark import generate_mv_spark
from src.preprocessor.proc_conf_spark import get_query_log_timestamp, get_mv_log_timestamp, \
    get_mv_output_path, get_omnimv_version, get_spark_schema_path, \
    get_spark_database, get_spark_sqls_path, get_table_bytes_path, get_spark_mock_database
from omnimv_logging import get_logger
from src.candidate_generation.spark.spark_utils import write_text_to_hdfs, \
    run_sqls_on_spark, get_rewrite_sqls, create_mv_tables, \
    generate_json_logs_interval, get_mock_database_session, write_objects_to_hdfs, get_mv_bytes, init_pyspark_plugin
from src.recommend_model.spark.recommend_spark import spark_greedy_recommend

OMNIMV_LOG = get_logger()


def init_database():
    database = get_spark_database()
    session = init_pyspark_plugin()
    use_database = f"use {database}"
    session.sql(use_database)
    df_all_tables = session.sql("show tables").toPandas()
    schema_sql = ''
    table_bytes = {}
    for table in df_all_tables['tableName'].values:
        mv_table_bytes = get_mv_bytes(session, table)
        table_bytes[table.upper()] = mv_table_bytes
        show_create = f"show create table {table}"
        query = session.sql(show_create).collect()[0].createtab_stmt
        schema_sql += f"{query[:-1]};\n\n"

    session.stop()
    table_bytes_path = get_table_bytes_path()
    write_objects_to_hdfs(table_bytes, table_bytes_path)
    schema_path = get_spark_schema_path()
    write_text_to_hdfs(schema_sql, schema_path)
    OMNIMV_LOG.info("init_database succeed!")


def run_original_sqls():
    sql_hdfs_path = get_spark_sqls_path()
    hdfs_fs = hdfs.hdfs("default", 0)
    query_pathes = []
    if not hdfs_fs.exists(sql_hdfs_path):
        OMNIMV_LOG.info(f"{sql_hdfs_path} not exist")
        return
    for sql_path in hdfs_fs.list_directory(sql_hdfs_path):
        query_pathes.append(sql_path.get('name'))
    run_sqls_on_spark(query_pathes, enable_omnimv='false')
    OMNIMV_LOG.info("run_original_sqls succeed!")


def parse_query_logs():
    start_time, end_time = get_query_log_timestamp()
    file_name = "query_logs"
    generate_json_logs_interval(file_name, start_time, end_time)
    OMNIMV_LOG.info("parse_query_logs succeed!")


def mock_rewrite_result(mv_path):
    mock_database = get_spark_mock_database()
    schema_path = get_spark_schema_path()
    sqls_path = get_spark_sqls_path()
    rewrite_sqls, mv_list = get_rewrite_sqls(mock_database, schema_path, mv_path, sqls_path)
    return rewrite_sqls, mv_list


def create_greedy_views():
    greedy_mv_path = f"{get_mv_output_path()}/greedy_recommend_mv"
    create_mv_tables(greedy_mv_path, enable_omnimv='true')
    OMNIMV_LOG.info("create_greedy_views succeed!")


def parse_mv_logs():
    start_time, end_time = get_mv_log_timestamp()
    file_name = "mv_logs"
    generate_json_logs_interval(file_name, start_time, end_time)
    OMNIMV_LOG.info("parse_mv_logs succeed!")

def run_spark_task(task):
    tasks = [
        "init_database", "run_original_sqls", "parse_query_logs", "generate_views", "create_greedy_views",
        "parse_mv_logs", "--version"
    ]
    if task not in tasks:
        raise Exception(f"task {task} is not supported, only following tasks are supported: {tasks}")
    if task == "init_database":
        init_database()
    elif task == "run_original_sqls":
        run_original_sqls()
    elif task == "parse_query_logs":
        parse_query_logs()
    elif task == "generate_views":
        sql_path = get_spark_sqls_path()
        schema_path = get_spark_schema_path()
        mv_path = get_mv_output_path()
        session = get_mock_database_session(schema_path)
        generate_mv_spark(sql_path, mv_path, session)
        spark_greedy_recommend()
    elif task == "create_greedy_views":
        create_greedy_views()
    elif task == "parse_mv_logs":
        parse_mv_logs()
    elif task == "--version":
        OMNIMV_LOG.info(get_omnimv_version())
    else:
        pass