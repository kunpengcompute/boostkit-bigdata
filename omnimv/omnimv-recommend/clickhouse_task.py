#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import json
import os
import stat
from collections import defaultdict

from clickhouse_driver.errors import ServerException, LogicalError

from omnimv_logging import get_logger
from src.candidate_generation.clickhouse.clickhouse_utils import init_clickhouse_schema, drop_projections, \
    get_rewrite_result, create_projections, measure_performance, extract_sqls, remove_file, get_ck_client
from src.candidate_generation.clickhouse.generate_materialize_view import generate_materialize_view
from src.candidate_generation.clickhouse.generate_projections import generate_projections_with_merge
from src.candidate_generation.clickhouse.parse_sql_text import parse_sql_text
from src.preprocessor.proc_conf_ck import get_ck_enable_projection, get_ck_num_round, get_ck_schema_path, \
    get_ck_sqls_path, get_ck_mv_path, get_ck_omnimv_version, get_ck_database, \
    get_clickhouse_tables, get_ck_projection_limit_each_table, get_ck_q_path, get_ck_model_path, get_ck_work_path

OMNIMV_LOG = get_logger()


class CkDropProjectionTask(object):
    def __init__(self, _client, _database):
        self.client = _client
        self.database = _database

    def run(self):
        drop_projections(self.client, self.database)
        OMNIMV_LOG.info("Drop Projections succeed!")


class CkInitDatabaseTask(object):
    def __init__(self, _client, _database):
        self.client = _client
        self.database = _database

    @staticmethod
    def init_folder():
        work_path = get_ck_work_path()
        q_path = get_ck_q_path()
        mv_path = get_ck_mv_path()
        model_path = get_ck_model_path()
        os.makedirs(work_path, mode=0o750, exist_ok=True)
        os.makedirs(q_path, mode=0o750, exist_ok=True)
        os.makedirs(mv_path, mode=0o750, exist_ok=True)
        os.makedirs(model_path, mode=0o750, exist_ok=True)

    def run(self):
        self.init_folder()
        output_schema_path = get_ck_schema_path()
        init_clickhouse_schema(self.client, self.database, output_schema_path)
        OMNIMV_LOG.info("Init database succeed!")


class CkGenerateViewsTask(object):
    def __init__(self, _client, _schema_tables):
        self.client = _client
        self.schema_tables = _schema_tables

    def run(self):
        self.client.execute("set allow_experimental_projection_optimization = 0")
        q_path = get_ck_sqls_path()
        mv_path = get_ck_mv_path()
        recommend_cubes = defaultdict(list)
        # remove remaining file under directory "mv_create_projections" and "mv_sql"
        remove_file(f"{mv_path}/mv_create_projections")
        remove_file(f"{mv_path}/mv_sql")
        projection_limit = get_ck_projection_limit_each_table()
        for table_name in self.schema_tables:
            recommend_order = generate_projections_with_merge(q_path, mv_path, self.client, table_name)
            if recommend_order:
                OMNIMV_LOG.info(f"Greedy recommend cubes for table {table_name}: "
                                   f"{recommend_order[:projection_limit]}")
                recommend_cubes[table_name] = recommend_order[:projection_limit]

        if len(recommend_cubes) > 0:
            flags = os.O_WRONLY | os.O_CREAT
            modes = stat.S_IWUSR | stat.S_IRUSR
            with os.fdopen(os.open(f"{mv_path}/greedy_recommend.txt", flags, modes), 'w') as file:
                file.truncate()
                file.write(json.dumps(recommend_cubes))
            OMNIMV_LOG.info("Generate projections succeed!")
        else:
            OMNIMV_LOG.info("Generate projections failed!")


class CkCreateProjectionsTask(object):
    def __init__(self, _client):
        self.client = _client

    def run(self):
        projection_path = get_ck_mv_path()
        create_projections(self.client, projection_path)
        OMNIMV_LOG.info("Create Projections succeed!")


class CkParseQueryMvTask(object):
    def __init__(self, _client):
        self.client = _client

    def run(self):
        self.client.execute("set allow_experimental_projection_optimization = 1")
        src_directory = get_ck_sqls_path()
        get_rewrite_result(self.client, src_directory)
        OMNIMV_LOG.info("Parse query-projection succeed!")

class CkMeasurePerformanceTask(object):
    def __init__(self, _client, _database):
        self.client = _client
        self.database = _database

    def run(self):
        sqls_path = get_ck_sqls_path()
        enable_projection = get_ck_enable_projection()
        num_round = get_ck_num_round()
        if enable_projection:
            OMNIMV_LOG.info("Execute query: set allow_experimental_projection_optimization = 1")
            self.client.execute("set allow_experimental_projection_optimization = 1")
        else:
            OMNIMV_LOG.info("Execute query: set allow_experimental_projection_optimization = 0")
            self.client.execute("set allow_experimental_projection_optimization = 0")
        OMNIMV_LOG.info(f"SQL path: {sqls_path}")
        sql_running_time, sql_running_avg_time, total_time = measure_performance(self.client, sqls_path, int(num_round))
        OMNIMV_LOG.info(f"Database: {self.database}")
        OMNIMV_LOG.info(f"Total Elapsed Time: {total_time}")
        OMNIMV_LOG.info(f"SQL Running Time: {sql_running_time}")
        OMNIMV_LOG.info(f"SQL Average Running Time: {sql_running_avg_time}")


class CkParseSqlTextTask(object):
    def __init__(self, _schema_tables, _is_incremental):
        self.schema_tables = _schema_tables
        self.is_incremental = _is_incremental

    def run(self):
        sql_path = get_ck_sqls_path()
        mv_path = get_ck_mv_path()
        all_projections = {}
        for table_name in self.schema_tables:
            table_projections = parse_sql_text(sql_path, mv_path, table_name, self.is_incremental)
            all_projections[table_name] = table_projections.get(table_name)
            OMNIMV_LOG.info(f"parse cubes for table {table_name}: {table_projections.get(table_name)}")

        flags = os.O_WRONLY | os.O_CREAT
        modes = stat.S_IWUSR | stat.S_IRUSR
        with os.fdopen(os.open(f"{mv_path}/greedy_recommend.txt", flags, modes), 'w') as file:
            file.truncate()
            file.write(json.dumps(all_projections))
        OMNIMV_LOG.info("Parse SQL text succeed!")


class CkExtractSqlTask(object):
    def __init__(self, _client, _schema_tables, _is_incremental):
        self.client = _client
        self.schema_tables = _schema_tables
        self.is_incremental = _is_incremental

    def run(self):
        for table_name in self.schema_tables:
            extract_sqls(self.client, table_name, self.is_incremental)


class CkGenerateMaterializeViewTask(object):
    def __init__(self, _client):
        self.client = _client

    def run(self):
        sql_path = get_ck_sqls_path()
        mv_path = get_ck_mv_path()
        generate_materialize_view(sql_path, mv_path, self.client)
        OMNIMV_LOG.info("Generate materialize view succeed!")


def run_clickhouse_task(clickhouse_task):
    if clickhouse_task == '--version':
        OMNIMV_LOG.info(get_ck_omnimv_version())
        return
    database = get_ck_database()
    try:
        client = get_ck_client()
    except LogicalError as _:
        OMNIMV_LOG.info('Login error.\n')
        return

    clickhouse_tables = get_clickhouse_tables()
    use_database = f"use {database}"
    try:
        client.execute(use_database)
        tables = client.execute("show tables;")
    except ServerException as error:
        OMNIMV_LOG.error(error)
        return
    if "all" in clickhouse_tables:
        schema_tables = [table[0] for table in tables]
    else:
        schema_tables = [table[0] for table in tables if table[0] in clickhouse_tables]
    clickhouse_tasks = {
        "drop_projections": CkDropProjectionTask(client, database),
        "init_database": CkInitDatabaseTask(client, database),
        "generate_views": CkGenerateViewsTask(client, schema_tables),
        "create_projections": CkCreateProjectionsTask(client),
        "create_projections_model": CkCreateProjectionsTask(client),
        "parse_query_mv": CkParseQueryMvTask(client),
        "measure_performance": CkMeasurePerformanceTask(client, database),
        "parse_sql_text": CkParseSqlTextTask(schema_tables, False),
        "extract_sqls": CkExtractSqlTask(client, schema_tables, False),
        "extract_sqls_incremental": CkExtractSqlTask(client, schema_tables, True),
        "parse_sql_text_incremental": CkParseSqlTextTask(schema_tables, True),
        "generate_materialize_view": CkGenerateMaterializeViewTask(client)
    }
    if clickhouse_task not in clickhouse_tasks.keys():
        raise Exception(f"task {clickhouse_task} is not supported, only following tasks are "
                        f"supported: {clickhouse_tasks.keys()}")
    else:
        ck_task = clickhouse_tasks.get(clickhouse_task)
        ck_task.run()