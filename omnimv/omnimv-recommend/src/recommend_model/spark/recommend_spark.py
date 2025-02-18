#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import copy
from collections import defaultdict, Counter

import pandas as pd

try:
    import pydoop.hdfs as hdfs
except ModuleNotFoundError as _:
    pass

from omnimv_logging import get_logger
from src.candidate_generation.spark.spark_utils import get_objects_from_hdfs, get_text_from_hdfs, write_text_to_hdfs, \
    get_rewrite_sqls, get_mock_database_session
from src.preprocessor.proc_conf_spark import get_mv_output_path, get_table_bytes_path, get_spark_model_path, \
    get_spark_schema_path, get_spark_sqls_path, get_large_table_threshold, get_mv_limit, get_spark_mock_database, \
    get_mv_metadata_path

OMNIMV_LOG = get_logger()


def get_large_tables():
    table_bytes_path = get_table_bytes_path()
    table_bytes = get_objects_from_hdfs(table_bytes_path)
    large_threshold = get_large_table_threshold()
    large_table = []
    for table, capacity in table_bytes.items():
        if int(capacity) >= large_threshold:
            large_table.append(table)
    return large_table


def get_selected_mv(rewrite_sqls, recommend_mv_list):
    mv_sql_mappings = defaultdict(list)
    for sql_id, mvs in rewrite_sqls.items():
        for mv_id in mvs:
            mv_sql_mappings[mv_id].append(sql_id)
    covered_sql = set()
    select_mv = []
    for recommend_mv in recommend_mv_list:
        original_cov = len(covered_sql)
        cur_covered_sql = copy.deepcopy(covered_sql)
        for sql_id in mv_sql_mappings.get(recommend_mv):
            cur_covered_sql.add(sql_id)
        current_cov = len(cur_covered_sql)
        if (current_cov - original_cov) >= 2:
            covered_sql = copy.deepcopy(cur_covered_sql)
            select_mv.append(recommend_mv)
            OMNIMV_LOG.info(f"mv count: {len(select_mv)}, covered sql count: {len(covered_sql)}")
            OMNIMV_LOG.info(f"selected mv: {select_mv}")
            OMNIMV_LOG.info(f"covered sql: {sorted(covered_sql)}")

    return mv_sql_mappings, covered_sql, select_mv


def spark_greedy_recommend():
    mv_output_path = get_mv_output_path()
    candidate_mv_path = f"{mv_output_path}/mv_create_sql"
    mv_sql_path = f"{mv_output_path}/mv_create_sql"
    mock_database = get_spark_mock_database()
    schema_path = get_spark_schema_path()
    sqls_path = get_spark_sqls_path()
    mv_limit = get_mv_limit()
    rewrite_sqls, mv_list = get_rewrite_sqls(mock_database, schema_path, mv_sql_path, sqls_path)
    OMNIMV_LOG.info(f"rewrite sql count {len(rewrite_sqls)}")
    OMNIMV_LOG.info(f"rewrite sqls: {rewrite_sqls}")

    mv_frequency = Counter(mv_list)
    mvs = sorted(mv_frequency.items(), key=lambda x: -x[1])
    recommend_mv_list = [x[0] for x in mvs]

    mv_sql_mappings, covered_sql, select_mv = get_selected_mv(rewrite_sqls, recommend_mv_list)
    large_tables = get_large_tables()
    for mv_id in recommend_mv_list:
        sql = get_text_from_hdfs(f"{mv_sql_path}/{mv_id}.sql")
        large_table_count = 0
        for table in large_tables:
            if f" {table.lower()} " in sql.lower():
                large_table_count += 1
        if large_table_count >= 2 and mv_id not in select_mv:
            select_mv.append(mv_id)
            for sql_id in mv_sql_mappings.get(mv_id):
                covered_sql.add(sql_id)
    OMNIMV_LOG.info(f"mv count: {len(select_mv)}, covered sql count: {len(covered_sql)}")
    OMNIMV_LOG.info(f"covered sql: {sorted(covered_sql)}")
    OMNIMV_LOG.info(f"greedy recommend for candidate mv: {select_mv}")
    final_covered_sql = set()
    for mv_id in select_mv[:mv_limit]:
        for sql_id in mv_sql_mappings.get(mv_id):
            final_covered_sql.add(sql_id.split("/")[-1])
    OMNIMV_LOG.info(f"final selected mv: {select_mv[:mv_limit]}; covered sql: [{','.join(sorted(list(final_covered_sql)))}]")

    for mv_id in select_mv[:mv_limit]:
        sql = get_text_from_hdfs(f"{candidate_mv_path}/{mv_id}.sql")
        write_text_to_hdfs(sql, f"{mv_output_path}/greedy_recommend_mv/{mv_id}.sql")
    write_text_to_hdfs('\n'.join(sorted(list(final_covered_sql))),f"{get_mv_metadata_path()}/greedy_recommend_mv_covered_sql/rewrite_sqls")