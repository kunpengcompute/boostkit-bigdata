#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

try:
    import pydoop.hdfs as hdfs
except ModuleNotFoundError as _:
    pass

from src.candidate_generation.common.materialize_view_generation.normalize_view import ViewNormalizer
from src.candidate_generation.common.materialize_view_generation.union_view import ViewsUnion
from src.candidate_generation.common.projection_candidate_generation.generation_utils import \
    remove_special_characters
from src.candidate_generation.common.projection_candidate_generation.sub_query_generation import TargetQueryGetter
from src.candidate_generation.spark.filter_view_sp import SparkViewFilter
from src.candidate_generation.spark.spark_utils import get_text_from_hdfs, try_execute_sql, get_logger, \
    write_text_to_hdfs

OMNIMV_LOG = get_logger()


def generate_mv_spark(sql_path, mv_path, session):
    materialize_views = get_mv_spark(sql_path, session)
    mv_id = 0
    for view in materialize_views:
        create_mv_sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS mv{mv_id} AS {str(view)}"
        write_text_to_hdfs(str(view), f"{mv_path}/mv_sql/mv{mv_id}.sql")
        write_text_to_hdfs(create_mv_sql, f"{mv_path}/mv_create_sql/mv{mv_id}.sql")
        mv_id += 1
    return materialize_views


def get_mv_spark(sql_s_path, session):
    normalized_views = []
    fs = hdfs.hdfs('default', 0)
    for sql_file_name in fs.list_directory(sql_s_path):
        sql_path = sql_file_name.get('name')
        sql_exp = get_text_from_hdfs(sql_path)
        sql_exp = remove_special_characters(sql_exp)
        sub_queries = TargetQueryGetter(sql_exp).queries_for_mv()
        if len(sub_queries) == 0:
            OMNIMV_LOG.info(
                f"{sql_path.split('/')[-1]} has none sub query : {sql_exp}")
        filter_views = []
        for sub_query in sub_queries:
            view_filter = SparkViewFilter(sub_query)
            view = view_filter.get_view()
            if view != '':
                filter_views.append(view)
            elif view != '':
                OMNIMV_LOG.info(
                    f"{sql_path.split('/')[-1]} has a sub query contain extend dependence which is : {view}")
        for view in filter_views:
            view_normalizer = ViewNormalizer(view, [sql_path.split('/')[-1]])
            normalized_view = view_normalizer.get_normalized_view()
            if try_execute_sql(session, str(normalized_view)):
                normalized_views.append(normalized_view)
            else:
                OMNIMV_LOG.info(
                    f"{sql_path.split('/')[-1]} has a view can not execute,view is : {str(normalized_view)}")
                continue
    union_view_er = ViewsUnion(normalized_views)
    union_views = union_view_er.get_union_views()
    materialize_views = []
    for view in union_views:
        if try_execute_sql(session, str(view)):
            materialize_views.append(view)
        else:
            OMNIMV_LOG.info(f'an union error,involve sql: {view.sources},view is: {str(view)}')
            continue
    return materialize_views


def get_tables(materialize_view):
    tables = []
    for table_mgs in materialize_view.tables_msg:
        tables.append(table_mgs.table_name)
    tables = sorted(tables)
    return ','.join(tables)