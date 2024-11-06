#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import os
import stat

from omnimv_logging import get_logger
from src.candidate_generation.clickhouse.clickhouse_utils import try_explain_sql, extract_multi_table_query
from src.candidate_generation.clickhouse.materialize_view_generation.filter_view import ViewFilter
from src.candidate_generation.clickhouse.materialize_view_generation.normalize_view import ViewNormalizer
from src.candidate_generation.clickhouse.materialize_view_generation.union_view import ViewsUnion
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import normalize_sql
from src.candidate_generation.clickhouse.projection_candidate_generation.sub_query_generation import TargetQueryGetter
from src.preprocessor.proc_conf_ck import get_ck_database

OMNIMV_LOG = get_logger()


def get_materialize_view(sql_path, client):
    extract_multi_table_query(sql_path, client)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} does not exists")
    count = 1
    total_count = len(os.listdir(sql_path))
    normalized_views = []
    for file_name in os.listdir(sql_path):
        OMNIMV_LOG.info(f"Parse sql progress: {count}/{total_count}  {file_name}")
        count += 1
        sql_file_path = os.path.join(sql_path, file_name)
        with open(sql_file_path, 'r') as file:
            database = get_ck_database()
            sql = (normalize_sql(file.read())).replace(f'{database}.', '')
        sub_queries = TargetQueryGetter(sql).queries_for_mv()
        filter_views = []
        for sub_query in sub_queries:
            view_filter = ViewFilter(sub_query)
            view = view_filter.get_view()
            if view != '':
                filter_views.append(view)
        for view in filter_views:
            view_normalizer = ViewNormalizer(view, [file_name])
            normalized_view = view_normalizer.get_normalized_view()
            if try_explain_sql(client, str(normalized_view)):
                normalized_views.append(normalized_view)
            else:
                OMNIMV_LOG.info(f'{file_name} has a view can not execute,view is : {str(normalized_view)}')
                continue
    union_view_er = ViewsUnion(normalized_views)
    union_views = union_view_er.get_union_views()
    materialize_views = []
    for view in union_views:
        if try_explain_sql(client, str(view)):
            materialize_views.append(view)
        else:
            OMNIMV_LOG.info(f'an union error,involve sql: {view.sources},view is: {str(view)}')
            continue
    return materialize_views


def generate_materialize_view(sql_path, mv_path, client):
    mv_create_path = f"{mv_path}/materialize_view_create"
    mv_sql_path = f"{mv_path}/materialize_view_sql"
    os.makedirs(mv_create_path, mode=0o750, exist_ok=True)
    os.makedirs(mv_sql_path, mode=0o750, exist_ok=True)
    materialize_views = get_materialize_view(sql_path, client)
    mv_id = 0

    flags = os.O_WRONLY | os.O_CREAT
    modes = stat.S_IWUSR | stat.S_IRUSR

    for materialize_view in materialize_views:
        mv_sql = str(materialize_view)
        mv_create = f"create materialized view if not exists mv{mv_id} " \
                    f"engine = MergeTree() order by tuple() POPULATE as {mv_sql}"
        OMNIMV_LOG.info(f'mv{mv_id}:{mv_sql}')
        OMNIMV_LOG.info(f'mv{mv_id} matched sql: {materialize_view.sources}')
        with os.fdopen(os.open(mv_create_path + f'/mv{mv_id}.sql', flags, modes), 'w') as file:
            file.truncate()
            file.write(mv_create)
        with os.fdopen(os.open(mv_sql_path + f'/mv{mv_id}.sql', flags, modes), 'w') as file:
            file.truncate()
            file.write(mv_sql)
        mv_id += 1