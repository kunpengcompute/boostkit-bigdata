#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import os
import time
from collections import defaultdict

from clickhouse_driver.errors import ServerException

from omnimv_logging import get_logger
from src.candidate_generation.clickhouse.clickhouse_utils import get_query_log, get_compress_ratio
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import normalize_sql, \
    get_table_name
from src.candidate_generation.clickhouse.projection_candidate_generation.get_projection import ProjectionGetter
from src.preprocessor.proc_conf_ck import get_ck_database, get_ck_cluster_name
from src.recommend_model.clickhouse.recommend_ck import candidate_recommend

OMNIMV_LOG = get_logger()


def merge_projection(projection_1, projection_2):
    if projection_1['sql_id'] != projection_2['sql_id']:  # 防止两个projection来自同一条sql，也就是防止两个projection相等
        projection_2['removed'] = True
        projection_1['sql_id'].extend(projection_2['sql_id'])
        projection_1['sql_id'] = list(set(projection_1['sql_id']))
        columns_set = set()
        for item in projection_1['columns_set']:
            columns_set.add(item.replace(' ', '').upper())
        for item in projection_2['columns_set']:
            if item.replace(' ', '').upper() not in columns_set:
                projection_1['columns_set'].append(item)


def could_merge(projections_dict, sql_id_1, sql_id_2):
    projection_1 = projections_dict.get(sql_id_1)
    projection_2 = projections_dict.get(sql_id_2)

    table_name_1 = projection_1.get('table_name')
    group_by_items_1 = projection_1.get('group_by_set')

    table_name_2 = projection_2.get('table_name')
    group_by_items_2 = projection_2.get('group_by_set')
    if table_name_1 != table_name_2:
        return False
    else:
        group_by_set_1 = set()
        for item_exp in group_by_items_1:
            group_by_set_1.add(item_exp.replace(' ', '').upper())
        group_by_set_2 = set()
        for item_exp in group_by_items_2:
            group_by_set_2.add(item_exp.replace(' ', '').upper())
        if group_by_set_1.issuperset(group_by_set_2):
            return True
        else:
            return False


def parse_merge_sqls(sql_path, table_name):
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} does not exists")
    projections_dict = defaultdict(dict)
    sqls_dict = {}
    for file_name in os.listdir(sql_path):
        sql_table_name = get_table_name(file_name)
        if table_name.strip().upper() != sql_table_name.strip().upper():
            continue
        file_id = file_name.split(".")[0]
        sql_file_path = os.path.join(sql_path, file_name)

        with open(sql_file_path, 'r') as file:
            database = get_ck_database()
            sql = (normalize_sql(file.read())).replace(f'{database}.', '')
            sqls_dict[file_id] = sql
            projection_getter = ProjectionGetter(sql, file_name, table_name)
            projection = projection_getter.get_projection()
            if projection is None:
                continue
            group_items = projection.group_items
            select_items = projection.select_items
            projections_dict[file_id]['table_name'] = projection.table
            projections_dict[file_id]['group_by_set'] = group_items
            projections_dict[file_id]['columns_set'] = select_items
            projections_dict[file_id]['removed'] = False
            projections_dict[file_id]['sql_id'] = [file_id]
            projections_dict[file_id]['is_distributed_table'] = projection.is_distributed_table
    sql_id_list = list(projections_dict.keys())

    is_merged = []
    for i, _ in enumerate(sql_id_list):
        if i in is_merged:
            continue
        for j, _ in enumerate(sql_id_list):
            if j in is_merged or j == i:
                continue
            sql_id_i = sql_id_list[i]
            sql_id_j = sql_id_list[j]
            if could_merge(projections_dict, sql_id_i, sql_id_j):
                projection1 = projections_dict.get(sql_id_list[i])
                projection2 = projections_dict.get(sql_id_list[j])
                merge_projection(projection1, projection2)
                is_merged.append(j)
    return projections_dict, sqls_dict


def get_projections(projections_dict, table_name):
    cube_sqls = defaultdict(list)
    mv_count = 0
    projection_list = []
    cluster_name = get_ck_cluster_name()
    for sql_id in projections_dict:
        cube_dict = {}
        sql_projection = projections_dict.get(sql_id)
        if not sql_projection.get('removed'):
            select_sql = ", ".join(sql_projection.get('columns_set'))
            group_by_sql = ", ".join(sql_projection.get('group_by_set'))
            projection_table_name = sql_projection.get('table_name')
            select_sql = select_sql.replace(f"{projection_table_name}.", "")
            group_by_sql = group_by_sql.replace(f"{projection_table_name}.", "")
            is_distributed_table = sql_projection.get("is_distributed_table")
            sql_ids = sql_projection.get("sql_id")

            cube_id = f"{table_name}_cube{mv_count}"
            cube_sql = f"(select {select_sql} group by {group_by_sql})"
            if cube_sqls.get(projection_table_name) and cube_sql in cube_sqls.get(projection_table_name):
                continue
            cube_sqls[projection_table_name].append(cube_sql)
            if is_distributed_table:
                materialize_sql = f"alter table {projection_table_name} on cluster {cluster_name} " \
                                  f"materialize projection {cube_id};"
                projection_sql = f"alter table {projection_table_name} on cluster {cluster_name} " \
                                 f"add projection if not exists {cube_id} {cube_sql};\n"
            else:
                materialize_sql = f"alter table {projection_table_name} " \
                                  f"materialize projection {cube_id};"
                projection_sql = f"alter table {projection_table_name} " \
                                 f"add projection if not exists {cube_id} {cube_sql};\n"
            projection_sql += materialize_sql
            mv_count_sql = f"select count(*) from (select 1 from {table_name} group by {group_by_sql});"
            mv_sql = f"select {select_sql} from {projection_table_name} group by {group_by_sql};"

            cube_dict['cube_id'] = cube_id
            cube_dict['sql_ids'] = sql_ids
            cube_dict['projection_sql'] = projection_sql
            cube_dict['mv_count_sql'] = mv_count_sql
            cube_dict['mv_sql'] = mv_sql
            cube_dict['table_name'] = projection_table_name
            projection_list.append(cube_dict)
            mv_count += 1

    return projection_list


def calculate_cost(projection_list, client, original_table_name):
    cube_cost_dict = defaultdict(dict)
    for projection in projection_list:
        cube_id = projection.get('cube_id')
        sql_ids = projection.get('sql_ids')
        mv_count_sql = projection.get('mv_count_sql')
        table_name = projection.get('table_name')
        projection_sql = projection.get('projection_sql')
        mv_sql = projection.get('mv_sql')

        try:
            now = client.execute('select now()')[0][0]
            mv_line_count = int(client.execute(mv_count_sql)[0][0])
            time.sleep(1)
        except (ServerException, IndexError) as error:
            OMNIMV_LOG.error(error)
            continue
        mv_cost_query = f"select query, memory_usage, query_duration_ms " \
                        f"from system.query_log where query_start_time >= '{now}' " \
                        f"order by query_start_time desc"
        query_log = get_query_log(client, mv_cost_query, mv_count_sql)
        if query_log:
            OMNIMV_LOG.info(f"cube: {cube_id}, mv_lines: {mv_line_count}, "
                               f"memory_usage: {query_log[1]}, query_duration_ms: {query_log[2]}")
        else:
            OMNIMV_LOG.info(f"cube: {cube_id}, mv_lines: {mv_line_count}, "
                               f"memory_usage: Not found, query_duration_ms: Not found")
            continue

        projection_compress_ratio, satisfy_limit = get_compress_ratio(client, mv_line_count, original_table_name)
        if not satisfy_limit:
            continue
        cube_cost_dict[f"{cube_id}.sql"]['mv_lines'] = mv_line_count
        cube_cost_dict[f"{cube_id}.sql"]['memory_usage'] = query_log[1]
        cube_cost_dict[f"{cube_id}.sql"]['query_duration_ms'] = query_log[2]
        cube_cost_dict[f"{cube_id}.sql"]['projection_compress_ratio'] = projection_compress_ratio
        cube_cost_dict[f"{cube_id}.sql"]['matched_sql'] = len(sql_ids)
        cube_cost_dict[f"{cube_id}.sql"]['table_name'] = table_name
        cube_cost_dict[f"{cube_id}.sql"]['projection_sql'] = projection_sql
        cube_cost_dict[f"{cube_id}.sql"]['mv_sql'] = mv_sql

        OMNIMV_LOG.info(f"cube: {cube_id}, matched sql id: {sql_ids}, "
                           f"projection ratio: {round(projection_compress_ratio * 100, 5)}%")

    return cube_cost_dict


def generate_projections_with_merge(sql_path, mv_path, client, table_name):
    mv_projection_path = f"{mv_path}/mv_create_projections"
    mv_sql_path = f"{mv_path}/mv_sql"
    os.makedirs(mv_projection_path, mode=0o750, exist_ok=True)
    os.makedirs(mv_sql_path, mode=0o750, exist_ok=True)
    recommend_order = None

    projections_dict, sqls_dict = parse_merge_sqls(sql_path, table_name)
    if len(projections_dict) < 1:
        OMNIMV_LOG.info(f"No projection is recommended for table {table_name}")
        return recommend_order

    projection_list = get_projections(projections_dict, table_name)

    cube_cost_dict = calculate_cost(projection_list, client, table_name)

    recommend_order = candidate_recommend(cube_cost_dict, mv_projection_path, mv_sql_path, table_name)
    return recommend_order