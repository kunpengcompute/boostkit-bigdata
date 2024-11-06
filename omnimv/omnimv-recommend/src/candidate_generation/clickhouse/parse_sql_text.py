#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import os
import stat
from collections import defaultdict

from omnimv_logging import get_logger
from src.candidate_generation.clickhouse.generate_projections import merge_projection, could_merge
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import normalize_sql, \
    get_table_name
from src.candidate_generation.clickhouse.projection_candidate_generation.get_projection import ProjectionGetter
from src.preprocessor.proc_conf_ck import get_ck_database, get_ck_cluster_name, \
    get_ck_projection_limit_each_table

OMNIMV_LOG = get_logger()


def parse_merge_sqls_text(sql_path, table_name, is_incremental):
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} does not exists")
    projections_dict = defaultdict(dict)
    count = 1
    for file_name in os.listdir(sql_path):
        sql_table_name = get_table_name(file_name)
        if table_name.strip().upper() != sql_table_name.strip().upper():
            continue
        if is_incremental and "incremental" not in file_name:
            continue
        OMNIMV_LOG.info(f"Parse sql count: {count}: {file_name}")
        count += 1
        file_id = file_name.split(".")[0]
        sql_file_path = os.path.join(sql_path, file_name)
        with open(sql_file_path, 'r') as file:
            database = get_ck_database()
            sql = (normalize_sql(file.read())).replace(f'{database}.', '')
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
    return projections_dict


def get_projection_sql(projections_dict, table_name, cube_set, is_incremental):
    cube_dict = {}
    mv_sql_dict = {}
    cube_sort_base = []
    cube_sqls = defaultdict(list)
    mv_count = len(cube_set)
    matched_sql_count = 0
    cluster_name = get_ck_cluster_name()
    for sql_id in projections_dict:
        sql_projection = projections_dict.get(sql_id)
        if not sql_projection.get('removed'):
            select_sql = ", ".join(sql_projection.get('columns_set'))
            group_by_sql = ", ".join(sql_projection.get('group_by_set'))
            projection_table_name = sql_projection.get('table_name')
            select_sql = select_sql.replace(f"{projection_table_name}.", "")
            group_by_sql = group_by_sql.replace(f"{projection_table_name}.", "")
            is_distributed_table = sql_projection.get("is_distributed_table")

            cube_id = f"{table_name}_cube{mv_count}"
            if is_incremental:
                cube_id += "_incremental"

            cube_sql = f"(select {select_sql} group by {group_by_sql})"
            mv_sql = f"select {select_sql} from {projection_table_name} group by {group_by_sql};"
            if is_distributed_table:
                projection_sql = f"alter table {projection_table_name} on cluster {cluster_name} " \
                                 f"add projection if not exists {cube_id} {cube_sql};\n"
                materialize_sql = f"alter table {projection_table_name} on cluster {cluster_name} " \
                                  f"materialize projection {cube_id};"
            else:
                projection_sql = f"alter table {projection_table_name} add projection if not exists " \
                                 f"{cube_id} {cube_sql};\n"
                materialize_sql = f"alter table {projection_table_name} materialize projection {cube_id};"

            if cube_sqls.get(projection_table_name) and cube_sql in cube_sqls.get(projection_table_name):
                continue
            cube_sqls[projection_table_name].append(cube_sql)
            projection_sql += materialize_sql

            cube_dict[f"{cube_id}.sql"] = projection_sql
            mv_sql_dict[f"{cube_id}.sql"] = mv_sql
            cube_sort_base.append([f"{cube_id}.sql", len(sql_projection.get('sql_id')),
                                   len(sql_projection.get('group_by_set'))])
            mv_count += 1

            OMNIMV_LOG.info(f"cube: {cube_id}, matched sql id: {sql_projection.get('sql_id')}")

            matched_sql_count += len(list(set(sql_projection.get('sql_id'))))
    return cube_dict, mv_sql_dict, cube_sort_base


def ranking_msg(log_show, table_name):
    if log_show == f'{table_name} parse result ranking: \n':
        OMNIMV_LOG.info(f'{table_name} parse result ranking: none projection generate!')
    else:
        OMNIMV_LOG.info(log_show)


def parse_sql_text(sql_path, mv_path, table_name, is_incremental=False):
    mv_projection_path = f"{mv_path}/mv_create_projections"
    mv_sql_path = f"{mv_path}/mv_sql"

    cube_set = get_exists_cube(mv_projection_path, mv_sql_path, table_name, is_incremental)

    projections_dict = parse_merge_sqls_text(sql_path, table_name, is_incremental)
    cube_dict, mv_sql_dict, cube_sort_base = get_projection_sql(projections_dict, table_name, cube_set, is_incremental)
    cube_sorted = sorted(cube_sort_base, key=lambda item: (item[1], -item[2]), reverse=True)
    table_projections = get_limit_projections(table_name, cube_sorted)
    if not is_incremental:
        sort_id = 1
        log_show = f'{table_name} parse result ranking: \n'
        for cube_msg in cube_sorted:
            cube_msg_show = f"{sort_id}th: [cube id: {cube_msg[0]}   match sql amount: {cube_msg[1]}" \
                            f"   projection group by amount: {cube_msg[2]}]\n"
            log_show += cube_msg_show
            sort_id += 1
        ranking_msg(log_show, table_name)

        for cube_id, cube_sql in cube_dict.items():
            cube_path = f"{mv_projection_path}/{cube_id}"
            mv_path = f"{mv_sql_path}/{cube_id}"
            mv_sql = mv_sql_dict.get(cube_id)
            write_cube_msg(cube_path, mv_path, cube_sql, mv_sql)
    else:
        cube_id_incremental = []
        for cube_msg in cube_sorted:
            cube_id = cube_msg[0]
            cube_sql = cube_dict.get(cube_id)
            if cube_sql not in cube_set:
                cube_id_incremental.append(cube_id)

        log_show = f'{table_name} parse result ranking: \n'

        count = 0
        for cube_id in cube_id_incremental:
            cube_msg_show = f"{count + 1}th: [cube id: {cube_id} " \
                            f"match incremental sql amount: {cube_sorted[count][1]}   " \
                            f"projection group by amount: {cube_sorted[count][2]}]\n"
            log_show += cube_msg_show

            cube_incremental_path = f"{mv_projection_path}/{cube_id}"
            cube_sql = cube_dict.get(cube_id)
            mv_incremental_path = f"{mv_sql_path}/{cube_id}"
            mv_sql = mv_sql_dict.get(cube_id)
            write_cube_msg(cube_incremental_path, mv_incremental_path, cube_sql, mv_sql)
            count += 1
        ranking_msg(log_show, table_name)
    return table_projections


def write_cube_msg(cube_path, mv_path, cube_sql, mv_sql):
    flags = os.O_WRONLY | os.O_CREAT
    modes = stat.S_IWUSR | stat.S_IRUSR
    with os.fdopen(os.open(cube_path, flags, modes), 'w') as file:
        file.truncate()
        file.write(cube_sql)
    with os.fdopen(os.open(mv_path, flags, modes), 'w') as file:
        file.truncate()
        file.write(mv_sql)


def get_exists_cube(mv_projection_path, mv_sql_path, table_name, is_incremental):
    cube_set = set()
    if os.path.exists(mv_projection_path) and is_incremental:
        for file_name in os.listdir(mv_projection_path):
            sql_table_name = get_table_name(file_name)
            if table_name.strip().upper() != sql_table_name.strip().upper():
                continue

            cube_path = os.path.join(mv_projection_path, file_name)
            with open(cube_path, 'r') as file:
                cube_set.add(file.read())
    else:
        os.makedirs(mv_projection_path, mode=0o750, exist_ok=True)
        os.makedirs(mv_sql_path, mode=0o750, exist_ok=True)
    return cube_set


def get_limit_projections(table_name, cube_sorted):
    projection_limit = get_ck_projection_limit_each_table()
    cube_amount = min(projection_limit, len(cube_sorted))
    table_projections = {table_name: []}
    for i in range(cube_amount):
        projections = table_projections.get(table_name)
        projections.append(cube_sorted[i][0])
    return table_projections