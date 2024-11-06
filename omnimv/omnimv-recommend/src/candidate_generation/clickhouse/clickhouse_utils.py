#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import json
import os
import re
import stat
import time
from collections import defaultdict

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException, SocketTimeoutError, LogicalError

from omnimv_logging import get_logger
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import normalize_sql, \
    get_table_drop_projections, remove_special_characters, get_table_name
from src.preprocessor.proc_conf_ck import get_ck_projection_wait_time, get_clickhouse_tables, \
    get_ck_projection_compress_ratio, get_ck_database, get_ck_sqls_path, get_ck_user, get_ck_password, get_ck_host, \
    get_ck_port, get_ck_allows_materialize_projection, get_ck_query_log_table, \
    get_ck_query_limit_each_table, get_ck_sql_running_time_threshold, get_ck_cluster_name, \
    get_ck_query_limit_mutil_table, get_ck_is_distributed_table

OMNIMV_LOG = get_logger()


def get_query_log(client, query_log_sql, target_query, target_query_index=0):
    try:
        for _ in range(10):
            results = client.execute(query_log_sql)
            target_query_result = search_target_msg(results, target_query, target_query_index)
            if target_query_result:
                return target_query_result
            else:
                time.sleep(1)
    except (ServerException, ValueError) as error:
        OMNIMV_LOG.error(error)
    return []


def search_target_msg(results, query_exp, query_index):
    for result in results:
        if result[query_index].replace(' ', '').upper() != query_exp.replace(' ', '').upper():
            continue
        values = []
        for i, value in enumerate(result):
            if i != query_index:
                values.append(int(value))
        if all(value > 0 for value in values):
            return result
    return []


def get_rewrite_result(client, src_directory):
    query_rewrite_mappings = {}

    for sql_file in os.listdir(src_directory):
        sql_file_path = f"{src_directory}/{sql_file}"
        with open(sql_file_path, 'r') as file:
            query = normalize_sql(file.read())
        json_query = f"explain {query}"
        try:
            json_query_result = str(client.execute(json_query))
            pattern = r'(projection \w+cube\d+)'
            result = re.findall(pattern, json_query_result)
            if result:
                query_rewrite_mappings[sql_file] = result[0].split(' ')[-1]
            else:
                continue
        except ServerException as error:
            OMNIMV_LOG.error(error)
            continue
    OMNIMV_LOG.info(f"Query rewrite mappings: {query_rewrite_mappings}")


def init_clickhouse_schema(client, database, output_schema_path):
    use_database = f"use {database}"
    clickhouse_tables = get_clickhouse_tables()
    try:
        client.execute(use_database)
        tables = client.execute("show tables;")
        if "all" in clickhouse_tables:
            schema_tables = [table[0] for table in tables]
        else:
            schema_tables = [table[0] for table in tables if table[0] in clickhouse_tables]
    except ServerException as error:
        OMNIMV_LOG.error(error)
        return
    schema_sql = ''
    for table in schema_tables:
        try:
            show_table_res = client.execute(f"show create table {table}")
            schema_sql += f"{show_table_res[0][0]};\n\n"
        except ServerException as error:
            OMNIMV_LOG.error(error)
            continue

    flags = os.O_WRONLY | os.O_CREAT
    modes = stat.S_IWUSR | stat.S_IRUSR
    with os.fdopen(os.open(output_schema_path, flags, modes), 'w') as file:
        file.truncate()
        file.write(schema_sql)


def to_drop_projection(client, database, table, drop_sql, drop_cmd):
    projection_wait_time = int(get_ck_projection_wait_time())
    time_count = 0
    is_drop_done = False
    try:
        # execute drop projection command
        client.execute(drop_sql)
    except ServerException as error:
        OMNIMV_LOG.error(error)
    while time_count < projection_wait_time:
        try:
            sql_mutations = f"select is_done from system.mutations where database='{database}' and " \
                            f"table='{table}' and command='{drop_cmd}'"
            is_done_result = client.execute(sql_mutations)
            result = [x[0] for x in is_done_result]
            if all(result):
                OMNIMV_LOG.info(f"Execute query is done: {drop_sql}")
                is_drop_done = True
                break
        except ServerException as error:
            OMNIMV_LOG.error(error)
        except SocketTimeoutError as error:
            OMNIMV_LOG.error(error)
        time.sleep(10)
        time_count += 10
        OMNIMV_LOG.info(f"waiting {time_count} seconds")
    if not is_drop_done:
        try:
            kill_mutations = f"kill mutation where database='{database}' and table='{table}' and command='{drop_cmd}'"
            client.execute(kill_mutations)
            OMNIMV_LOG.warning(f"Exceed time limit, try to kill mutations: {kill_mutations}")
        except ServerException as error:
            OMNIMV_LOG.error(error)


def drop_other_agent_projection(client, drop_sql, database, table_name, drop_projection_cmd):
    is_distributed_table = get_ck_is_distributed_table()
    if is_distributed_table:
        cluster_ip_port = get_cluster_ips(client)
        for ip_port in cluster_ip_port:
            ck_user = client.connection.user
            ck_password = client.connection.password
            ip = ip_port[0]
            port = ip_port[1]
            ip_client = Client(host=ip, port=port, user=ck_user, password=ck_password, database=database)
            OMNIMV_LOG.info(f'{ip}: {drop_sql}')
            to_drop_projection(ip_client, database, table_name, drop_sql, drop_projection_cmd)


def drop_projections(client, database):
    table_drop_projections = get_table_drop_projections()
    for table_name, projections in table_drop_projections.items():
        for projection in projections:
            drop_projection_cmd = f"DROP PROJECTION IF EXISTS {projection}"
            drop_sql = f"alter table {table_name} {drop_projection_cmd}"
            OMNIMV_LOG.info(drop_sql)
            try:
                to_drop_projection(client, database, table_name, drop_sql, drop_projection_cmd)
                drop_other_agent_projection(client, drop_sql, database, table_name, drop_projection_cmd)
                OMNIMV_LOG.info("Drop projection is done")
            except ServerException as error:
                OMNIMV_LOG.error(error)
                continue


def wait_materialize_projection(client, cube_name):
    projection_wait_time = int(get_ck_projection_wait_time())
    create_cmd = f"command='MATERIALIZE PROJECTION {cube_name}'"
    sql_mutations = f"select is_done from system.mutations where database='{client.connection.database}' and (" \
                    f"{create_cmd})"
    time_count = 0
    is_create_done = False
    while time_count < projection_wait_time:
        try:
            is_done_result = client.execute(sql_mutations)
            result = [x[0] for x in is_done_result]
            if all(result):
                OMNIMV_LOG.info(f"Materialize projection is done")
                is_create_done = True
                break
        except ServerException as error:
            OMNIMV_LOG.error(error)
        except SocketTimeoutError as error:
            OMNIMV_LOG.error(error)
        time.sleep(10)
        time_count += 10
        OMNIMV_LOG.info(f"waiting {time_count} seconds")
    if not is_create_done:
        try:
            kill_mutations = f"kill mutation where database='{client.connection.database}' and ({create_cmd})"
            client.execute(kill_mutations)
            OMNIMV_LOG.warning(f"Exceed time limit, try to kill mutations: {kill_mutations}")
        except ServerException as error:
            OMNIMV_LOG.error(error)


def create_projections(client, projection_path):
    recommend_path = f"{projection_path}/greedy_recommend.txt"
    if not os.path.exists(recommend_path):
        raise FileNotFoundError(f"{recommend_path} does not exists")
    with open(recommend_path, 'r') as file:
        recommend_cubes = json.loads(file.read())
    recommend_ids = []
    for cubes in recommend_cubes.values():
        recommend_ids += cubes
    OMNIMV_LOG.info(f"create selected cube: {recommend_ids}")
    src_directory = f"{projection_path}/mv_create_projections"
    allows_materialize_projection = get_ck_allows_materialize_projection()
    for sql_file in recommend_ids:
        sql_file_path = f"{src_directory}/{sql_file}"
        with open(sql_file_path, 'r') as file:
            query = file.read()
        create_projection_sql = query.split('\n')[0]
        materialize_sql = query.split('\n')[1]
        pattern = r'(projection \w+cube\d+)'
        projection_name = re.findall(pattern, materialize_sql)[0]
        cube_name = projection_name.split(' ')[-1]
        OMNIMV_LOG.info(f"Execute query: {create_projection_sql}")
        try:
            client.execute(create_projection_sql)  # 创建Projection直接使用文件内的on cluster
            time.sleep(3)
        except ServerException as error:
            OMNIMV_LOG.error(error)
            continue
        if allows_materialize_projection:
            materialize_projection(client, materialize_sql, cube_name)


def materialize_projection(client, materialize_sql, cube_name):
    is_distributed_table = get_ck_is_distributed_table()
    OMNIMV_LOG.info(f"Execute query: {materialize_sql}")
    try:
        client.execute(materialize_sql)
        wait_materialize_projection(client, cube_name)
        if is_distributed_table:  # 如果materialize_sql包括on cluster,则需要等待其他节点
            ck_user = client.connection.user
            ck_password = client.connection.password
            database = client.connection.database
            cluster_ip_port = get_cluster_ips(client)
            for ip_port in cluster_ip_port:
                ip = ip_port[0]
                port = ip_port[1]
                ip_client = Client(host=ip, port=port, user=ck_user, password=ck_password, database=database)
                OMNIMV_LOG.info(f'{ip}: {materialize_sql}')
                wait_materialize_projection(ip_client, cube_name)
    except ServerException as error:
        OMNIMV_LOG.error(error)


def measure_performance(client, sqls_path, num_round=5):
    sql_running_time = defaultdict(list)
    sql_running_avg_time = {}
    initial_time = time.time()

    for _ in range(num_round):
        for sql_file in os.listdir(sqls_path):
            file_id = sql_file.split('.')[0]
            sql_file_path = f"{sqls_path}/{sql_file}"
            with open(sql_file_path, 'r') as file:
                query = file.read()
            start = time.time()
            try:
                client.execute(query)
            except ServerException as error:
                OMNIMV_LOG.error(error)
                continue
            sql_running_time[file_id].append((time.time() - start) * 1000)

    total_time = (time.time() - initial_time) * 1000
    for file_id in sql_running_time:
        tmp_result = sql_running_time.get(file_id)
        if len(tmp_result) > 0:
            sql_running_avg_time[file_id] = sum(tmp_result) / len(tmp_result)
    sql_running_time = dict(sorted(sql_running_time.items(), key=lambda x: x[0], reverse=False))
    sql_running_avg_time = dict(sorted(sql_running_avg_time.items(), key=lambda x: x[0], reverse=False))
    return sql_running_time, sql_running_avg_time, total_time


def try_explain_sql(client, sql_exp):
    try:
        client.execute(f'explain {sql_exp}')
        return True
    except (ServerException, IndexError) as _:
        return False


def get_compress_ratio(client, mv_lines, table_name):
    table_lines_query = f"select count() from {table_name}"
    try:
        table_lines_result = client.execute(table_lines_query)
        table_lines = table_lines_result[0][0]
    except (ServerException, ValueError) as error:
        OMNIMV_LOG.error(error)
        return 0, False
    # 计算平均数据压缩比 (mv 行数) / (table行数)
    if table_lines > 0 and mv_lines > 0:
        projection_compress_ratio = mv_lines / table_lines
        # 淘汰数据压缩比过大的projection
        compress_ratio = float(get_ck_projection_compress_ratio())
        if projection_compress_ratio < compress_ratio:
            return projection_compress_ratio, True
        else:
            OMNIMV_LOG.info(f"compress ratio upper limit: {compress_ratio},"
                               f"current projection compress ratio: {projection_compress_ratio}")
    return 0, False


def get_ck_client():
    database = get_ck_database()
    ck_host = get_ck_host()
    ck_port = get_ck_port()

    ck_connected_failed = True
    client = None
    try_count = 0

    while ck_connected_failed:
        if try_count > 5:
            break
        try_count += 1
        ck_user = get_ck_user()
        ck_password = get_ck_password()
        client = Client(host=ck_host, port=ck_port, user=ck_user, password=ck_password, database=database)
        try:
            client.connection.connect()
            ck_connected_failed = False
        except ServerException as error:
            client.disconnect()
            OMNIMV_LOG.error(error.message.split('\n')[0])

    if ck_connected_failed:
        OMNIMV_LOG.info('The user name or password is incorrect for more than five times\n')
        raise LogicalError

    return client


def is_desired_query(query, table_name):
    flag = False
    if 'select count(*) from ' not in query.lower():
        query_line_break = ' ' + query.lower() + '\n'
        mutil_table_name = [
            f' {table_name.lower()} ',
            f' {table_name.lower()}\n',
            f'\n{table_name.lower()} ',
            f'\n{table_name.lower()}\n',
            f'.{table_name.lower()} ',
            f'.{table_name.lower()}\n'
        ]
        if any(name in query_line_break for name in mutil_table_name):
            flag = True
        query_whitespace = ' ' + query.lower() + ' '
        if any(name in query_whitespace for name in mutil_table_name):
            flag = True
    return flag


def check_input(input_str):
    pattern = r'^[\w.]+$'  # 匹配字母、数字、下划线和点号
    if re.match(pattern, input_str):
        return True
    else:
        return False


def extract_sqls(client, table_name, is_incremental=False):
    sql_directory = get_ck_sqls_path()
    flags = os.O_WRONLY | os.O_CREAT
    modes = stat.S_IWUSR | stat.S_IRUSR

    query_set_exists = set()
    if os.path.exists(sql_directory) and is_incremental:
        for file_name in os.listdir(sql_directory):
            sql_table_name = get_table_name(file_name)
            if table_name.strip().upper() != sql_table_name.strip().upper():
                continue
            sql_file_path = os.path.join(sql_directory, file_name)
            with open(sql_file_path, 'r') as file:
                query_set_exists.add(file.read())
    else:
        os.makedirs(sql_directory, mode=0o750, exist_ok=True)

    query_set, query_set_incremental = get_query_set(client, table_name, query_set_exists, is_incremental)
    query_log_table = get_ck_query_log_table()
    if not is_incremental:
        if len(query_set) == 0:
            OMNIMV_LOG.info(f"No query has been extracted from {query_log_table}")
            return
        for sql_id, query in enumerate(sorted(list(query_set))):
            with os.fdopen(os.open(f"{sql_directory}/{table_name}_{sql_id}.sql", flags, modes), 'w') as file:
                file.truncate()
                file.write(query)
        OMNIMV_LOG.info(f"SQL has been extracted from system.query_log into directory: {sql_directory}")
    else:
        if len(query_set_incremental) == 0:
            OMNIMV_LOG.info(f"No incremental query has been extracted from {query_log_table}")
            return
        for sql_id, query in enumerate(sorted(list(query_set_incremental))):
            file_path = f"{sql_directory}/{table_name}_{sql_id + len(query_set)}_incremental.sql"
            with os.fdopen(os.open(file_path, flags, modes), 'w') as file:
                file.truncate()
                file.write(query)
        OMNIMV_LOG.info(f"Incremental SQL has been extracted from system.query_log into directory: {sql_directory}")


def get_query_set(client, table_name, query_set, is_incremental):
    query_set_incremental = set()
    sql_count = get_ck_query_limit_each_table()
    duration_limit = get_ck_sql_running_time_threshold()
    database = get_ck_database()
    limit_count = min(sql_count * 100, 10000)
    query_log_table = get_ck_query_log_table()
    if not check_input(query_log_table):
        OMNIMV_LOG.error(f"{query_log_table} is invalid")
        return query_set, query_set_incremental
    extract_sql_query = f"select distinct on (query) query from {query_log_table} " \
                        f"where query_duration_ms>%(duration_limit)s and databases=[%(database)s] " \
                        f"and arrayJoin(tables) like %(table)s " \
                        f"and query_kind='Select' and is_initial_query=1 " \
                        f"and type='QueryFinish' order by query_start_time desc limit %(limit_count)s"
    params = {
        "duration_limit": duration_limit,
        "database": database,
        "table": f"%{database}.{table_name}%",
        "limit_count": limit_count
    }
    OMNIMV_LOG.info(f"Extract SQL: {extract_sql_query % params}")
    try:
        query_result = client.execute(extract_sql_query, params)
        for query in query_result:
            query = query[0]
            query = remove_special_characters(query)
            desired_count = len(query_set_incremental) if is_incremental else len(query_set)
            if not (is_desired_query(query, table_name) and desired_count < sql_count):
                continue
            if " format " in query.lower():
                query = query[:query.lower().index(" format ")]
            if query not in query_set and is_incremental:
                query_set_incremental.add(query)
            if not is_incremental:
                query_set.add(query)
    except (ServerException, ValueError) as error:
        OMNIMV_LOG.error(error)

    return query_set, query_set_incremental


def get_cluster_ips(client):
    cluster_name = get_ck_cluster_name()
    result = []
    try:
        params = {
            "cluster_name": cluster_name
        }
        query = f"select host_address,port from system.clusters where cluster = %(cluster_name)s and is_local = 0"
        result = client.execute(query, params)
    except (ServerException, ValueError) as error:
        OMNIMV_LOG.error(error)
    return result


def remove_file(directory):
    if os.path.isdir(directory):
        for file_name in os.listdir(directory):
            file_path = f"{directory}/{file_name}"
            os.remove(file_path)


def extract_multi_table_query(sql_path, client):
    database = get_ck_database()
    query_log_table = get_ck_query_log_table()
    sql_count = get_ck_query_limit_mutil_table()
    params = {
        "database": database,
        "sql_count": sql_count
    }
    if not check_input(query_log_table):
        OMNIMV_LOG.error(f"{query_log_table} is invalid")
        return
    extract_sql_query = f"select distinct on (query) query from {query_log_table} " \
                        f"where databases=[%(database)s] and length(tables) > 2 " \
                        f"and query_kind='Select' and is_initial_query=1 " \
                        f"and type='QueryFinish' order by query_start_time desc limit %(sql_count)s"
    OMNIMV_LOG.info(f"Extract SQL: {extract_sql_query}")

    flags = os.O_WRONLY | os.O_CREAT
    modes = stat.S_IWUSR | stat.S_IRUSR
    os.makedirs(sql_path, mode=0o750, exist_ok=True)

    try:
        query_result = client.execute(extract_sql_query, params)
        sql_id = 0
        for query in query_result:
            query = query[0]
            query = remove_special_characters(query)
            if " format " in query.lower():
                query = query[:query.lower().index(" format ")]
            if query.strip() == '':
                continue
            file_path = f"{sql_path}/{database}_{sql_id}_.sql"
            with os.fdopen(os.open(file_path, flags, modes), 'w') as file:
                file.truncate()
                file.write(query)
                sql_id += 1
    except (ServerException, ValueError) as error:
        OMNIMV_LOG.error(error)


