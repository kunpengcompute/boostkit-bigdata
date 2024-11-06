#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import json
import os
import pickle
import re
import stat
import subprocess

from py4j.protocol import Py4JJavaError

from omnimv_logging import get_logger, LOG_DIR
from src.preprocessor.proc_conf_spark import get_spark_app_name, get_spark_master, get_spark_num_executors, \
    get_executor_cores, get_spark_executor_memory, \
    get_driver_memory, get_spark_database, get_cache_plugin_path, \
    get_session_timeout, get_logparser_path, get_hdfs_inputpath, get_hdfs_outputpath, \
    get_spark_mock_database, spark_mock_database_truncate_enable, get_mv_metadata_path

try:
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import AnalysisException, ParseException
    import pydoop.hdfs as hdfs
except ModuleNotFoundError as _:
    pass

INVALID_VALUE = None
OMNIMV_LOG = get_logger()


def check_environment():
    if 'SPARK_HOME' not in os.environ.keys() and 'HADOOP_HOME' not in os.environ.keys():
        raise Exception("SPARK_HOME and HADOOP_HOME is not set")
    spark_home = os.environ['SPARK_HOME']
    hadoop_home = os.environ['HADOOP_HOME']
    return spark_home, hadoop_home


def init_pyspark_plugin(mode='local[*]'):
    spark_home, hadoop_home = check_environment()
    os.environ['PYSPARK_PYTHON'] = f"{spark_home}/python"
    os.environ['PYSPARK_DRIVER_PYTHON'] = f"{hadoop_home}/python"
    cache_plugin_jar_path = get_cache_plugin_path()
    if not os.path.exists(cache_plugin_jar_path):
        raise Exception(f"{cache_plugin_jar_path} does not exists, please check")
    conf = SparkConf().setAppName(get_spark_app_name())
    conf.setMaster(mode)
    conf.set('spark.num.executors', get_spark_num_executors()) \
        .set('spark.executor.cores', get_executor_cores()) \
        .set('spark.executor.memory', get_spark_executor_memory()) \
        .set('spark.driver.memory', get_driver_memory()) \
        .set('spark.jars', cache_plugin_jar_path) \
        .set('spark.sql.extensions', 'com.huawei.boostkit.spark.OmniMV')
    spark_session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    return spark_session


def print_query_plan(spark_session, query, mode="formatted"):
    spark_session.sql(query).explain(mode)


# 包含以下几种mode: [simple, extended, codegen, cost, formatted]
def get_query_plan(spark_session, query, mode="formatted"):
    data_frame = spark_session.sql(query)
    return data_frame._sc._jvm.PythonSQLUtils.explainString(data_frame._jdf.queryExecution(), mode)


def run_cmd(args_list, output_path=None):
    OMNIMV_LOG.info('Running system command: {0}'.format(' '.join(args_list)))
    if not output_path:
        output_file = subprocess.PIPE
    else:
        flags = os.O_WRONLY | os.O_CREAT  # 注意根据具体业务的需要，通过改变os的对应常量来设置相应文件读写方式。
        modes = stat.S_IWUSR | stat.S_IRUSR  # 注意根据具体业务的需要设置文件权限
        output_file = os.fdopen(os.open(output_path, flags, modes), 'w')
    proc = subprocess.Popen(args_list, stdout=output_file, stderr=output_file)
    session_timeout = int(get_session_timeout())
    s_output, s_err = proc.communicate(timeout=session_timeout)
    s_return = proc.returncode
    if s_output:
        s_output = s_output.decode('utf-8').split('\n')
    return s_return, s_output, s_err


def get_mv_bytes(spark_session, mv_table):
    desc_table = f"desc formatted {mv_table}"
    try:
        location = spark_session.sql(desc_table).filter("col_name=='Location'").collect()[0].data_type
    except AnalysisException:
        return INVALID_VALUE
    OMNIMV_LOG.info(f"materialized view location: {location}")
    mv_table_bytes = INVALID_VALUE
    try:
        _, hadoop_home = check_environment()
        (ret, out, err) = run_cmd([f'{hadoop_home}/bin/hdfs', 'dfs', '-du', '-s', location])
        # type(out) is bytes, thus, we should decode it
        if ret == 0:
            mv_table_bytes = out[0].split(' ')[0]
    except subprocess.TimeoutExpired as exception:
        OMNIMV_LOG.error(exception)
    return mv_table_bytes


def generate_json_logs_interval(file_name, start_time, end_time):
    logparser_jar_path = get_logparser_path()
    if not os.path.exists(logparser_jar_path):
        raise FileNotFoundError(f"{logparser_jar_path} does not exists, please check")
    spark_home, hadoop_home = check_environment()
    spark_conf_path = f"{spark_home}/conf/"
    spark_jar_path = f"{spark_home}/jars/*"
    hadoop_conf_path = f"{hadoop_home}/etc/hadoop/"
    logparser_classname = "org.apache.spark.deploy.history.ParseLogs"
    cache_plugin_jar_path = get_cache_plugin_path()
    hdfs_spark_history_path = get_hdfs_inputpath()
    hdfs_output_path = get_hdfs_outputpath()
    dependency_path = f"{spark_conf_path}:{spark_jar_path}:{hadoop_conf_path}:" \
                      f"{cache_plugin_jar_path}:{logparser_jar_path}"
    try:
        run_cmd(['java', '-cp', dependency_path, logparser_classname,
                 hdfs_spark_history_path, hdfs_output_path, file_name,
                 start_time, end_time])
    except subprocess.TimeoutExpired as exception:
        OMNIMV_LOG.error(exception)


def parse_json_logs_interval(hdfs_json_file):
    json_list = []
    if hdfs.path.exists(hdfs_json_file):
        with hdfs.open(hdfs_json_file, 'r') as file:
            json_list = json.loads(file.read().decode('utf-8'))
    return json_list


def write_objects_to_hdfs(data, path):
    hdfs.dump(pickle.dumps(data), path)


def get_objects_from_hdfs(path):
    data = INVALID_VALUE
    if hdfs.path.exists(path):
        data = pickle.loads(hdfs.load(path))
    return data


def write_text_to_hdfs(data, path):
    with hdfs.open(path, 'w') as file:
        file.write(data.encode('utf-8'))
    file.fs.close()


def get_text_from_hdfs(path):
    data = INVALID_VALUE
    if hdfs.path.exists(path):
        with hdfs.open(path, 'r') as file:
            data = file.read().decode('utf-8')
        file.fs.close()
    return data


def get_basic_args_list(enable_omnimv="false"):
    cache_plugin_jar_path = get_cache_plugin_path()
    if not os.path.exists(cache_plugin_jar_path):
        raise FileNotFoundError(f"{cache_plugin_jar_path} does not exists, please check")

    basic_args_list = [
        f"{os.environ['SPARK_HOME']}/bin/spark-sql", "--deploy-mode", "client", "--driver-memory",
        get_driver_memory(), "--num-executors", get_spark_num_executors(), "--executor-cores",
        get_executor_cores(), "--executor-memory", get_spark_executor_memory(), "--master", get_spark_master(),
        "--conf", "spark.sql.extensions=com.huawei.boostkit.spark.OmniMV",
        "--conf", f"spark.sql.omnimv.enable={enable_omnimv}",
        "--conf", f"spark.sql.omnimv.metadata.path={get_mv_metadata_path()}",
        "--jars", cache_plugin_jar_path, "--database", get_spark_database()
    ]

    return basic_args_list


def run_sqls_on_spark(sqls_path, enable_omnimv="false", post_fix="rewrite"):
    flags = os.O_WRONLY | os.O_CREAT  # 注意根据具体业务的需要，通过改变os的对应常量来设置相应文件读写方式。
    modes = stat.S_IWUSR | stat.S_IRUSR  # 注意根据具体业务的需要设置文件权限

    basic_args_list = get_basic_args_list(enable_omnimv)

    for sql_path in sqls_path:
        sql_name = sql_path.split('/')[-1]
        query_sql = get_text_from_hdfs(sql_path)
        with os.fdopen(os.open('tmp.sql', flags, modes), 'w') as file:
            file.truncate()
            file.write(query_sql)
        output_path = f"{LOG_DIR}/{sql_name}_omnimv_plugin_{enable_omnimv}_{post_fix}.log"
        query_arg_list = basic_args_list + ["--name", sql_name, "-f", "tmp.sql"]
        try:
            run_cmd(query_arg_list, output_path)
        except subprocess.TimeoutExpired as exception:
            OMNIMV_LOG.error(exception)
            if os.path.exists('tmp.sql'):
                os.remove('tmp.sql')
    if os.path.exists('tmp.sql'):
        os.remove('tmp.sql')


def create_mv_tables(mv_path, enable_omnimv='true'):
    flags = os.O_WRONLY | os.O_CREAT  # 注意根据具体业务的需要，通过改变os的对应常量来设置相应文件读写方式。
    modes = stat.S_IWUSR | stat.S_IRUSR  # 注意根据具体业务的需要设置文件权限

    basic_args_list = get_basic_args_list(enable_omnimv)
    fs = hdfs.hdfs("default", 0)
    for mv_file in fs.list_directory(mv_path):
        query_path = mv_file.get('name')
        mv_id = query_path.split('/')[-1]
        create_mv_sql = get_text_from_hdfs(query_path)
        with os.fdopen(os.open('tmp.sql', flags, modes), 'w') as file:
            file.truncate()
            file.write(create_mv_sql)
        output_path = f"{LOG_DIR}/{mv_id}_omnimv_plugin_{enable_omnimv}.log"
        query_arg_list = basic_args_list + ["--name", mv_id, "-f", "tmp.sql"]
        try:
            run_cmd(query_arg_list, output_path)
        except subprocess.TimeoutExpired as exception:
            OMNIMV_LOG.error(exception)
            if os.path.exists('tmp.sql'):
                os.remove('tmp.sql')
    if os.path.exists('tmp.sql'):
        os.remove('tmp.sql')


def get_rewrite_sqls(mock_database, schema_path, mv_path, sqls_path):
    session = get_mock_database_session(schema_path, mock_database)

    fs = hdfs.hdfs("default", 0)

    for mv_file in fs.list_directory(mv_path):
        query_path = mv_file.get('name')
        create_mv_sql = get_text_from_hdfs(query_path)
        try:
            session.sql(create_mv_sql)
        except (AnalysisException, ParseException, Py4JJavaError) as exception:
            OMNIMV_LOG.info(exception)
            continue

    pattern = re.compile(r'mv[0-9]+')
    rewrite_sqls = {}
    mv_set = set()
    mv_list = []
    for sql_name in fs.list_directory(sqls_path):
        sql_path = sql_name.get('name')
        try:
            query_sql = get_text_from_hdfs(sql_path)
            result = get_query_plan(session, query_sql, 'simple')
            mv_id = pattern.findall(result)
            if mv_id:
                rewrite_sqls[sql_path] = set(mv_id)
                mv_list.extend(mv_id)
        except (AnalysisException, ParseException) as exception:
            continue

    for mv_ids in rewrite_sqls.values():
        for mv_id in mv_ids:
            mv_set.add(mv_id)

    session.stop()

    return rewrite_sqls, mv_list


def truncate_mock_database(session):
    if spark_mock_database_truncate_enable():
        tables = session.sql("SHOW TABLES").collect()
        for table in tables:
            table_name = table['tableName']
            if re.match("mv\d+", table_name):
                drop_table = f"DROP MATERIALIZED VIEW IF EXISTS {table_name}"
            else:
                drop_table = f"DROP TABLE IF EXISTS {table_name}"
            session.sql(drop_table)

def get_mock_database_session(schema_path, mock_database=get_spark_mock_database()):
    session = init_pyspark_plugin()
    with hdfs.open(schema_path, 'r', encoding='utf-8') as file:
        schema_sql = file.read().decode('utf-8')
    create_table_sqls = schema_sql.split(';')
    # create database and use database
    create_database = f"create database if not exists {mock_database}"
    use_database = f"use {mock_database}"
    try:
        session.sql(create_database)
        session.sql(use_database)
        truncate_mock_database(session)
    except (AnalysisException, ParseException) as exception:
        OMNIMV_LOG.error(f'{mock_database} create fail: {exception}')
    for sql in create_table_sqls[:-1]:
        sql = sql.replace(get_spark_database(), mock_database)
        if 'if not exists' not in sql.lower():
            sql = sql.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
        try:
            session.sql(sql)
        except ParseException as exception:
            OMNIMV_LOG.error(f'{mock_database} tables create fail: {exception}')
    return session


def try_execute_sql(session, sql_exp):
    try:
        session.sql(sql_exp).collect()
        return True
    except (AnalysisException, ParseException) as _:
        return False