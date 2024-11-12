#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest

from src.preprocessor.proc_conf_spark import get_mv_output_path, get_spark_master, get_spark_app_name, \
    get_spark_num_executors, get_spark_executor_memory, get_driver_memory, get_mv_limit, \
    get_spark_database, get_logparser_path, get_cache_plugin_path, get_hdfs_inputpath, get_hdfs_outputpath, \
    get_omnimv_version, get_session_timeout, get_executor_cores, \
    get_large_table_threshold, get_spark_sqls_path, \
    get_spark_schema_path, \
    get_table_bytes_path


class Test03(unittest.TestCase):
    def test(self):
        get_spark_schema_path()

        get_table_bytes_path()

        get_spark_database()

        get_logparser_path()

        get_cache_plugin_path()

        get_hdfs_inputpath()

        get_hdfs_outputpath()

        get_spark_sqls_path()

        get_large_table_threshold()

        get_mv_limit()

        get_mv_output_path()

        get_spark_master()

        get_spark_app_name()

        get_spark_num_executors()

        get_spark_executor_memory()

        get_driver_memory()

        get_executor_cores()

        get_session_timeout()

        get_omnimv_version()


if __name__ == '__main__':
    unittest.main()