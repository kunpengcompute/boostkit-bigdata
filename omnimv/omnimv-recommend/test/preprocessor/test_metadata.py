#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest
from unittest import mock

from src.preprocessor import metadata
from src.preprocessor.metadata import get_spark_table_columns, TableSchema


class MockTableSchema(object):
    def __init__(self):
        self.data = {'SSB.LINEORDER_FLAT': {'columns': {'DATA': 'Data', 'ID': 'int16'}}}


class Test03(unittest.TestCase):
    """
         test get_spark_table_columns
    """

    def setUp(self) -> None:
        self.columns = ['DATA', 'ID']

    def test(self):
        metadata.hdfs = mock.Mock(return_value=None)
        metadata.get_ck_schema_path = mock.Mock(return_value=None)
        metadata.init_pyspark_session = mock.Mock(return_value=None)
        metadata.get_spark_database = mock.Mock(return_value=None)
        metadata.get_mv_bytes = mock.Mock(return_value=None)

        schema = MockTableSchema()
        metadata.TableSchema = mock.Mock(return_value=schema)
        columns = get_spark_table_columns('lineorder_flat')
        self.assertEqual(columns, self.columns)


class Test04(unittest.TestCase):
    """
        test TableSchema()
    """

    def setUp(self) -> None:
        import sys
        sys.argv = ['', 'common']
        # 如果没有首行\n，会被消失一个字符，真正从文件读没发现此问题
        self.schema_file = "\n" \
                           "CREATE TABLE cxy_distrabuted.lineorder_flat_all\n" \
                           "(\n" \
                           "    `LO_ORDERKEY` UInt32,\n" \
                           "    `LO_LINENUMBER` UInt8\n" \
                           ")\n" \
                           "ENGINE = Distributed('mycluster', 'cxy_distrabuted', 'lineorder_flat', rand());\n" \
                           "\n" \
                           "CREATE TABLE cxy_distrabuted.lineorder_flat\n" \
                           "(\n" \
                           "    `LO_ORDERKEY` UInt32,\n" \
                           "    `LO_LINENUMBER` UInt8,\n" \
                           "    PROJECTION cube1\n" \
                           "    (\n" \
                           "        SELECT\n" \
                           "             P_BRAND\n" \
                           "        GROUP BY \n" \
                           "            P_BRAND\n    ),\n" \
                           "    PROJECTION cube3\n" \
                           "    (\n" \
                           "        SELECT \n" \
                           "            C_NATION\n" \
                           "        GROUP BY \n" \
                           "            C_NATION\n" \
                           "    )\n" \
                           ")\n" \
                           "ENGINE = MergeTree\n" \
                           "ORDER BY (LO_ORDERDATE, LO_ORDERKEY)\nSETTINGS index_granularity = 8192;"
        self.lineorder_flat_all = {
            'columns':
                {
                    'LO_ORDERKEY': 'UInt32',
                    'LO_LINENUMBER': 'UInt8'
                },
            'projections': [],
            'engine_info': {
                'local_table': 'lineorder_flat'
            }
        }

if __name__ == '__main__':
    unittest.main()