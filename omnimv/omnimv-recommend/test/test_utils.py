#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import json
import unittest
from unittest import mock
from src.candidate_generation.spark import spark_utils

from src.candidate_generation.spark.spark_utils import get_mv_bytes, \
    get_objects_from_hdfs, write_objects_to_hdfs, \
    write_text_to_hdfs, get_text_from_hdfs, run_sqls_on_spark, create_mv_tables, get_mock_database_session
from omnimv_logging import get_logger


class Test00(unittest.TestCase):
    """
        test get_logger    √
    """

    def test(self):
        get_logger()


class Test02(unittest.TestCase):
    """
        test get_mv_bytes    √
    """

    class SparkSession:
        class SQL:
            class Filter:
                class Num:
                    def __init__(self):
                        self.data_type = 'int'

                @staticmethod
                def collect():
                    return [Test02.SparkSession.SQL.Filter.Num()]

            @staticmethod
            def filter(s=""):
                return Test02.SparkSession.SQL.Filter()

        @staticmethod
        def sql(s=""):
            return Test02.SparkSession.SQL()

    def setUp(self) -> None:
        self.spark_session = Test02.SparkSession()
        self.mv_table = ""

    def test(self):
        spark_utils.run_cmd = \
            mock.Mock(return_value=(0, ["spark session"], 'err'))
        get_mv_bytes(self.spark_session, self.mv_table)


class Test08(unittest.TestCase):
    """
        test write_objects_to_hdfs
    """

    def setUp(self) -> None:
        self.data = ''
        self.path = ''

    @mock.patch("src.candidate_generation.spark.spark_utils.pickle.dumps")
    @mock.patch("src.candidate_generation.spark.spark_utils.hdfs.open")
    def test(self, mock_hdfs, mock_pickle):
        mock_hdfs = mock.mock_open(read_data="data")
        mock_pickle = mock.Mock(return_value="")
        write_objects_to_hdfs(self.data, self.path)


class Test09(unittest.TestCase):
    """
        test get_objects_from_hdfs
    """

    def setUp(self) -> None:
        self.path = ''

    @mock.patch("src.candidate_generation.spark.spark_utils.hdfs.path")
    @mock.patch("src.candidate_generation.spark.spark_utils.pickle.loads")
    @mock.patch("src.candidate_generation.spark.spark_utils.hdfs.open")
    def test(self, mock_hdfs, mock_pickle, mock_path):
        mock_hdfs = mock.mock_open(read_data="data")
        mock_pickle = mock.Mock(return_value="")
        mock_path.exists = mock.Mock(return_value=True)

        get_objects_from_hdfs(self.path)


class Test10(unittest.TestCase):
    """
        test write_text_to_hdfs
    """

    def setUp(self) -> None:
        self.data = json.dumps({'1': 1})
        self.path = ''

    @mock.patch("src.candidate_generation.spark.spark_utils.hdfs.open")
    def test(self, mock_hdfs):
        mock_hdfs = mock.mock_open(read_data="data")

        write_text_to_hdfs(self.data, self.path)


class Test11(unittest.TestCase):
    """
        test get_text_from_hdfs
    """

    def setUp(self) -> None:
        self.path = ''

    @mock.patch("src.candidate_generation.spark.spark_utils.hdfs.path")
    @mock.patch("src.candidate_generation.spark.spark_utils.hdfs.open")
    def test(self, mock_hdfs, mock_path):
        mock_hdfs = mock.mock_open(read_data=b"data")
        mock_path.exists = mock.Mock(return_value=True)

        get_text_from_hdfs(self.path)


class Test12(unittest.TestCase):
    """
        test run_sqls_on_spark
    """

    def setUp(self) -> None:
        self.sqls_path = ['data/sql1', 'data/sql2']

    def test(self):
        spark_utils.get_basic_args_list = \
            mock.Mock(return_value=(["usr/local/spark/"]))
        spark_utils.get_text_from_hdfs = \
            mock.Mock(return_value='select * from text')
        spark_utils.run_cmd = mock.Mock(return_value=None)

        run_sqls_on_spark(self.sqls_path)


class MockSession(object):
    def __init__(self, database):
        self.database = database

    def sql(self, query):
        return True


class Test13(unittest.TestCase):
    """
        test run_sqls_on_spark
    """

    def setUp(self) -> None:
        self.database = 'mock'
        self.data = 'a;b;c'
        self.schema_path = 'test_path'

    @mock.patch("src.candidate_generation.spark.spark_utils.truncate_mock_database")
    @mock.patch("pydoop.hdfs.open")
    @mock.patch("src.candidate_generation.spark.spark_utils.init_pyspark_plugin")
    def test(self, mock_session, mock_open, mock_truncate):
        mock_session.return_value = MockSession(self.database)
        mock_hdfs = mock.mock_open(read_data=self.data)
        mock_truncate.return_value = True
        get_mock_database_session(self.schema_path)


if __name__ == '__main__':
    unittest.main()