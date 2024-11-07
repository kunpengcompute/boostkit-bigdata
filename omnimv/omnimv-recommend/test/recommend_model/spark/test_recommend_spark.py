#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest
from unittest import mock

from src.recommend_model.spark.recommend_spark import get_large_tables, get_selected_mv, \
    spark_greedy_recommend


class Test01(unittest.TestCase):

    def setUp(self) -> None:
        self.table_bytes = {
            'WEB_RETURNS': '2000000000',
            'WEB_SALES': '3000000000',
            'WEB_SITE': '7337'
        }
        self.large_table = ['WEB_RETURNS', 'WEB_SALES']

    @mock.patch("src.recommend_model.spark.recommend_spark.get_objects_from_hdfs")
    def test(self, mock_table_bytes):
        mock_table_bytes.return_value = self.table_bytes
        large_table = get_large_tables()
        self.assertEqual(large_table, self.large_table)


class Test02(unittest.TestCase):

    def setUp(self) -> None:
        self.rewrite_sqls = {'q1': ['mv1', 'mv2'], 'q2': ['mv1', 'mv3'], 'q3': ['mv3', 'mv2']}
        self.recommend_mv_list = ['mv1', 'mv2']

    def test(self):
        mv_sql_mappings, covered_sql, select_mv = get_selected_mv(self.rewrite_sqls, self.recommend_mv_list)
        self.assertEqual(len(covered_sql), 2)
        self.assertEqual(select_mv, ['mv1'])


class Test03(unittest.TestCase):

    def setUp(self) -> None:
        self.rewrite_sqls = {'q1': ['mv1', 'mv2'], 'q2': ['mv1', 'mv3'], 'q3': ['mv3', 'mv2']}
        self.recommend_mv_list = ['mv1', 'mv2']

    @mock.patch("src.recommend_model.spark.recommend_spark.get_large_tables")
    @mock.patch("src.recommend_model.spark.recommend_spark.write_text_to_hdfs")
    @mock.patch("src.recommend_model.spark.recommend_spark.get_text_from_hdfs")
    @mock.patch("src.recommend_model.spark.recommend_spark.get_rewrite_sqls")
    def test(self, mock_rewrite, mock_get, mock_write, mock_large_tables):
        mock_rewrite.return_value = self.rewrite_sqls, self.recommend_mv_list
        mock_get.return_value = "select * from test"
        mock_write.return_value = True
        mock_large_tables.return_value = ['test']
        spark_greedy_recommend()


if __name__ == '__main__':
    unittest.main()