#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest
from unittest import mock

from src.candidate_generation.common.materialize_view_generation import normalize_view
from src.candidate_generation.common.materialize_view_generation.normalize_view import ViewNormalizer


class Test00(unittest.TestCase):
    """
        test get_sub_queries：最简单join
    """

    def setUp(self) -> None:
        self.sql = "select sum(t1.cost) " \
                   "from line_order t1 join line_order t2 on t1.data = t2.data,line_order t3 " \
                   "where region = 'ccc' and region = 'bbb' and t3.data = t1.data " \
                   "group by t1.data,t2.data"
        self.view = "SELECT sum ( table_1.cost ) column0 " \
                    "FROM line_order table_1 INNER JOIN line_ordertable_2 " \
                    "ON table_2.data = table_1.data , line_order table_3  " \
                    "WHERE table_3.data = table_1.data AND ( table_1.region = 'ccc' AND table_1.region = 'bbb' ) " \
                    "GROUP BY table_1.data , table_2.data;"

    def test(self):
        normalize_view.get_table_out_columns = mock.Mock(return_value=['cost', 'data', 'region'])
        view_normalizer = ViewNormalizer(self.sql, ['1.sql'])
        normalized_view = view_normalizer.get_normalized_view()
        view = str(normalized_view)
        normalized_view.union(normalized_view)


if __name__ == '__main__':
    unittest.main()