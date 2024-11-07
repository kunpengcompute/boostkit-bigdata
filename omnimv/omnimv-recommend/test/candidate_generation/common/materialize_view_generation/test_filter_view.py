#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest.mock

from src.candidate_generation.common.materialize_view_generation.filter_view import ViewFilter


class Test00(unittest.TestCase):
    """
        test get_sub_queries：最简单join
    """

    def setUp(self) -> None:
        self.sql = "select sum(t1.cost) " \
                   "from line_order t1 join line_order t2 on t1.data = t2.data " \
                   "where region = 'ccc'"
        self.view = "select sum ( t1.cost ) " \
                    "from line_order t1 join line_order t2 on t1.data = t2.data " \
                    "where region = 'ccc'"

    def test(self):
        view_filter = ViewFilter(self.sql)
        view = view_filter.get_view()
        self.assertEqual(view, self.view)


if __name__ == '__main__':
    unittest.main()