#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest.mock

from src.candidate_generation.common.materialize_view_generation.parse_view import ViewParser


class Test00(unittest.TestCase):
    """
        test get_sub_queries：最简单join
    """

    def setUp(self) -> None:
        self.sql = "select sum(t1.cost) " \
                   "from line_order t1 join line_order t2 on t1.data = t2.data " \
                   "where region = 'ccc'"
        self.select_items = [{'item_name': 'sum ( t1.cost )', 'item_alias': ''}]
        self.conditions = ["region = 'ccc'"]
        self.group_items = []

    def test(self):
        view_parser = ViewParser(self.sql)
        parse_result = view_parser.parse()
        join_msgs = parse_result[0]
        select_items = parse_result[1]
        conditions = parse_result[2]
        group_items = parse_result[3]
        self.assertEqual(select_items, self.select_items)
        self.assertEqual(conditions, self.conditions)
        self.assertEqual(group_items, self.group_items)


class Test01(unittest.TestCase):
    """
        test get_sub_queries：最简单join
    """

    def setUp(self) -> None:
        self.sql = "select sum(t1.cost) " \
                   "from line_order t1 join line_order t2 on t1.data = t2.data,line_order t3 " \
                   "where region = 'ccc' and region = 'bbb' and t3.data = t1.data " \
                   "group by t1.data,t2.data"
        self.select_items = [{'item_name': 'sum ( t1.cost )', 'item_alias': ''}]
        self.conditions = ["region = 'ccc'", "region = 'bbb'", 't3.data = t1.data']
        self.group_items = ['t1.data', 't2.data']

    def test(self):
        view_parser = ViewParser(self.sql)
        parse_result = view_parser.parse()
        join_msgs = parse_result[0]
        select_items = parse_result[1]
        conditions = parse_result[2]
        group_items = parse_result[3]
        self.assertEqual(select_items, self.select_items)
        self.assertEqual(conditions, self.conditions)
        self.assertEqual(group_items, self.group_items)


if __name__ == '__main__':
    unittest.main()

