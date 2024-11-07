#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest.mock

from src.candidate_generation.common.materialize_view_generation.parse_alias import AliasParser


class Test00(unittest.TestCase):
    """
        test AliasParser
    """

    def setUp(self) -> None:
        self.items = [
            'date_dim.d_month_seq + 3 ds', 'cs_quantity * cs_list_price sales', '\'work\' wk', 'sum(a+b) sum',
            'toYear(data) as year', 'cost AS ct',
            "sum ( CASE WHEN ( cast ( d_date AS DATE ) < "
            "cast ( '2000-03-11' AS DATE ) ) "
            "THEN inv_quantity_on_hand ELSE 0 END ) AS inv_before"
        ]
        self.result = {
            'date_dim.d_month_seq + 3 ds': ['date_dim.d_month_seq + 3', 'ds'],
            'cs_quantity * cs_list_price sales': ['cs_quantity * cs_list_price', 'sales'],
            '\'work\' wk': ['\'work\'', 'wk'],
            'sum(a+b) sum': ['sum(a+b)', 'sum'],
            'toYear(data) as year': ['toYear(data)', 'year'],
            'cost AS ct': ['cost', 'ct'],
            "sum ( CASE WHEN ( cast ( d_date AS DATE ) < "
            "cast ( '2000-03-11' AS DATE ) ) "
            "THEN inv_quantity_on_hand ELSE 0 END ) AS inv_before": [
                "sum ( CASE WHEN ( cast ( d_date AS DATE ) < "
                "cast ( '2000-03-11' AS DATE ) ) "
                "THEN inv_quantity_on_hand ELSE 0 END )", 'inv_before'
            ]
        }

    def test(self):
        for item in self.items:
            alias_parser = AliasParser(item)
            item_name, item_alias = alias_parser.get_name_alias()
            self.assertEqual([item_name, item_alias], self.result.get(item))


if __name__ == '__main__':
    unittest.main()