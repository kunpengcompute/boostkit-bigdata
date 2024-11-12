#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest.mock

from src.candidate_generation.common.materialize_view_generation.join_msg import TableMsg
from src.candidate_generation.common.materialize_view_generation.parse_condition import split_conditions_by_and, \
    normalize_conditions


class Test00(unittest.TestCase):
    """
        test split_conditions_by_and
    """

    def setUp(self) -> None:
        self.conditions_tokens = ['id', '=', '10', 'and', 'year', 'between', '2021', 'and', '2023']
        self.result = ['id = 10', 'year between 2021 and 2023']

    def test(self):
        conditions = split_conditions_by_and(self.conditions_tokens)
        self.assertEqual(conditions, self.result)


class Test01(unittest.TestCase):
    """
        test ConditionNormalizer
    """

    def setUp(self) -> None:
        self.conditions = ['sc.year between 2021 and 2023', 'sc.id = ss.id', '( ss.cost + 10 ) > 100']
        self.tables_msg = [TableMsg('score', 'sc', 't1', ['id', 'year']), TableMsg('ssa', 'ss', 't2', ['id', 'cost'])]

    def test(self):
        normalized_conditions = normalize_conditions(self.tables_msg, self.conditions)
        self.assertEqual(normalized_conditions,
                          ['t1.year between 2021 and 2023', 't2.id = t1.id', '( t2.cost + 10 ) > 100'])


if __name__ == '__main__':
    unittest.main()