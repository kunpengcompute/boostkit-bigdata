#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import unittest.mock

from src.candidate_generation.common.materialize_view_generation.join_msg import JoinMsg, JoinedTableMsg, TableMsg


class Test00(unittest.TestCase):
    """
        test JoinMsg
    """

    def setUp(self) -> None:
        self.head_table = ['table1', 't1']
        self.follow_table = ['table2', 't2', 'inner join', ['t1.id = t2.id']]

    def test(self):
        join_msg = JoinMsg(self.head_table[0], self.head_table[1])
        join_msg.append(self.follow_table[0], self.follow_table[1], self.follow_table[2], self.follow_table[3])
        not_equal = not join_msg.equal(join_msg)
        self.assertFalse(not_equal)


class Test01(unittest.TestCase):
    """
        test JoinedTableMsg
    """

    def setUp(self) -> None:
        self.table_name = 'table1'
        self.table_alias = 't1'
        self.join_type = 'inner join'
        self.join_conditions = ['t0.id = t1.id']

    def test(self):
        joined_table_msg = JoinedTableMsg(self.table_name, self.table_alias, self.join_type, self.join_conditions)
        not_equal = not joined_table_msg.equal(joined_table_msg)
        self.assertFalse(not_equal)


class Test02(unittest.TestCase):
    """
        test TableMsg
    """

    def setUp(self) -> None:
        self.table_name = 'table1'
        self.table_old_alias = 't1'
        self.table_new_alias = 't_1'
        self.columns = ['id', 'year']

    def test(self):
        table_msg = TableMsg(self.table_name, self.table_old_alias, self.table_new_alias, self.columns)
        self.assertEqual(
            [table_msg.table_name, table_msg.table_old_alias, table_msg.table_new_alias, table_msg.columns],
            [self.table_name, self.table_old_alias, self.table_new_alias, self.columns])


if __name__ == '__main__':
    unittest.main()

