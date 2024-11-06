#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

class JoinMsg(object):
    def __init__(self, head_table, head_table_alias):
        self.head_table = head_table
        self.head_table_alias = head_table_alias
        self.joined_tables_msg = []

    def append(self, table_name, table_alias, join_type, join_conditions):
        # on 里不能合并，故join_conditions不区分连接条件和过滤条件
        joined_table_msg = JoinedTableMsg(table_name, table_alias, join_type, join_conditions)
        self.joined_tables_msg.append(joined_table_msg)

    def equal(self, join_msg):
        if isinstance(join_msg, JoinMsg):
            if self.head_table != join_msg.head_table:
                return False
            if self.head_table_alias != join_msg.head_table_alias:
                return False
            if len(self.joined_tables_msg) != len(join_msg.joined_tables_msg):
                return False
            for i, _ in enumerate(self.joined_tables_msg):
                joined_table_msg_1 = self.joined_tables_msg[i]
                joined_table_msg_2 = join_msg.joined_tables_msg[i]
                if not joined_table_msg_1.equal(joined_table_msg_2):
                    return False
            return True
        return False


class JoinedTableMsg(object):
    def __init__(self, table_name, table_alias, join_type, join_conditions):
        self.table_name = table_name
        self.table_alias = table_alias
        self.join_type = join_type
        self.join_conditions = join_conditions

    def equal(self, joined_table_msg):
        if isinstance(joined_table_msg, JoinedTableMsg):
            if self.table_name.strip() != joined_table_msg.table_name.strip():
                return False
            if self.table_alias.strip() != joined_table_msg.table_alias.strip():
                return False
            if self.join_type.strip() != joined_table_msg.join_type.strip():
                return False
            if len(self.join_conditions) != len(joined_table_msg.join_conditions):
                return False
            join_conditions_set_1 = set()
            join_conditions_set_2 = set()
            for condition in self.join_conditions:
                join_conditions_set_1.add(condition.replace(' ', '').upper())
            for condition in joined_table_msg.join_conditions:
                join_conditions_set_2.add(condition.replace(' ', '').upper())
            return join_conditions_set_1 == join_conditions_set_2
        return False


class TableMsg(object):
    """
        标准化from子句时获取此类对象，用于标准化其他子句和on的条件表达式
    """

    def __init__(self, table_name, table_old_alias, table_new_alias, columns):
        self.table_name = table_name
        self.table_old_alias = table_old_alias
        self.table_new_alias = table_new_alias
        self.columns = columns