#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from src.candidate_generation.clickhouse.materialize_view_generation.join_msg import JoinMsg
from src.candidate_generation.clickhouse.materialize_view_generation.parse_alias import AliasParser
from src.candidate_generation.clickhouse.materialize_view_generation.parse_condition import split_conditions_by_and
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import get_tokens, \
    TargetIndexGetter


class ViewParser(object):
    def __init__(self, original_view):
        if original_view.startswith('('):
            original_view = original_view[1:len(original_view) - 1]
        self.original_view = original_view
        self.original_view_tokens = get_tokens(original_view)

    def parse(self):
        join_msgs = self.__parse_from_item()
        select_items = self.__parse_select_items()
        conditions = self.__get_conditions()
        group_items = self.__get_group_items()
        return [join_msgs, select_items, conditions, group_items]

    def get_view_tables(self):
        join_msgs = self.__parse_from_item()
        tables_and_alias = []
        for join_msg in join_msgs:
            tables_and_alias.append({'table_name': join_msg.head_table,
                                     'table_alias': join_msg.head_table_alias})
            for joined_table_msg in join_msg.joined_tables_msg:
                tables_and_alias.append({'table_name': joined_table_msg.table_name,
                                         'table_alias': joined_table_msg.table_alias})
        return tables_and_alias

    def __get_clause_items(self, clause_start, clause_end):
        clause_tokens = []
        for i in range(clause_start + 1, clause_end):
            clause_tokens.append(self.original_view_tokens[i])
        target_index_getter = TargetIndexGetter(clause_tokens)
        commas_index = target_index_getter.get_commas_index()
        items_tokens = []
        for i in range(0, len(commas_index) - 1):
            item_start = commas_index[i] + 1
            item_end = commas_index[i + 1]
            item_tokens = []
            for j in range(item_start, item_end):
                item_tokens.append(clause_tokens[j])
            items_tokens.append(item_tokens)
        return items_tokens

    def __get_from_items(self):
        target_index_getter = TargetIndexGetter(self.original_view_tokens)
        from_index = target_index_getter.get_target_index('from')
        end_signs = ['where', 'group by', 'having', 'order by', 'limit', ';']
        from_end = target_index_getter.get_target_end_index(end_signs, from_index)
        from_items = self.__get_clause_items(from_index, from_end)
        return from_items

    def __parse_from_item(self):
        items_tokens = self.__get_from_items()
        join_msgs = []
        for item_tokens in items_tokens:  # 按逗号分割循环处理
            item_exp = ' ' + ' '.join(item_tokens) + ' '
            # item_exp 共2种：1、单表;  2、多表连续join
            if ' join ' not in item_exp.lower():
                alias_parser = AliasParser(item_exp)
                table_name, table_alias = alias_parser.get_name_alias()
                join_msg = JoinMsg(table_name, table_alias)
                join_msgs.append(join_msg)
            else:
                join_msg = get_continuum_join_msg(item_tokens)
                join_msgs.append(join_msg)
        return join_msgs

    def __get_select_items(self):
        target_index_getter = TargetIndexGetter(self.original_view_tokens)
        select_index = target_index_getter.get_target_index('select')
        from_index = target_index_getter.get_target_index('from')
        select_items = self.__get_clause_items(select_index, from_index)
        return select_items

    def __parse_select_items(self):
        items_tokens = self.__get_select_items()
        select_items = []
        for item_tokens in items_tokens:
            item_exp = ' '.join(item_tokens)
            alias_parser = AliasParser(item_exp)
            item_name, item_alias = alias_parser.get_name_alias()
            select_items.append({'item_name': item_name, 'item_alias': item_alias})
        return select_items

    def __get_where_tokens(self):
        target_index_getter = TargetIndexGetter(self.original_view_tokens)
        where_index = target_index_getter.get_target_index('where')
        if where_index == -1:
            return []
        end_signs = ['group by', 'having', 'order by', 'limit', ';']
        where_end = target_index_getter.get_target_end_index(end_signs, where_index)
        where_tokens = []
        for i in range(where_index + 1, where_end):
            where_tokens.append(self.original_view_tokens[i])
        return where_tokens

    def __get_conditions(self):
        where_tokens = self.__get_where_tokens()
        target_index_getter = TargetIndexGetter(where_tokens)
        and_indexes = target_index_getter.get_and_indexes()
        filter_conditions = []
        for i in range(len(and_indexes) - 1):
            condition_start = and_indexes[i] + 1
            condition_end = and_indexes[i + 1]
            condition_tokens = []
            for j in range(condition_start, condition_end):
                condition_tokens.append(where_tokens[j])
            condition_exp = ' '.join(condition_tokens)
            filter_conditions.append(condition_exp)
        return filter_conditions

    def __get_group_items(self):
        target_index_getter = TargetIndexGetter(self.original_view_tokens)
        group_by_index = target_index_getter.get_target_index('group by')
        if group_by_index == -1:
            return []
        end_signs = ['having', 'order by', 'limit', ';']
        group_by_end = target_index_getter.get_target_end_index(end_signs, group_by_index)
        group_items_tokens = self.__get_clause_items(group_by_index, group_by_end)
        group_items_exp = []
        for item_tokens in group_items_tokens:
            item_exp = ' '.join(item_tokens)
            group_items_exp.append(item_exp)
        return group_items_exp


def get_continuum_join_msg(join_tokens):
    """
        解析连续join
    """
    join_index = []
    for i, _ in enumerate(join_tokens):
        token = ' ' + join_tokens[i] + ' '
        if ' join ' in token.lower():
            join_index.append(i)
    # 首表处理
    first_join_index = join_index[0]
    first_table_tokens = []
    for i in range(0, first_join_index):
        first_table_tokens.append(join_tokens[i])
    first_table_exp = ' '.join(first_table_tokens)

    first_table_name, first_table_alias = AliasParser(first_table_exp).get_name_alias()
    join_msg = JoinMsg(first_table_name, first_table_alias)

    # 处理后续表
    join_index.append(len(join_tokens))
    for i, _ in enumerate(join_index):
        if i == len(join_index) - 1:
            break
        join_type = join_tokens[join_index[i]]
        start_index = join_index[i] + 1
        end_index = join_index[i + 1]  # [start_index,end_index)
        follow_table_tokens = []
        condition_tokens = []
        visited_on = False
        for j in range(start_index, end_index):
            if join_tokens[j].lower().strip() == 'on':
                visited_on = True
                continue
            if not visited_on:
                follow_table_tokens.append(join_tokens[j])
            else:
                condition_tokens.append(join_tokens[j])
        follow_table_exp = ' '.join(follow_table_tokens)

        follow_table_name, follow_table_alias = AliasParser(follow_table_exp).get_name_alias()

        if condition_tokens[0] == '(' and condition_tokens[-1] == ')':
            swap_tokens = []
            for j in range(1, len(condition_tokens) - 1):
                swap_tokens.append(condition_tokens[j])
            condition_tokens = swap_tokens
        conditions = split_conditions_by_and(condition_tokens)
        join_msg.append(table_name=follow_table_name,
                        table_alias=follow_table_alias, join_type=join_type,
                        join_conditions=conditions)
    return join_msg