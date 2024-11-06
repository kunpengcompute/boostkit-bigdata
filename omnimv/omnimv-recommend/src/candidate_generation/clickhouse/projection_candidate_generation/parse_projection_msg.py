#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from src.candidate_generation.clickhouse.materialize_view_generation.parse_alias import AliasParser
from src.candidate_generation.clickhouse.projection_candidate_generation.agg_functions import agg_functions
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import get_tokens, \
    TargetIndexGetter, get_table_columns, normalize_sql
from src.preprocessor.proc_conf_ck import get_ck_projection_with_conditional_functions, get_clickhouse_tables
from omnimv_logging import get_logger

OMNIMV_LOG = get_logger()


class ProjectionMsg(object):
    def __init__(self, _select_items, _group_items, _table):
        self.select_items = _select_items
        self.group_items = _group_items
        self.table = _table
        self.deduplicate()
        self.is_distributed_table = False

    def merge_projection(self, projection_msg):
        if isinstance(projection_msg, ProjectionMsg):
            if self.table == projection_msg.table:
                self.select_items.extend(projection_msg.select_items)
                self.group_items.extend(projection_msg.group_items)
                self.deduplicate()

    def deduplicate(self):
        items_set = set()
        select_items = []
        for select_item in self.select_items:
            if select_item.upper().replace(' ', '') not in items_set:
                select_items.append(select_item)
                items_set.add(select_item.upper().replace(' ', ''))
        self.select_items = select_items
        items_set.clear()
        group_items = []
        for group_item in self.group_items:
            if group_item.upper().replace(' ', '') not in items_set:
                group_items.append(group_item)
                items_set.add(group_item.upper().replace(' ', ''))
        self.group_items = group_items

    def verify_query(self):
        table_columns = get_table_columns(self.table)
        if len(table_columns) == 0:
            return False
        group_items_set = set()
        for group_item in self.group_items:
            if '(' not in group_item:
                if group_item.strip() not in table_columns:
                    return False
                group_items_set.add(group_item.replace(' ', '').upper().strip())
        for select_item in self.select_items:
            if '(' not in select_item:
                if select_item.strip() not in table_columns or \
                        select_item.replace(' ', '').upper().strip() not in group_items_set:
                    return False
        return True


class ProjectionMsgGetter(object):
    """
        1、抹去参数型的子查询
        2、返回ProjectionMsg对象
    """

    def __init__(self, _query):
        _query = normalize_sql(_query)
        self.query = _query.replace(';', '')
        self.tokens = get_tokens(self.query)
        self.removed_tokens = []
        self.aggregate_functions = agg_functions
        self.suit_projection = True
        self.table = ""
        self.table_alias = ""
        self.table_columns = []
        self.alias_exp_tokens = {}

    def get_projection_msg(self):
        inner_queries_indexes = self.__get_inner_queries_indexes()
        # 抹去参数型子查询，括号不抹
        self.removed_tokens = []
        for i, _ in enumerate(self.tokens):
            if i not in inner_queries_indexes:
                self.removed_tokens.append(self.tokens[i])
        self.__parse_from_clause()
        projection_msg = None
        if not self.__in_clickhouse_tables():
            return projection_msg
        # 解析with，替换常量别名
        with_clause_msg = self.__parse_with_clause()
        select_items = self.__parse_select_clause(with_clause_msg)
        where_columns = self.__parse_where_clause(with_clause_msg)
        group_columns = self.__parse_group_clause(with_clause_msg)
        having_columns = self.__parse_having_clause(with_clause_msg)
        order_columns = self.__parse_order_clause(with_clause_msg)
        select_items.extend(where_columns)
        select_items.extend(having_columns)
        select_items.extend(order_columns)
        for item in select_items:
            if '(' not in item:
                group_columns.append(item)
        if self.suit_projection:
            projection_msg = ProjectionMsg(select_items, group_columns, self.table)
        return projection_msg

    def __in_clickhouse_tables(self):
        clickhouse_tables = get_clickhouse_tables()
        if 'all' not in clickhouse_tables:
            if not any(table == self.table for table in clickhouse_tables):
                return False
        return True

    def __parse_order_clause(self, with_clause_msg):
        order_items_tokens, order_items_exp = self.__get_order_items()
        order_items = self.__parse_clause_items(order_items_tokens, with_clause_msg)
        return order_items

    def __parse_group_clause(self, with_clause_msg):
        group_items_tokens, group_items_exp = self.__get_group_items()
        for item_exp in group_items_exp:
            for alias in self.alias_exp_tokens.keys():
                if f' {alias} ' in f' {item_exp} ':
                    group_items_tokens.append(self.alias_exp_tokens.get(alias))
        group_items = self.__parse_clause_items(group_items_tokens, with_clause_msg)
        if len(group_items) == 0:
            self.suit_projection = False
        return group_items

    def __parse_where_clause(self, with_clause_msg):
        where_items_tokens, where_items_exp = self.__get_where_items()
        where_items = self.__parse_clause_items(where_items_tokens, with_clause_msg)
        return where_items

    def __parse_having_clause(self, with_clause_msg):
        having_items_tokens, having_items_exp = self.__get_having_items()
        having_items = self.__parse_clause_items(having_items_tokens, with_clause_msg)
        return having_items

    def __parse_select_clause(self, with_clause_msg):
        select_items_tokens, select_items_exp = self.__get_select_items()
        for i, _ in enumerate(select_items_exp):
            item_exp = select_items_exp[i]
            alias_parser = AliasParser(item_exp)
            item_name, item_alias = alias_parser.get_name_alias()
            if item_alias != '':
                self.alias_exp_tokens[item_alias] = select_items_tokens[i]
        select_items = self.__parse_clause_items(select_items_tokens, with_clause_msg)
        return select_items

    def __parse_clause_items(self, items_tokens, with_clause_msg):
        clause_items = []
        for item_tokens in items_tokens:
            # 1.提取 sum(...),和残余表达式，所有子句共同使用以下处理逻辑
            inner_exps = self.__get_agg_and_remained_exps(item_tokens)
            for exp in inner_exps:
                exp_items = self.__parse_item_exp(exp, with_clause_msg)
                clause_items.extend(exp_items)
        return clause_items

    def __parse_item_exp(self, exp, with_clause_msg):
        exp_items = []
        if not self.__is_agg_func(exp):
            item_columns = self.__get_columns(exp)
            exp_items.extend(item_columns)
            return exp_items
        try_exp = exp.replace(' ', '').lower().replace('count()', '')
        if '()' not in try_exp.replace(' ', ''):
            exp = ' ' + exp + ' '
            # 表名及别名去掉(不认识表别名了)
            for column in self.table_columns:
                exp = exp.replace(f'{self.table}.{column}', column)
                exp = exp.replace(f'{self.table_alias}.{column}', column)
            # with中的别名处理
            for name in with_clause_msg.get('remove'):
                if f' {name} ' in exp:
                    self.suit_projection = False
                    OMNIMV_LOG.info('one sub-query have a agg-func which contains'
                                       ' sub-query-alias(because of with clause)')
            for alias in with_clause_msg.get('replace'):
                exp = (exp.replace(alias[0], alias[1])).strip()
            # 适配聚合函数表达式过长
            core_exp, condition_columns = self.__adapt_long_exp(exp)
            # 消一下别名
            if core_exp[-1] != ')':
                bracket_index = core_exp.rindex(')')
                core_exp = core_exp[0:bracket_index + 1]
            exp_items.append(core_exp.strip())
            exp_items.extend(condition_columns)
            return exp_items
        else:
            self.suit_projection = False
            OMNIMV_LOG.info('one sub-query have a agg-func contains an other sub-query')
            return exp_items

    def __get_agg_and_remained_exps(self, item_tokens):
        # 从一系列tokens中，将聚合函数表达式单独取出来，被挖掉聚合函数后的部分统一作为一个表达式
        # 返回值是[agg_exp,agg_exp,other_exp]
        agg_func_name = []
        for agg in self.aggregate_functions:
            agg_func_name.append(agg.replace('(', ''))
        agg_functions_exp = []
        agg_functions_indexes = []
        for i, _ in enumerate(item_tokens):
            token = item_tokens[i].strip().lower()
            if token not in agg_func_name or i + 1 >= len(item_tokens):
                continue
            next_token = item_tokens[i + 1].strip().lower()
            if next_token != '(':
                continue
            agg_indexes = []
            agg_tokens = []
            right_bracket_count = 0
            agg_end = len(item_tokens) - 1
            for j in range(i + 2, len(item_tokens)):
                if item_tokens[j] == '(':
                    right_bracket_count -= 1
                if item_tokens[j] == ')':
                    right_bracket_count += 1
                if right_bracket_count == 1:
                    agg_end = j
                    break
            for j in range(i, agg_end + 1):
                agg_indexes.append(j)
                agg_tokens.append(item_tokens[j])
            agg_exp = ' '.join(agg_tokens)
            agg_functions_exp.append(agg_exp)
            agg_functions_indexes.extend(agg_indexes)
        remained_tokens = []
        for i, _ in enumerate(item_tokens):
            if i not in agg_functions_indexes:
                remained_tokens.append(item_tokens[i])
        remained_exp = ' '.join(remained_tokens)
        agg_functions_exp.append(remained_exp)
        return agg_functions_exp

    def __adapt_long_exp(self, agg_exp):
        agg_exp = agg_exp.strip()
        core_exp = agg_exp
        condition_columns = []
        enable_conditional_funcs = get_ck_projection_with_conditional_functions()
        if enable_conditional_funcs:
            func_name = agg_exp[0:agg_exp.index('(')].strip()
            if "multi_if".replace('_', '') in agg_exp.lower():
                exp = agg_exp.split(',')[-2].strip()  # 被聚合字段，可能包了一层函数
                core_exp = f"{func_name} ( {exp} )".strip()  # 核心聚合表达式
                agg_exp = agg_exp.replace(exp, '')
                condition_columns = self.__get_columns(agg_exp)
            elif "case when" in agg_exp.lower():
                exp = agg_exp[
                      agg_exp.lower().index('then ') + 5:agg_exp.lower().index(' else ')]
                core_exp = f"{func_name} ( {exp} )".strip()
                agg_exp = agg_exp.replace(exp, '')
                condition_columns = self.__get_columns(agg_exp)
        else:
            if "multi_if".replace('_', '') in agg_exp.lower() or "case when" in agg_exp.lower():
                self.suit_projection = False
                OMNIMV_LOG.info('one sub-query have a agg-func contains long exp')
        return core_exp, condition_columns

    def __get_columns(self, item_exp):
        item_exp = ' ' + item_exp + ' '
        item_columns = []
        for column in self.table_columns:
            if f" {column} " in item_exp or f'.{column} ' in item_exp:
                item_columns.append(column)
        return item_columns

    def __is_agg_func(self, item_exp):
        for agg_func_name in self.aggregate_functions:
            if item_exp.replace(' ', '').lower().startswith(agg_func_name):
                return True
        return False

    def __parse_with_clause(self):
        # clickhouse的with子句必然会使用 as 或 AS 空格不行
        with_items_tokens, with_items_exp = self.__get_with_items()
        with_clause_msg = {'replace': [], 'remove': []}
        for item_exp in with_items_exp:
            item_exp = item_exp.replace(' as ', ' AS ')
            item_alias = item_exp.split(' AS ')[-1]
            item_exp = item_exp.split(' AS ')[0]
            if '()' not in item_alias.replace(' ', '').strip() and '()' not in item_exp.replace(' ', '').strip():
                # 未被掏掉子查询
                with_clause_msg.get('replace').append([item_alias, item_exp])
            else:
                if '()' not in item_alias.replace(' ', '').strip():
                    with_clause_msg.get('remove').append(item_alias)
                if '()' not in item_exp.replace(' ', '').strip():
                    with_clause_msg.get('remove').append(item_exp)
        return with_clause_msg

    def __parse_from_clause(self):
        end_signs = ['where', 'group by', 'having', 'order by', 'limit', 'limit by', 'into', 'format']
        from_items_tokens, from_items_exp = self.__get_clause_items('from', end_signs)
        # 包含 ，、join、select的from子句，在get_query_generation就已经被过滤了
        from_exp = from_items_exp[0].strip().replace(' as ', ' ').replace(' AS ', ' ')
        table_name = from_exp.split(' ')[0]
        table_alias = from_exp.split(' ')[-1]
        self.table = table_name
        self.table_alias = table_alias
        self.table_columns = get_table_columns(self.table)

    def __get_select_items(self):
        end_signs = ['from']
        select_items_tokens, select_items_exp = self.__get_clause_items('select', end_signs)
        return select_items_tokens, select_items_exp

    def __get_order_items(self):
        end_signs = ['limit', 'limit by', 'into', 'format']
        order_items_tokens, order_items_exp = self.__get_clause_items('order by', end_signs)
        return order_items_tokens, order_items_exp

    def __get_group_items(self):
        end_signs = ['having', 'order by', 'limit', 'limit by', 'into', 'format']
        group_items_tokens, group_items_exp = self.__get_clause_items('group by', end_signs)
        return group_items_tokens, group_items_exp

    def __get_where_items(self):
        end_signs = ['group by', 'having', 'order by', 'limit', 'limit by', 'into', 'format']
        where_items_tokens, where_exp = self.__get_clause_items('where', end_signs)
        return where_items_tokens, where_exp

    def __get_having_items(self):
        end_signs = ['order by', 'limit', 'limit by', 'into', 'format']
        having_items_tokens, having_exp = self.__get_clause_items('having', end_signs)
        return having_items_tokens, having_exp

    def __get_with_items(self):
        end_signs = ['select']
        with_items_tokens, with_items_exp = self.__get_clause_items('with', end_signs)
        return with_items_tokens, with_items_exp

    def __get_clause_items(self, clause, end_signs):
        target_index_getter = TargetIndexGetter(self.removed_tokens)
        index = target_index_getter.get_target_index(clause)
        if index == -1:
            return [], []
        end = target_index_getter.get_target_end_index(end_signs, index)
        clause_tokens = []
        for i in range(index + 1, end):
            clause_tokens.append(self.removed_tokens[i])
        target_index_getter = TargetIndexGetter(clause_tokens)
        commas_index = target_index_getter.get_commas_index()
        clause_items_tokens = []
        clause_items_exp = []
        for i in range(len(commas_index) - 1):
            start = commas_index[i] + 1  # [start,end)
            end = commas_index[i + 1]
            item_tokens = []
            for j in range(start, end):
                item_tokens.append(clause_tokens[j])
            clause_items_tokens.append(item_tokens)
            item_exp = ' '.join(item_tokens)
            clause_items_exp.append(item_exp)
        return clause_items_tokens, clause_items_exp

    def __get_inner_queries_indexes(self):
        search_start = 0
        all_sub_query_indexes = []
        while search_start < len(self.tokens):
            sub_query_indexes, sub_query_end = self.__get_inner_query(search_start)
            all_sub_query_indexes.extend(sub_query_indexes)
            search_start = sub_query_end + 1
        return all_sub_query_indexes

    def __get_inner_query(self, search_start):
        left_bracket_count = 0
        sub_query_start = -1
        for i in range(search_start, len(self.tokens)):
            if self.tokens[i] == '(':
                left_bracket_count += 1
            elif self.tokens[i] == ')':
                left_bracket_count -= 1
            elif self.tokens[i].lower() == 'select':
                if left_bracket_count > 0:
                    sub_query_start = i
                    break
        sub_query_end = len(self.tokens)
        if sub_query_start == -1:
            return [], sub_query_end
        right_bracket_count = 0
        for i in range(sub_query_start, len(self.tokens)):
            if self.tokens[i] == '(':
                right_bracket_count -= 1
            elif self.tokens[i] == ')':
                right_bracket_count += 1
                if right_bracket_count == 1:
                    sub_query_end = i
                    break
        sub_query_indexes = []
        if sub_query_start != -1:
            for i in range(sub_query_start, sub_query_end):
                sub_query_indexes.append(i)
        return sub_query_indexes, sub_query_end