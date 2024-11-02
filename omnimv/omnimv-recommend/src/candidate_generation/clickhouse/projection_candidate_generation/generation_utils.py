#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from sql_metadata import Parser

from src.preprocessor.metadata import TableSchema

query_tokens_cache = {}


def get_tokens(query):
    query_no_space = query.replace(' ', '')
    if query_no_space not in query_tokens_cache.keys():
        parser = Parser(query)
        query_tokens = []
        for token in parser.tokens:
            query_tokens.append(str(token.value))
        query_tokens_cache[query_no_space] = query_tokens
    return query_tokens_cache.get(query_no_space)


def get_table_columns(table):
    table_metadata = TableSchema()
    table_columns = []
    for schema_table_name in table_metadata.data:
        if table.upper() == schema_table_name.split('.')[-1]:
            table_columns_map = table_metadata.data.get(schema_table_name).get('columns')
            for column, _ in table_columns_map.items():
                table_columns.append(column.strip())
    return table_columns


def get_local_table(distributed_table):
    table_metadata = TableSchema()
    for schema_table_name in table_metadata.data:
        if distributed_table.upper() == schema_table_name.split('.')[-1]:
            table_msg = table_metadata.data.get(schema_table_name)
            if 'engine_info' in table_msg.keys():
                return table_msg.get('engine_info').get('local_table')
    return ''


def get_table_name(file_name):
    # file name命名是 table_name+_id.sql 或者 table_name + _id + _incremental.sql
    file_name = file_name.replace('_incremental.sql', '')
    table_name = file_name[0:file_name.rfind('_')]
    return table_name
def remove_special_characters(exp):
    # 去除 " ` 等特殊字符
    quotes_index = []
    special_quotes_index = []
    for i, c in enumerate(exp):
        if c == "\"":
            quotes_index.append(i)
        if c == "`":
            special_quotes_index.append(i)
    for i in range(len(quotes_index) - 1):
        if i % 2 != 0:
            continue
        quote_start = quotes_index[i]
        quote_end = quotes_index[i + 1]
        inner_exp = exp[quote_start + 1:quote_end]
        legitimate_exp = get_legitimate_name(inner_exp)
        exp = exp.replace(inner_exp, legitimate_exp)
    for i in range(len(special_quotes_index) - 1):
        if i % 2 != 0:
            continue
        special_quote_start = special_quotes_index[i]
        special_quote_end = special_quotes_index[i + 1]
        inner_exp = exp[special_quote_start + 1:special_quote_end]
        legitimate_exp = get_legitimate_name(inner_exp)
        exp = exp.replace(inner_exp, legitimate_exp)
    exp = exp.replace("\"", "").replace("`", "")
    return exp


def get_legitimate_name(name):
    legitimate_name_list = []
    for c in name:
        if c == "_" or c.isdigit() or c.isalpha():
            legitimate_name_list.append(c)
        else:
            legitimate_name_list.append('_')
    legitimate_name = ''.join(legitimate_name_list)
    return legitimate_name


class TargetIndexGetter(object):
    """
        用于在tokens中寻找，不在括号中的目标表达式的下标
    """

    def __init__(self, _tokens):
        self.tokens = _tokens

    def get_target_index(self, target, index=0):
        """
            从tokens中获得，首次出现的、不在括号内的 target 的下标。包括index
        """
        left_bracket_count = 0
        for i in range(index, len(self.tokens)):
            if self.tokens[i].lower().strip() == '(':
                left_bracket_count += 1
            elif self.tokens[i].lower().strip() == ')':
                left_bracket_count -= 1
            elif self.tokens[i].lower().strip() == target:
                if left_bracket_count == 0:
                    return i
        return -1

    def get_target_end_index(self, end_signs, index=0):
        """
            从view_tokens里获得 index之后的、第一个不在括号内的 结束标志 的下标。
            如果不存在结束标志，则数组越界处为结束处。不包括index
        """
        left_bracket_count = 0
        for i in range(index + 1, len(self.tokens)):
            if self.tokens[i].lower().strip() == '(':
                left_bracket_count += 1
            elif self.tokens[i].lower().strip() == ')':
                left_bracket_count -= 1
            elif self.tokens[i].lower().strip() in end_signs:
                if left_bracket_count == 0:
                    return i
        return len(self.tokens)

    def get_commas_index(self):
        """
            返回tokens中不在括号里的 逗号 的下标，并为了方便使用，
            首位分别添加了 -1和len(self.tokens)
        """
        left_bracket_count = 0
        commas_index = [-1]
        for i, _ in enumerate(self.tokens):
            if self.tokens[i] == '(':
                left_bracket_count += 1
            elif self.tokens[i] == ')':
                left_bracket_count -= 1
            elif self.tokens[i].lower() == ',':
                if left_bracket_count == 0:
                    commas_index.append(i)
        commas_index.append(len(self.tokens))
        return commas_index

    def get_target_indexes(self, target):
        target_indexes = []
        left_bracket_count = 0
        for i, _ in enumerate(self.tokens):
            if self.tokens[i] == '(':
                left_bracket_count += 1
            elif self.tokens[i] == ')':
                left_bracket_count -= 1
            elif self.tokens[i].lower() == target:
                if left_bracket_count == 0:
                    target_indexes.append(i)
        return target_indexes

    def get_and_indexes(self):
        # 1.找到与between匹配的and
        between_exist = False
        between_and_index = []
        for i, _ in enumerate(self.tokens):
            if self.tokens[i].lower() == 'between':
                between_exist = True
            if self.tokens[i].lower() == 'and':
                if between_exist:
                    between_exist = False
                    between_and_index.append(i)
        # 2.越过最外层的括号，不一定有最外层的括号
        start = 0
        for i, _ in enumerate(self.tokens):
            if self.tokens[i].strip() != '(':
                start = i
                break
        # 3.找到不在括号里的 and
        and_index = [-1]
        left_bracket_count = 0
        for i, _ in enumerate(self.tokens):
            if i < start or i in between_and_index:
                continue
            if self.tokens[i].lower() == '(':
                left_bracket_count += 1
            elif self.tokens[i].lower() == ')':
                left_bracket_count -= 1
            elif self.tokens[i].lower() == 'and':
                if left_bracket_count == 0:
                    and_index.append(i)
        and_index.append(len(self.tokens))
        return and_index


def normalize_sql(sql):
    sql_tokens = get_tokens(sql)
    normalize_tokens = []
    for token in sql_tokens:
        if token.startswith('/*') and token.endswith('*/'):
            continue
        if token.startswith('--'):
            continue
        normalize_tokens.append(token)
    normalized_sql = ' '.join(normalize_tokens)
    return normalized_sql


class QueryClauseGetter(object):
    """
        功能：用于获取查询语句的子句，以token列表返回。
            若含子查询，则子查询也属于所属子句的一部分，子查询内含在token列表中返回。
            select子句这种有','的，按','分割返回[[],[]...]
            where这种没有','的，返回[[]]
    """

    def __init__(self, _query_tokens):
        self.query_tokens = _query_tokens

    def get_with_clause_tokens(self):
        clause_name = 'with'
        clause_end_signs = ['select']
        with_clause_tokens = self.__get_clause_tokens(clause_name, clause_end_signs)
        return with_clause_tokens

    def get_select_clause_tokens(self):
        clause_name = 'select'
        clause_end_signs = ['from', ';']
        select_clause_tokens = self.__get_clause_tokens(clause_name, clause_end_signs)
        return select_clause_tokens

    def get_from_clause_tokens(self):
        clause_name = 'from'
        clause_end_signs = ['where', 'group by', 'having', 'order by', 'limit', ';']
        from_clause_tokens = self.__get_clause_tokens(clause_name, clause_end_signs)
        return from_clause_tokens

    def get_where_clause_tokens(self):
        clause_name = 'where'
        clause_end_signs = ['group by', 'having', 'order by', 'limit', ';']
        where_clause_tokens = self.__get_clause_tokens(clause_name, clause_end_signs)
        return where_clause_tokens

    def get_group_by_clause_tokens(self):
        clause_name = 'group by'
        clause_end_signs = ['having', 'order by', 'limit', ';']
        group_by_clause_tokens = self.__get_clause_tokens(clause_name, clause_end_signs)
        return group_by_clause_tokens

    def get_having_clause_tokens(self):
        clause_name = 'having'
        clause_end_signs = ['order by', 'limit', ';']
        having_clause_tokens = self.__get_clause_tokens(clause_name, clause_end_signs)
        return having_clause_tokens

    def get_order_by_tokens(self):
        clause_name = 'order by'
        clause_end_signs = ['limit', ';']
        order_by_clause_tokens = self.__get_clause_tokens(clause_name, clause_end_signs)
        return order_by_clause_tokens

    def __get_clause_tokens(self, clause_name, end_signs):
        target_index_getter = TargetIndexGetter(self.query_tokens)
        clause_index = target_index_getter.get_target_index(clause_name)
        clause_end = target_index_getter.get_target_end_index(end_signs, clause_index)
        clause_tokens = self.__get_clause_items(clause_index, clause_end)
        return clause_tokens

    def __get_clause_items(self, clause_start, clause_end):
        clause_tokens = []
        for i in range(clause_start + 1, clause_end):
            clause_tokens.append(self.query_tokens[i])
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