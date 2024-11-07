#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from src.candidate_generation.common.projection_candidate_generation.generation_utils import TargetIndexGetter


def split_conditions_by_and(conditions_tokens):
    target_index_getter = TargetIndexGetter(conditions_tokens)
    and_index = target_index_getter.get_and_indexes()
    filter_conditions = []
    for i in range(len(and_index) - 1):
        condition_start = and_index[i] + 1
        condition_end = and_index[i + 1]
        condition_tokens = []
        for j in range(condition_start, condition_end):
            condition_tokens.append(conditions_tokens[j])
        condition_exp = ' '.join(condition_tokens).strip()
        filter_conditions.append(condition_exp)
    return filter_conditions


def normalize_conditions(tables_msg, conditions):
    normalized_conditions = []
    for condition in conditions:
        condition_normalizer = ConditionNormalizer(tables_msg, condition)
        condition_normalizer.normalize_condition()
        normalized_condition = condition_normalizer.condition.strip()
        normalized_conditions.append(normalized_condition)
    return normalized_conditions


class ConditionNormalizer(object):
    def __init__(self, tables_msg, condition):
        self.tables_msg = tables_msg
        self.condition = condition

    def normalize_condition(self):
        condition = ' ' + self.condition + ' '
        # 别名优化
        for table_msg in self.tables_msg:
            for column in table_msg.columns:
                if f' {table_msg.table_old_alias}.{column} ' in condition:
                    condition = condition.replace(f' {table_msg.table_old_alias}.{column} ',
                                                  f' {table_msg.table_new_alias}.{column} ')
                elif f' {column} ' in condition:
                    condition = condition.replace(f' {column} ', f' {table_msg.table_new_alias}.{column} ')
        self.condition = condition
        # 排序
        if self.is_table_join_condition():
            self.__sort_table_join_condition()
        self.condition = self.condition.strip()

    def is_table_join_condition(self):
        all_columns = []
        for table_msg in self.tables_msg:
            out_columns = table_msg.columns
            for column in out_columns:
                all_columns.append(column)

        operators = ['=', '>', '<', '<>', '>=', '<=']
        for operator in operators:
            if not (operator in self.condition and self.__without_other_operators(operator, operators)):
                return False
            if self.condition.rindex(operator) != self.condition.index(operator):
                return False

            left_of_equal = ' ' + self.condition.split(operator)[0] + ' '
            right_of_equal = ' ' + self.condition.split(operator)[-1] + ' '
            if bracket_is_legitimate(left_of_equal) and bracket_is_legitimate(right_of_equal):
                left_contain_column = False
                right_contain_column = False
                if any(f".{column} " in left_of_equal for column in all_columns):
                    left_contain_column = True
                if any(f".{column} " in right_of_equal for column in all_columns):
                    right_contain_column = True
                if left_contain_column and right_contain_column:
                    return True
        return False

    def get_columns(self):
        # self.condition已经标准化了，才能调用此函数
        columns = []
        for table_msg in self.tables_msg:
            for column in table_msg.columns:
                if f' {table_msg.table_new_alias}.{column} ' in self.condition:
                    columns.append({'item_name': f' {table_msg.table_new_alias}.{column} '})
        return columns

    def __without_other_operators(self, operator, operators):
        # 功能：用于排除 <=对=识别的干扰
        if len(operator) == 1:  # operator是 = > 或 <
            operator_index = self.condition.index(operator)
            if self.condition[operator_index + 1] in operators or self.condition[operator_index - 1] in operators:
                return False
            else:
                return True
        elif len(operator) == 2:
            operator_index = self.condition.index(operator)
            if '=' in self.condition and abs(self.condition.index('=') - operator_index) > 1:
                return False
            if '>' in self.condition and abs(self.condition.index('>') - operator_index) > 1:
                return False
            if '<' in self.condition and abs(self.condition.index('<') - operator_index) > 1:
                return False
            return True
        else:
            return False

    def __sort_table_join_condition(self):
        operators = ['=', '>', '<', '<>', '>=', '<=']
        for operator in operators:
            if not (operator in self.condition and self.__without_other_operators(operator, operators)):
                return
            left = self.condition.split(operator)[0].strip()
            right = self.condition.split(operator)[1].strip()
            if len(left) > len(right):
                self.condition = f"{left} {operator} {right}"
                return
            elif len(left) < len(right):
                self.condition = f"{right} {operator} {left}"
                return
            for i, _ in enumerate(left):
                if ord(left[i]) > ord(right[i]):
                    self.condition = f"{left} {operator} {right}"
                    break
                elif ord(left[i]) < ord(right[i]):
                    self.condition = f"{right} {operator} {left}"
                    break


def bracket_is_legitimate(condition_exp):
    my_stack = []
    for char in condition_exp:
        if char == '(':
            my_stack.append('(')
        elif char == ')':
            if len(my_stack) == 0:
                return False
            my_stack.pop()
    return len(my_stack) == 0