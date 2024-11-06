#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import sys

from src.candidate_generation.common.materialize_view_generation.join_msg import TableMsg
from src.candidate_generation.common.materialize_view_generation.parse_condition import normalize_conditions, \
    ConditionNormalizer
from src.candidate_generation.common.materialize_view_generation.parse_view import ViewParser
from src.candidate_generation.common.projection_candidate_generation.generation_utils import get_tokens, \
    get_table_columns
from src.preprocessor.metadata import get_spark_table_columns

class NormalizedView(object):
    def __init__(self, select_items, group_by_items, filter_conditions,
                 join_conditions, join_msgs, sources, tables_msg):
        self.select_items = select_items
        self.group_by_items = group_by_items
        self.filter_conditions = filter_conditions
        self.join_conditions = join_conditions
        self.join_msgs = join_msgs
        self.sources = sources
        self.tables_msg = tables_msg

    def __str__(self):
        select_items_exp = []
        for select_item in self.select_items:
            select_item_exp = f"{select_item.get('item_name')} {select_item.get('item_alias')}"
            select_items_exp.append(select_item_exp)
        select_items_exp = self.deduplicate(select_items_exp)
        select_exp = '\nSELECT ' + ' , '.join(select_items_exp) + '\n'

        join_msgs_exp = []
        for join_msg in self.join_msgs:
            table_name = str(join_msg.head_table)
            table_alias = join_msg.head_table_alias
            join_msg_exp = table_name + ' ' + table_alias + ' '
            for joined_table_msg in join_msg.joined_tables_msg:
                joined_table_exp = ' ' + joined_table_msg.join_type + ' '
                joined_table_exp += ' ' + joined_table_msg.table_name + ' '
                joined_table_exp += ' ' + joined_table_msg.table_alias + ' ON '
                conditions_exp = ' AND '.join(joined_table_msg.join_conditions)
                joined_table_exp += ' ' + conditions_exp + ' '
                join_msg_exp += ' ' + joined_table_exp.strip() + ' '
            join_msgs_exp.append(join_msg_exp)
        from_exp = ' FROM ' + ' , '.join(join_msgs_exp) + '\n'

        where_exp = ''
        if len(self.join_conditions) != 0 or len(self.filter_conditions) != 0:
            join_conditions_exp = ' AND '.join(self.join_conditions)
            filter_conditions_exp = ' AND '.join(self.filter_conditions)
            if len(join_conditions_exp) == 0:
                where_exp = filter_conditions_exp + '\n'
            elif len(filter_conditions_exp) == 0:
                where_exp = join_conditions_exp + '\n'
            else:
                where_exp = f'{join_conditions_exp} AND ( {filter_conditions_exp} )' + '\n'

        group_exp = ''
        self.group_by_items = self.deduplicate(self.group_by_items)
        if len(self.group_by_items) != 0:
            group_exp = ' , '.join(self.group_by_items) + '\n'

        view_exp = select_exp + from_exp
        if len(where_exp.strip()) != 0:
            view_exp += ' WHERE ' + where_exp.strip() + '\n'
        if len(group_exp.strip()) != 0:
            view_exp += ' GROUP BY ' + group_exp.strip() + '\n'
        return view_exp + ';\n'

    def append_source(self, sources):
        self.sources.extend(sources)

    def could_union(self, normalized_view):
        if len(self.join_msgs) == len(normalized_view.join_msgs) \
                and len(self.join_conditions) == len(normalized_view.join_conditions):
            # 同时有group by或同时没有 & where中的表连接条件相同 & from中的','数量相同
            for i, _ in enumerate(self.join_msgs):
                join_msg_1 = self.join_msgs[i]
                join_msg_2 = normalized_view.join_msgs[i]
                if not join_msg_1.equal(join_msg_2):
                    return False  # from必须相同
            join_conditions_set_1 = set()
            join_conditions_set_2 = set()
            for condition in self.join_conditions:
                join_conditions_set_1.add(condition.replace(' ', '').upper())
            for condition in normalized_view.join_conditions:
                join_conditions_set_2.add(condition.replace(' ', '').upper())
            if join_conditions_set_1 == join_conditions_set_2:  # where中的表连接条件必须相同
                select_exp_1 = ','.join(item.get('item_name') for item in self.select_items)
                select_exp_2 = ','.join(item.get('item_name') for item in normalized_view.select_items)
                return self.help_could_join(select_exp_1, select_exp_2, normalized_view)
        return False

    def union(self, normalized_view):
        if not self.could_union(normalized_view):
            return
        if str(self) == str(normalized_view):
            return
        self.sources.extend(normalized_view.sources)
        filter_condition_1 = ' AND '.join(normalized_view.filter_conditions)
        filter_condition_2 = ' AND '.join(self.filter_conditions)
        if filter_condition_1.strip() != '' and filter_condition_2.strip() != '':
            filter_condition = f'( {filter_condition_2} ) OR ( {filter_condition_1} )'
        elif filter_condition_1.strip() != '' and filter_condition_2.strip() == '':
            filter_condition = f'( {filter_condition_1} )'
        elif filter_condition_1.strip() == '' and filter_condition_2.strip() != '':
            filter_condition = f'( {filter_condition_2} )'
        else:
            filter_condition = f''

        self.filter_conditions = [filter_condition]
        # filter_condition里涉及到的所有列，需要加入到select与group by里，如果没有group by，加入到select里就行了。
        filter_condition_columns = []
        for table_msg in self.tables_msg:
            for column in table_msg.columns:
                if f'{table_msg.table_new_alias}.{column} ' in filter_condition:
                    filter_condition_columns.append(f'{table_msg.table_new_alias}.{column}')

        normalized_view_select_items = []
        for item in normalized_view.select_items:
            normalized_view_select_items.append(item.get('item_name'))
        self.select_items_append(normalized_view_select_items)
        self.group_by_items_append(normalized_view.group_by_items)
        self.select_items_append(filter_condition_columns)
        if len(self.group_by_items) > 0:
            self.group_by_items_append(filter_condition_columns)
        else:
            # 能合并，且都没有group by => 纯select agg union 纯select agg、纯select字段 union 纯select字段
            select_exp_1 = ','.join(item.get('item_name') for item in self.select_items)
            select_exp_2 = ','.join(item.get('item_name') for item in normalized_view.select_items)
            agg_function = [
                'avg (', 'avg(', 'sum (', 'sum(', 'count (',
                'count(', 'max (', 'max(', 'min (', 'min('
            ]
            if any(agg in select_exp_1.lower() for agg in agg_function) \
                    and any(agg in select_exp_2.lower() for agg in agg_function):  # self只查agg，other也只查agg
                self.group_by_items_append(filter_condition_columns)

            if any(agg in select_exp_1.lower() for agg in agg_function) \
                    and len(normalized_view.group_by_items) > 0:  # self只查agg,other带group by
                self.group_by_items_append(filter_condition_columns)

    def select_items_append(self, items):
        select_items = []
        select_items_set = set()
        for select_item in self.select_items:
            item_name = select_item.get('item_name')
            select_items.append(item_name)
            select_items_set.add(item_name.replace(' ', '').upper())
        for item in items:
            if item.replace(' ', '').upper() not in select_items_set:
                select_items.append(item)
                select_items_set.add(item.replace(' ', '').upper())
        self.select_items = []
        item_count = 0
        for item in select_items:
            self.select_items.append({'item_name': item, 'item_alias': f'column{item_count}'})
            item_count += 1

    def group_by_items_append(self, items):
        group_by_items_set = set()
        for item in self.group_by_items:
            group_by_items_set.add(item.replace(' ', '').upper())
        for item in items:
            if item.replace(' ', '').upper() not in group_by_items_set:
                self.group_by_items.append(item)
                group_by_items_set.add(item.replace(' ', '').upper())

    def help_could_join(self, select_exp_1, select_exp_2, normalized_view):
        agg_function = [
            'avg (', 'avg(', 'sum (', 'sum(', 'count (',
            'count(', 'max (', 'max(', 'min (', 'min('
        ]
        if len(self.group_by_items) == 0 and len(normalized_view.group_by_items) == 0:
            # 在两个都没有group 的情况下，只有两者都只查询聚合函数才能合并
            if any(agg in select_exp_1.lower() for agg in agg_function) \
                    and any(agg in select_exp_2.lower() for agg in agg_function):
                return True
            elif all(agg not in select_exp_1.lower() for agg in agg_function) and \
                    all(agg not in select_exp_2.lower() for agg in agg_function):
                return True
            else:
                return False
        elif len(self.group_by_items) > 0 and len(normalized_view.group_by_items) > 0:
            return True
        elif len(self.group_by_items) == 0 and len(normalized_view.group_by_items) > 0:
            if any(agg in select_exp_1.lower() for agg in agg_function):
                return True
        else:
            if any(agg in select_exp_2.lower() for agg in agg_function):
                return True
        return False

    def deduplicate(self, items_exp):
        items_exp_set = set()
        deduplicated_items_exp = []
        for item_exp in items_exp:
            if item_exp.replace(' ', '').strip().upper() not in items_exp_set:
                deduplicated_items_exp.append(item_exp)
                items_exp_set.add(item_exp.replace(' ', '').strip().upper())
        return deduplicated_items_exp


class ViewNormalizer(object):
    def __init__(self, original_view, sources):
        self.original_view = original_view
        self.original_view_tokens = get_tokens(original_view)
        self.tables_msg = []
        self.join_msgs = None
        self.select_items = None
        self.conditions = None
        self.group_items = None
        self.sources = sources  # 来自哪条sql

    def get_normalized_view(self):
        view_parser = ViewParser(self.original_view)
        parse_result = view_parser.parse()
        self.join_msgs = parse_result[0]
        self.select_items = parse_result[1]
        self.conditions = parse_result[2]
        self.group_items = parse_result[3]
        self.__normalize_join_msgs()
        filter_conditions, join_conditions, parameter_query_columns = self.__normalize_conditions()
        self.__normalize_select_items(parameter_query_columns)
        self.__normalize_group_items(parameter_query_columns)
        normalized_view = NormalizedView(self.select_items, self.group_items, filter_conditions,
                                         join_conditions, self.join_msgs, self.sources, self.tables_msg)
        return normalized_view

    def __normalize_group_items(self, parameter_query_columns):
        for item in parameter_query_columns:
            self.group_items.append(item.get('item_name'))
        if len(self.group_items) == 0:
            return
        items_name = []
        for group_item in self.group_items:
            group_item = ' ' + group_item + ' '
            group_item = self.__replace_old_alias(group_item)
            items_name.append(group_item)
        self.group_items = items_name

    def __normalize_select_items(self, parameter_query_columns):
        items_name = []
        self.select_items.extend(parameter_query_columns)
        for select_item in self.select_items:
            item_name = ' ' + select_item.get('item_name') + ' '
            item_name = self.__replace_old_alias(item_name)
            items_name.append(item_name.strip())
            if item_name.lower().startswith('avg') and ('avg( ' in item_name.lower() or 'avg (' in item_name.lower()):
                func_core = item_name[item_name.index('('):item_name.rindex(')') + 1]
                items_name.append(f'count {func_core}')
        self.select_items = []
        item_id = 0
        for item_name in items_name:
            self.select_items.append({'item_name': item_name, 'item_alias': f'column{item_id}'})
            item_id += 1

    def __replace_old_alias(self, item_exp):
        for table_msg in self.tables_msg:
            for column in table_msg.columns:
                if f' {table_msg.table_old_alias}.{column} ' in item_exp:
                    item_exp = item_exp.replace(f' {table_msg.table_old_alias}.{column} ',
                                                f' {table_msg.table_new_alias}.{column} ')
                elif f' {column} ' in item_exp:
                    item_exp = item_exp.replace(f' {column} ', f' {table_msg.table_new_alias}.{column} ')
        return item_exp.strip()

    def __normalize_conditions(self):
        normalized_conditions = normalize_conditions(self.tables_msg, self.conditions)
        filter_conditions = []
        join_conditions = []
        parameter_query_columns = []
        for condition in normalized_conditions:
            condition_normalizer = ConditionNormalizer(self.tables_msg, condition)
            if 'is parameter_sub_query' in condition:
                condition_columns = condition_normalizer.get_columns()
                parameter_query_columns.extend(condition_columns)
            elif condition_normalizer.is_table_join_condition():
                join_conditions.append(condition)
            else:
                filter_conditions.append(condition)
        return filter_conditions, join_conditions, parameter_query_columns

    def __normalize_join_msgs(self):
        table_id = 1
        for join_msg in self.join_msgs:
            # normalize first table
            table_name = join_msg.head_table
            table_old_alias = join_msg.head_table_alias
            table_new_alias = f'table_{table_id}'
            table_id += 1
            join_msg.head_table_alias = table_new_alias
            if table_old_alias == '':
                table_old_alias = table_name
            out_columns = get_table_out_columns(join_msg.head_table)
            table_msg = TableMsg(join_msg.head_table, table_old_alias, table_new_alias, out_columns)
            self.tables_msg.append(table_msg)
            # normalize follow table
            for joined_table_msg in join_msg.joined_tables_msg:
                table_name = joined_table_msg.table_name
                table_old_alias = joined_table_msg.table_alias
                table_new_alias = f'table_{table_id}'
                table_id += 1
                joined_table_msg.table_alias = table_new_alias
                joined_table_msg.join_type = normalize_join_type(joined_table_msg.join_type)

                if table_old_alias == '':
                    table_old_alias = table_name

                out_columns = get_table_out_columns(joined_table_msg.table_name)
                table_msg = TableMsg(joined_table_msg.table_name, table_old_alias, table_new_alias, out_columns)
                self.tables_msg.append(table_msg)
                # on 的条件标准化
                normalized_join_conditions = normalize_conditions([self.tables_msg[0], table_msg],
                                                                  joined_table_msg.join_conditions)
                joined_table_msg.join_conditions = normalized_join_conditions


def get_table_out_columns(table_name):
    out_columns = []
    table_columns = get_spark_table_columns(table_name)
    for column in table_columns:
        out_columns.append(column)
    return out_columns


def normalize_join_type(join_type):
    # INNER   JOIN中间多个空格,获取的token也有多个空格
    words = join_type.split(' ')
    key_words = []
    for word in words:
        if word != '':
            key_words.append(word.upper())
    upper_join_type = ' '.join(key_words)
    ck_join_type = {
        'JOIN': 'INNER JOIN',
        'INNER JOIN': 'INNER JOIN',
        'LEFT JOIN': 'LEFT OUTER JOIN',
        'LEFT OUTER JOIN': 'LEFT OUTER JOIN',
        'RIGHT JOIN': 'RIGHT OUTER JOIN',
        'RIGHT OUTER JOIN': 'RIGHT OUTER JOIN',
        'FULL JOIN': 'FULL OUTER JOIN',
        'FULL OUTER JOIN': 'FULL OUTER JOIN',
        'CROSS JOIN': 'CROSS JOIN',
        'SEMI JOIN': 'LEFT SEMI JOIN',
        'LEFT SEMI JOIN': 'LEFT SEMI JOIN',
        'ANTI JOIN': 'LEFT ANTI JOIN',
        'LEFT ANTI JOIN': 'LEFT ANTI JOIN',
        'RIGHT SEMI JOIN': 'RIGHT SEMI JOIN',
        'RIGHT ANTI JOIN': 'RIGHT ANTI JOIN',
        'LEFT ANY JOIN': 'LEFT ANY JOIN',
        'RIGHT ANY JOIN': 'RIGHT ANY JOIN',
        'INNER ANY JOIN': 'INNER ANY JOIN',
        'ASOF JOIN': 'ASOF JOIN',
        'LEFT ASOF JOIN': 'LEFT ASOF JOIN'
    }
    normalized_join_type = ck_join_type.get(upper_join_type)
    if normalized_join_type is None:
        return upper_join_type
    else:
        return normalized_join_type