#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils \
    import get_tokens, TargetIndexGetter, normalize_sql, QueryClauseGetter


class TargetQueryGetter(object):
    def __init__(self, _query, _table=''):
        _query = normalize_sql(_query)
        self.query = _query.replace(';', '')
        self.tokens = get_tokens(self.query)
        self.table = _table  # 重点表

    def queries_for_projection(self):
        union_queries = self.split_union_query()
        target_queries = []
        if len(union_queries) > 1:
            for query in union_queries:
                target_query_getter = TargetQueryGetter(query, self.table)
                target_queries.extend(target_query_getter.queries_for_projection())
            return target_queries

        if self.from_clause_qualified():
            target_queries.append(self.query)
        else:
            if self.contain_inner_query():
                inner_queries = self.get_inner_queries()
                for query in inner_queries:
                    target_query_getter = TargetQueryGetter(query, self.table)
                    target_queries.extend(target_query_getter.queries_for_projection())
        return target_queries

    def queries_for_mv(self):
        union_queries = self.split_union_query()
        target_queries = []
        if len(union_queries) > 1:
            for query in union_queries:
                target_query_getter = TargetQueryGetter(query)
                target_queries.extend(target_query_getter.queries_for_mv())
            return target_queries
        self.remove_useless_clause()

        if not self.contain_inner_query():
            target_queries.append(self.query)
        else:
            if self.contain_non_parameter_sub_query():
                inner_queries = self.get_inner_queries()
                for query in inner_queries:
                    target_query_getter = TargetQueryGetter(query)
                    target_queries.extend(target_query_getter.queries_for_mv())
            else:
                query = self.remove_parameter_sub_query()
                target_queries.append(query)
        return target_queries

    def contain_inner_query(self):
        left_bracket_count = 0
        for i, _ in enumerate(self.tokens):
            if self.tokens[i].lower().strip() == '(':
                left_bracket_count += 1
            elif self.tokens[i].lower().strip() == ')':
                left_bracket_count -= 1
            elif self.tokens[i].lower().strip() == 'select':
                if left_bracket_count > 0:
                    return True
        return False

    def from_clause_qualified(self):
        from_tokens = self.get_from_tokens()
        from_exp = ' '.join(from_tokens).strip().lower()
        key_words = [' join ', ' select ', ',']
        if len(from_exp) != 0 and all(key_word not in from_exp for key_word in key_words):
            if f' {self.table.lower()} ' in f' {from_exp} ' or f'.{self.table.lower()} ' in f' {from_exp} ':
                return True
        return False

    def get_from_tokens(self):
        target_index_getter = TargetIndexGetter(self.tokens)
        from_index = target_index_getter.get_target_index('from')
        end_signs = ['where', 'group by', 'having', 'order by', 'limit', 'union', 'into', 'format', ';']
        from_end_index = target_index_getter.get_target_end_index(end_signs, from_index)
        from_tokens = []
        for i in range(from_index + 1, from_end_index):
            from_tokens.append(self.tokens[i])
        return from_tokens

    def split_union_query(self):
        target_index_getter = TargetIndexGetter(self.tokens)
        select_indexes = []
        start = 0
        while True:
            select_index = target_index_getter.get_target_index('select', start)
            if select_index == -1:
                break
            else:
                select_indexes.append(select_index)
                start = select_index + 1
        select_indexes.append(len(self.tokens) + 1)
        union_queries = []
        for i in range(len(select_indexes) - 1):
            union_query_tokens = []
            for j in range(select_indexes[i], select_indexes[i + 1] - 1):
                union_query_tokens.append(self.tokens[j])
            union_queries.append(' '.join(union_query_tokens))
        return union_queries

    def get_inner_queries(self):
        search_start = 0
        all_sub_query = []
        while search_start < len(self.tokens):
            sub_query_tokens, sub_query_end = self.get_inner_query(search_start)
            sub_query_exp = " ".join(sub_query_tokens)
            if sub_query_tokens:
                all_sub_query.append(sub_query_exp + ' ')
            search_start = sub_query_end + 1
        return all_sub_query

    def get_inner_query(self, search_start):
        # 找到sql的所有第一层子查询
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
        sub_query_tokens = []
        if sub_query_start != -1:
            for i in range(sub_query_start, sub_query_end):
                sub_query_tokens.append(self.tokens[i])
        return sub_query_tokens, sub_query_end

    def remove_useless_clause(self):
        target_index_getter = TargetIndexGetter(self.tokens)
        group_by_index = target_index_getter.get_target_index('group by')
        if group_by_index != -1:
            group_end_signs = ['having', 'order by', 'limit', ';']
            group_by_end = target_index_getter.get_target_end_index(group_end_signs, group_by_index)
            remove_start = group_by_end
        else:
            where_index = target_index_getter.get_target_index('where')
            if where_index != -1:
                where_end_signs = ['group by', 'having', 'order by', 'limit', ';']
                where_end = target_index_getter.get_target_end_index(where_end_signs, where_index)
                remove_start = where_end
            else:
                from_index = target_index_getter.get_target_index('from')
                if from_index == -1:
                    return
                from_end_signs = ['where', 'group by', 'having', 'order by', 'limit', ';']
                from_end = target_index_getter.get_target_end_index(from_end_signs, from_index)
                remove_start = from_end

        new_tokens = []
        for i, _ in enumerate(self.tokens):
            if i < remove_start:
                new_tokens.append(self.tokens[i])
        sql_exp = ' '.join(new_tokens)
        self.query = sql_exp
        self.tokens = get_tokens(self.query)

    def contain_non_parameter_sub_query(self):
        """
            判断是否包含非参数型子查询
        """
        query_clause_getter = QueryClauseGetter(self.tokens)
        with_clause_tokens = query_clause_getter.get_with_clause_tokens()
        from_clause_tokens = query_clause_getter.get_from_clause_tokens()
        from_clause_exp = ''
        for item_tokens in from_clause_tokens:
            from_clause_exp += ' '.join(item_tokens) + ' '
        with_clause_exp = ''
        for item_tokens in with_clause_tokens:
            with_clause_exp += ' '.join(item_tokens) + ' '
        if ' select ' in with_clause_exp.lower() or ' select ' in from_clause_exp.lower():
            return True
        return False

    def remove_parameter_sub_query(self):
        target_index_getter = TargetIndexGetter(self.tokens)
        where_index = target_index_getter.get_target_index('where')
        end_signs = ['group by', 'having', 'order by', 'limit', ';']
        where_end = target_index_getter.get_target_end_index(end_signs, where_index)

        is_indexes = []
        sub_queries_indexes = []
        index = where_index
        while index < where_end:
            if self.tokens[index].lower() == 'select':
                left_bracket_index = index - 1
                if self.tokens[left_bracket_index] != '(':
                    return ''
                right_bracket_index = self.get_right_bracket_index(left_bracket_index)
                is_indexes.append(left_bracket_index - 1)
                if left_bracket_index < 1 or right_bracket_index == -1:
                    return ''
                for i in range(left_bracket_index, right_bracket_index + 1):
                    sub_queries_indexes.append(i)
                index = right_bracket_index + 1
            else:
                index += 1
        removed_tokens = []
        sub_queries_tokens = []
        for i, token in enumerate(self.tokens):
            if i in is_indexes:
                removed_tokens.append('is')
                removed_tokens.append('parameter_sub_query')
            elif i not in sub_queries_indexes:
                removed_tokens.append(token)
            else:
                sub_queries_tokens.append(token)
        removed_query_exp = ' '.join(removed_tokens)
        return removed_query_exp

    def get_right_bracket_index(self, left_bracket_index):
        left_bracket_count = 0
        right_bracket_index = -1
        for i, token in enumerate(self.tokens):
            if i < left_bracket_index:
                continue
            if token == '(':
                left_bracket_count += 1
            elif token == ')':
                left_bracket_count -= 1
                if left_bracket_count == 0:
                    right_bracket_index = i
                    break
        return right_bracket_index


def get_inner_dependence(main_query_tokens, sub_queries_tokens):
    # 获取子查询对外部查询的依赖，此情况极少。待细化
    return []