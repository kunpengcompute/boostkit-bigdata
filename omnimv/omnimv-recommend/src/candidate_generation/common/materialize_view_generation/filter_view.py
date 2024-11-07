#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from src.candidate_generation.common.projection_candidate_generation.generation_utils import get_tokens, \
    TargetIndexGetter


class ViewFilter(object):
    def __init__(self, sub_query):
        self.sub_query_tokens = get_tokens(sub_query)
        self.sub_query = ' '.join(self.sub_query_tokens)

    def get_view(self):
        if self.contain_multi_table():
            return self.sub_query
        else:
            return ''

    def contain_multi_table(self):
        from_tokens = self.get_from_tokens()
        from_exp = ' '.join(from_tokens).strip()
        if len(from_exp) != 0 and (' join ' in from_exp.lower() or ',' in from_exp.lower()):
            return True
        return False

    def get_from_tokens(self):
        target_index_getter = TargetIndexGetter(self.sub_query_tokens)
        from_index = target_index_getter.get_target_index('from')
        end_signs = ['where', 'group by', 'having', 'order by', 'limit', 'union', 'into', 'format', ';']
        from_end_index = target_index_getter.get_target_end_index(end_signs, from_index)
        from_tokens = []
        for i in range(from_index + 1, from_end_index):
            from_tokens.append(self.sub_query_tokens[i])
        return from_tokens