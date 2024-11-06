#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from src.candidate_generation.clickhouse.materialize_view_generation.filter_view import ViewFilter


class SparkViewFilter(ViewFilter):
    def __init__(self, sub_query):
        super().__init__(sub_query)

    def get_view(self):
        if self.contain_multi_table():
            return self.compensate_view()
        return ''

    def compensate_view(self):
        view = self.sub_query
        view_lower = view.lower()
        unsupported_exp = ['rollup', ' over ', 'grouping', 'stddev_samp', 'cross join']
        if any(exp in view_lower for exp in unsupported_exp):
            view = ''
        if 'distinct' in view.lower():
            view = ''
        if 'having' in view.lower():
            view = view[0:view.lower().index('having')]
        return view

