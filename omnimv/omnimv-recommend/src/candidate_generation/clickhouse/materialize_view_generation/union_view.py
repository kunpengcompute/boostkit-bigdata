#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

class ViewsUnion(object):
    def __init__(self, _normalized_views):
        self.normalized_views = _normalized_views

    def get_union_views(self):
        removed = []
        for i, _ in enumerate(self.normalized_views):
            for j, _ in enumerate(self.normalized_views):
                if j <= i or (i in removed or j in removed):
                    continue
                normalized_view_i = self.normalized_views[i]
                normalized_view_j = self.normalized_views[j]
                if normalized_view_i.could_union(normalized_view_j):
                    removed.append(j)
                    normalized_view_i.union(normalized_view_j)
        union_views = []
        for i, _ in enumerate(self.normalized_views):
            if i not in removed:
                union_views.append(self.normalized_views[i])
        return union_views