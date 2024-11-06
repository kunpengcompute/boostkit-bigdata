#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

from omnimv_logging import get_logger
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import get_local_table
from src.candidate_generation.clickhouse.projection_candidate_generation.parse_projection_msg import ProjectionMsgGetter
from src.candidate_generation.clickhouse.projection_candidate_generation.sub_query_generation import TargetQueryGetter
from src.preprocessor.proc_conf_ck import get_ck_is_distributed_table

OMNIMV_LOG = get_logger()


class ProjectionGetter(object):
    def __init__(self, _sql, _sql_file_name, _table):
        self.sql = _sql
        self.sql_file_name = _sql_file_name
        self.table = _table

    def get_projection(self):
        base_projection = None
        try:
            queries = self.get_queries()
            table_projections = get_table_projections(queries)
            sorted_projections = sorted(table_projections.values(), key=lambda x: len(x), reverse=True)
            if len(sorted_projections) > 0:
                base_projection = sorted_projections[0][0]
                for projection in sorted_projections[0]:
                    base_projection.merge_projection(projection)
            is_distributed_table = get_ck_is_distributed_table()
            local_table = get_local_table(self.table)
            if is_distributed_table and local_table == '':
                base_projection = None  # 分布式模式下，遇到非分布式表，不提供projection
            if is_distributed_table and local_table != '' and base_projection is not None:
                base_projection.table = local_table
                base_projection.is_distributed_table = True
            return base_projection
        except (KeyError, IndexError, AttributeError, TypeError, ValueError) as error:
            OMNIMV_LOG.error(f'get_projection: {error}')
            return base_projection

    def get_queries(self):
        target_query_getter = TargetQueryGetter(self.sql, self.table)
        queries = target_query_getter.queries_for_projection()
        return queries


def get_table_projections(queries):
    table_projections = {}
    for query in queries:
        projection_msg_getter = ProjectionMsgGetter(query)
        projection_msg = projection_msg_getter.get_projection_msg()
        if projection_msg is not None and projection_msg.verify_query():
            if projection_msg.table in table_projections.keys():
                table_projections.get(projection_msg.table).append(projection_msg)
            else:
                table_projections[projection_msg.table] = [projection_msg]
    return table_projections