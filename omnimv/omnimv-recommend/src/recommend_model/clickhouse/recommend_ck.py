#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import os
import stat

import pandas as pd

from omnimv_logging import get_logger
from src.candidate_generation.clickhouse.projection_candidate_generation.generation_utils import normalize_sql, \
    get_table_name
from src.preprocessor.metadata import TableSchema
from src.preprocessor.proc_conf_ck import get_ck_candidate_recommend_strategy

OMNIMV_LOG = get_logger()


def sort_by_strategy(df_cube):
    col_names = ['projection_compress_ratio', 'matched_sql']
    sort_direction = [True, False]
    strategy = get_ck_candidate_recommend_strategy()
    if strategy == 'sql':
        col_names = ['matched_sql', 'projection_compress_ratio']
        sort_direction = [False, True]
    elif strategy == 'compress_ratio':
        col_names = ['projection_compress_ratio', 'matched_sql']
        sort_direction = [True, False]
    df_cube = df_cube.sort_values(col_names, ascending=sort_direction)
    return df_cube


def candidate_recommend(cube_cost_dict, mv_projection_path, mv_sql_path, table_name):
    df_cube_table = pd.DataFrame(cube_cost_dict).transpose()
    recommend_index = []
    if len(df_cube_table) > 0:
        sort_norm_df = sort_by_strategy(df_cube_table)
        col_names = ['projection_compress_ratio', 'matched_sql', 'table_name']
        OMNIMV_LOG.info(f"Projection for table {table_name}\nProjection cost:\n {sort_norm_df[col_names]}")

        flags = os.O_WRONLY | os.O_CREAT
        modes = stat.S_IWUSR | stat.S_IRUSR

        for cube_id in list(sort_norm_df.index):
            with os.fdopen(os.open(f"{mv_projection_path}/{cube_id}", flags, modes), 'w') as file:
                file.truncate()
                file.write(cube_cost_dict.get(cube_id).get('projection_sql'))

            with os.fdopen(os.open(f"{mv_sql_path}/{cube_id}", flags, modes), 'w') as file:
                file.truncate()
                file.write(cube_cost_dict.get(cube_id).get('mv_sql'))

        return list(sort_norm_df.index)
    else:
        return recommend_index

