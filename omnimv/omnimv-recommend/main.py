#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import os
import sys


def change_file_permission():
    log_file_name = "omnimv"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base_dir, "logs")
    log_file = os.path.join(log_dir, log_file_name)
    if os.path.exists(log_dir):
        for bak_file_name in os.listdir(log_dir):
            if bak_file_name != log_file_name:
                bak_file_path = os.path.join(log_dir, bak_file_name)
                os.chmod(bak_file_path, 0o440)
        if os.path.exists(log_file):
            os.chmod(log_file, 0o640)


if __name__ == '__main__':
    engine = sys.argv[1]
    if engine == 'clickhouse':
        task = sys.argv[2]
        from clickhouse_task import run_clickhouse_task
        run_clickhouse_task(task)
        change_file_permission()
        sys.exit()
    elif engine == 'spark':
        task = sys.argv[2]
        from spark_task import run_spark_task
        run_spark_task(task)
        change_file_permission()
        sys.exit()
    else:
        raise ValueError("Only spark or clickhouse engine is supported\n"
                         "Usage: python main.py [clickhouse/spark] [task]")