#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import sys

from src.preprocessor.proc_conf_spark import get_spark_schema_path

try:
    import pydoop.hdfs as hdfs
except ModuleNotFoundError as _:
    pass


class TableSchema(object):
    def __init__(self):
        self.data = {}
        self.partition_col = {}
        engine = sys.argv[1]
        if engine == 'spark':
            schema_path = get_spark_schema_path()
            with hdfs.open(schema_path, 'r', encoding='utf-8') as file:
                while '' != file.read(1).decode('utf-8'):
                    file.seek(file.tell() - 1)
                    self.build_table(file)

    def build_table(self, table_schema, need_decode=True):
        name = ""
        key_name = {}
        partition = {}
        projections = []
        engine = {}
        last_line = ""
        while True:
            if need_decode:
                line = table_schema.readline().decode('utf-8')
            else:
                line = table_schema.readline()
            if '\n' == line:
                break
            if line.upper().find("CREATE") != -1 and line.upper().find("TABLE") != -1:
                name = line.upper().strip().split()[2].replace('`', '')  # spark 表名库名也会被包裹
                continue
            if line.upper().find("PARTITIONED") != -1:
                partition_col = line.upper().split("BY")[-1].strip()
                if name != "":
                    self.partition_col[name] = partition_col.lower()
            if line.upper().strip().startswith("PARTITION BY"):
                # 没有函数作用在key上的时候，如果只有一个key也有可能没有用括号括起来
                partition_key, partition_desc = self.parse_partition(line)
                partition["key"] = partition_key
                partition["desc"] = partition_desc
            if line.startswith('ENGINE = Distributed('):
                local_table = line.split(',')[2].strip().replace('\'', '')
                engine["local_table"] = local_table

            if line.find(";") != -1:
                break
            if len(line.strip()) == 0 or len(line.strip().split()) < 2:
                continue
            if ' PROJECTION ' in line:
                projection_name = line.strip().split(' ')[-1]
                projections.append(projection_name)
            if (line.endswith(",\n") or last_line.endswith(",\n")) and ' = ' not in line:
                # last_line用于防止最后一个列定义找不到
                key = line.strip().split()[0].replace('`', '')
                tmp_table_type = line.strip().split()[1].replace('`', '')
                table_type = tmp_table_type[:-1] if tmp_table_type.endswith(",") else tmp_table_type
                key_name[key] = table_type
                last_line = line
        self.init_data(name, key_name, partition, engine, projections)
        return

    def init_data(self, name, key_name, partition, engine, projections):
        if name != "":
            self.data[name] = {}
            self.data[name]['columns'] = key_name
            self.data[name]['projections'] = projections
            if partition:
                self.data[name]["partition_info"] = partition
            if engine:
                self.data[name]['engine_info'] = engine

    def parse_partition(self, line):
        if line.find("(") == -1:
            partition_key = [line.strip().split()[-1]]
            partition_desc = line.strip().split()[-1]
        else:
            partition_key = line[line.find("(") + 1:line.find(")")].split(",")
            partition_desc = line[
                             line.upper().index("PARTITION BY") + len("PARTITION BY") + 1:line.rfind(")") + 1]
        return partition_key, partition_desc


def get_spark_table_columns(table):
    table_metadata = TableSchema()
    table_columns = []
    for schema_table_name in table_metadata.data:
        if table.upper() == schema_table_name.split('.')[-1]:
            table_columns_map = table_metadata.data.get(schema_table_name)
            table_columns = list(table_columns_map.get('columns').keys())
    return table_columns