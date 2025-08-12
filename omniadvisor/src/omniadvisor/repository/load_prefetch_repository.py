#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
from django.db.models import Prefetch
from server.app.models import DatabaseTuningRecord, DatabaseLoad
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.tuning_record import TuningRecord
from omniadvisor.repository.repository import Repository


class LoadPrefetchRepository(Repository):
    """
    提前批量获取反向外键相关对象的数据 避免N+1的查询问题
    """

    @classmethod
    def exclude_query_load_with_tuning_records(cls, exclude_load: Load):
        """
        获取 exclude_load 的 id 以外的 load 及  tuning record的聚合

        :param exclude_load: 排除的load id
        :return:
        """
        res = []

        # 预取相关TuningRecord和ExamRecord
        lower_str = DatabaseTuningRecord.__name__.lower()

        # lookup 参数为固定格式 xx__set
        tuning_record_prefetch = Prefetch(
            f'{lower_str}_set',
            queryset=DatabaseTuningRecord.objects.select_related('load')
        )

        # 获取所有不包含exclude_load的Load记录
        loads = DatabaseLoad.objects.exclude(id=exclude_load.id) \
            .prefetch_related(tuning_record_prefetch) \
            .all()

        for prefetch_load in loads:
            database_tuning_records = prefetch_load.databasetuningrecord_set.all()
            res.append(
                (
                    Load(prefetch_load),
                    [TuningRecord(database_tuning_record) for database_tuning_record in database_tuning_records]
                )
            )

        return res
