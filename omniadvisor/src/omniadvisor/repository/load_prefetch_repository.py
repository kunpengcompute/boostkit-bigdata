#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
from django.db.models import Prefetch
from server.app.models import DatabaseTuningRecord, DatabaseLoad
from omniadvisor.repository.repository import Repository


class LoadPrefetchRepository(Repository):

    @classmethod
    def get_loads_exclude(cls, exclude_load_id: str):
        """
        获取 exclude_load_id 以外的 load 及  tuning record的聚合
        :param exclude_load_id: 排除的load id
        :return:
        """

        # 预取相关TuningRecord和ExamRecord
        lower_str = DatabaseTuningRecord.__name__.lower()

        # lookup 参数为固定格式 xx__set
        tuning_record_prefetch = Prefetch(
            f'{lower_str}_set',
            queryset=DatabaseTuningRecord.objects.select_related('load')
        )

        # 获取所有不包含exclude_load的Load记录
        loads = DatabaseLoad.objects.exclude(id=exclude_load_id) \
            .prefetch_related(tuning_record_prefetch) \
            .all()

        return loads
