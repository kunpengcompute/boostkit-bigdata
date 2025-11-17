# -*- coding: utf-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
""" kmc可配置项
"""

from common.config_parser import user_configs

# kmc配置
KMC_SEM_KEY = int(user_configs.get('crypto', 'kmc.sem.key'), 16)
