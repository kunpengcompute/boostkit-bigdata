#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

import os
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from recommend_model.spark import test_recommend_spark

from preprocessor import test_proc_conf_spark, test_metadata
import test_utils

if __name__ == '__main__':
    suite = unittest.TestSuite()

    tests = [

        test_proc_conf_spark.Test03("test"),

        test_metadata.Test03('test'),

        test_utils.Test00("test"),
        test_utils.Test02("test"),
        test_utils.Test08("test"),
        test_utils.Test09("test"),
        test_utils.Test10("test"),
        test_utils.Test11("test"),
        test_utils.Test12("test"),
        test_utils.Test13("test"),

        test_recommend_spark.Test01("test"),
        test_recommend_spark.Test02("test"),
        test_recommend_spark.Test03("test"),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)