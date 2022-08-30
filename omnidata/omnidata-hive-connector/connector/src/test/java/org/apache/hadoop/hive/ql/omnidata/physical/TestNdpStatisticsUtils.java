/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata.physical;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.junit.Test;

/**
 * Test for OmniData Hive statistics tool
 * Used to collect statistics for optimization
 *
 * @since 2022-08-23
 */
public class TestNdpStatisticsUtils {
    private static final int TEST_MAX_REDUCE = 1009;

    @Test
    public void testEstimateReducersByFileSize() {
        assertEquals(NdpStatisticsUtils.estimateReducersByFileSize(0, 0, 0), 0);
        assertEquals(NdpStatisticsUtils.estimateReducersByFileSize(102400, 1024, TEST_MAX_REDUCE), 100);
        assertEquals(NdpStatisticsUtils.estimateReducersByFileSize(10240, 1024, TEST_MAX_REDUCE),
                NdpStatisticsUtils.BASED_REDUCES);
    }

    @Test
    public void testSetOptimizedNumReduceTasks() {
        HiveConf testConf = new HiveConf();
        TezEdgeProperty tezEdgeProperty = new TezEdgeProperty(testConf, TezEdgeProperty.EdgeType.SIMPLE_EDGE, -1);
        ReduceWork reduceWork = new ReduceWork("test");
        reduceWork.setAutoReduceParallelism(true);
        reduceWork.setNumReduceTasks(TEST_MAX_REDUCE);
        reduceWork.setMinReduceTasks(TEST_MAX_REDUCE);
        reduceWork.setMaxReduceTasks(TEST_MAX_REDUCE);
        reduceWork.setEdgePropRef(tezEdgeProperty);
        int optimizedNumReduces = 100;
        NdpStatisticsUtils.setOptimizedNumReduceTasks(reduceWork, optimizedNumReduces, 1024);
        assertEquals(reduceWork.getNumReduceTasks().intValue(), optimizedNumReduces);
        assertEquals(reduceWork.getMinReduceTasks(), optimizedNumReduces / 2);
        assertEquals(reduceWork.getMaxReduceTasks(), optimizedNumReduces);
        assertEquals(tezEdgeProperty.getMinReducer(), optimizedNumReduces / 2);
        assertEquals(tezEdgeProperty.getMaxReducer(), optimizedNumReduces);

        reduceWork.setAutoReduceParallelism(false);
        NdpStatisticsUtils.setOptimizedNumReduceTasks(reduceWork, optimizedNumReduces, 1024);
        assertEquals(reduceWork.getNumReduceTasks().intValue(), optimizedNumReduces);
    }

    @Test
    public void testOptimizeLengthPerGroup() {
        double delta = 0.001d;
        HiveConf testConf = new HiveConf();

        // agg configCoefficient
        OmniDataConf.setOmniDataTableOptimizedSelectivity(testConf, 0.5d);
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, true);
        testConf.setDouble(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_COEFFICIENT, 10d);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 10d, delta);

        // filter configCoefficient
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, false);
        OmniDataConf.setOmniDataFilterOptimizedEnabled(testConf, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 10d, delta);

        testConf.clear();

        // agg
        OmniDataConf.setOmniDataTableOptimizedSelectivity(testConf, 0.5d);
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 3.0d, delta);

        // filter
        OmniDataConf.setOmniDataTableOptimizedSelectivity(testConf, 0.25d);
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, false);
        OmniDataConf.setOmniDataFilterOptimizedEnabled(testConf, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 2.5d, delta);

        // group
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, false);
        OmniDataConf.setOmniDataFilterOptimizedEnabled(testConf, false);
        testConf.setBoolean(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_ENABLED, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 1.8d, delta);
    }
}