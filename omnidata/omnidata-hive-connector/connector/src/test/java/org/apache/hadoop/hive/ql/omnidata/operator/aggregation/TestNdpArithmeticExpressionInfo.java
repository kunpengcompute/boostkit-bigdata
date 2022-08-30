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

package org.apache.hadoop.hive.ql.omnidata.operator.aggregation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test for NdpArithmeticExpressionInfo
 *
 * @since 2022-08-24
 */
public class TestNdpArithmeticExpressionInfo {
    @Test
    public void test() {
        // col
        String expr1 = "col 2:bigint, col 6:bigint";
        NdpArithmeticExpressionInfo info1 = new NdpArithmeticExpressionInfo(expr1);
        Assert.assertTrue(Arrays.equals(info1.getIsVal(), new boolean[] {false, false}));
        Assert.assertArrayEquals(info1.getColId(), new int[] {2, 6});
        Assert.assertArrayEquals(info1.getColType(), new String[] {"bigint", "bigint"});
        Assert.assertArrayEquals(info1.getColValue(), new String[] {null, null});

        // val
        String expr2 = "val 2, val 6";
        NdpArithmeticExpressionInfo info2 = new NdpArithmeticExpressionInfo(expr2);
        Assert.assertTrue(Arrays.equals(info2.getIsVal(), new boolean[] {true, true}));
        Assert.assertArrayEquals(info2.getColValue(), new String[] {"2", "6"});
    }
}