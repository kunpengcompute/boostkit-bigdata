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

package org.apache.hadoop.hive.ql.omnidata.serialize;

import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestNdpSerializationUtils
 *
 * @since 2022-08-24
 */
public class TestNdpSerializationUtils {
    @Test
    public void testSerializeNdpPredicateInfo() {
        NdpPredicateInfo ndpPredicateInfo = new NdpPredicateInfo(false);
        String str = NdpSerializationUtils.serializeNdpPredicateInfo(ndpPredicateInfo);
        Assert.assertEquals(str, "{\"isPushDown\":false,\"isPushDownAgg\":false,"
                + "\"isPushDownFilter\":false,\"hasPartitionColumn\":false}");
    }

    @Test
    public void testDeserializeNdpPredicateInfo() {
        String str = "{\"isPushDown\":false,\"isPushDownAgg\":false,"
                + "\"isPushDownFilter\":false,\"hasPartitionColumn\":false}";
        NdpPredicateInfo ndpPredicateInfo = NdpSerializationUtils.deserializeNdpPredicateInfo(str);
        NdpPredicateInfo test = new NdpPredicateInfo(false);
        Assert.assertEquals(ndpPredicateInfo.getIsPushDown(), test.getIsPushDown());
        Assert.assertEquals(ndpPredicateInfo.getIsPushDownAgg(), test.getIsPushDownAgg());
        Assert.assertEquals(ndpPredicateInfo.getIsPushDownFilter(), test.getIsPushDownFilter());
        Assert.assertEquals(ndpPredicateInfo.getHasPartitionColumn(), test.getHasPartitionColumn());
        try {
            String s1 = "{isPushDown\":false,\"isPushDownAgg\":false,"
                    + "\"isPushDownFilter\":false,\"hasPartitionColumn\":false}";
            NdpPredicateInfo ndpPredicateInfo1 = NdpSerializationUtils.deserializeNdpPredicateInfo(s1);
        } catch (RuntimeException e) {
            Assert.assertEquals(e.getClass(), RuntimeException.class);
        }

        try {
            String s2 = "";
            NdpPredicateInfo ndpPredicateInfo2 = NdpSerializationUtils.deserializeNdpPredicateInfo(s2);
        } catch (RuntimeException e) {
            Assert.assertEquals(e.getClass(), RuntimeException.class);
        }
    }
}