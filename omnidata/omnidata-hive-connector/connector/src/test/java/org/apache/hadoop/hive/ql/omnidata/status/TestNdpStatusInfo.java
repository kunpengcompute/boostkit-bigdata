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

package org.apache.hadoop.hive.ql.omnidata.status;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * TestNdpStatusInfo
 *
 * @since 2022-08-24
 */
public class TestNdpStatusInfo {
    NdpStatusInfo ndpStatusInfo;

    @Before
    public void init() {
        this.ndpStatusInfo = new NdpStatusInfo();
        this.ndpStatusInfo = new NdpStatusInfo("agent1", "1.0.0", 10000.02, 5, 10);
    }

    @Test
    public void testGetAndSet() {
        String datanodeHost = this.ndpStatusInfo.getDatanodeHost();
        Assert.assertEquals(datanodeHost, "agent1");

        String version = this.ndpStatusInfo.getVersion();
        Assert.assertEquals(version, "1.0.0");
        this.ndpStatusInfo.setVersion("5.0.0");

        Double threshold = this.ndpStatusInfo.getThreshold();
        Assert.assertEquals(threshold, Double.valueOf("10000.02"));
        this.ndpStatusInfo.setThreshold(50.55);

        int runningTasks = this.ndpStatusInfo.getRunningTasks();
        Assert.assertEquals(runningTasks, 5);

        int maxTasks = this.ndpStatusInfo.getMaxTasks();
        Assert.assertEquals(maxTasks, 10);
    }
}