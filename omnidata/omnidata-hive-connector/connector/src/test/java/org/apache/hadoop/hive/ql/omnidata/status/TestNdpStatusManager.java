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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TestNdpStatusManager
 *
 * @since 2022-08-24
 */
public class TestNdpStatusManager {
    private Configuration conf;

    private Configuration mockConfiguration() {
        Configuration mockConf = new Configuration();
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ENABLED, "true");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_SELECTIVITY_ENABLED, "false");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_TABLE_SIZE_THRESHOLD, "1024");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_SELECTIVITY, "0.6");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_QUORUM_SERVER, "agent2:2181");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_STATUS_NODE, "/sdi/status");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_CONF_PATH, "/opt/hadoopclient/ZooKeeper/zookeeper/conf/");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_SECURITY_ENABLED, "true");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_MS, "5000");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_SESSION_TIMEOUT_MS, "10000");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_RETRY_INTERVAL_MS, "5");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_OPTIMIZED_THREAD_NUMS, "32");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_AGG_OPTIMIZED_ENABLED, "true");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_OPTIMIZED_ENABLED, "true");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_REDUCE_OPTIMIZED_ENABLED, "true");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_EXISTS_TABLE_PUSHDOWN, "true");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_COEFFICIENT, "3");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_ENABLED, "true");
        mockConf.set(OmniDataConf.OMNIDATA_HIVE_TABLE_OPTIMIZED_SELECTIVITY, "0.5");
        mockConf.set(OmniDataConf.DFS_REPLICATION, "4");
        mockConf.set(NdpStatusManager.NDP_DATANODE_HOSTNAMES, "agent1,agent2,agent3");
        return mockConf;
    }

    private Map<String, NdpStatusInfo> createStatusMap() {
        NdpStatusInfo agent1 = new NdpStatusInfo("agent1", "1.0.0", 10000.01, 5, 10);
        NdpStatusInfo agent2 = new NdpStatusInfo("agent2", "1.0.0", 10000.02, 5, 10);
        NdpStatusInfo agent3 = new NdpStatusInfo("agent3", "1.0.0", 10000.03, 5, 10);
        return new HashMap<String, NdpStatusInfo>() {
            {
                put("omnidata1", agent1);
                put("omnidata2", agent2);
                put("omnidata3", agent3);
            }
        };
    }

    @Before
    public void init() {
        this.conf = mockConfiguration();
    }

    @Test
    public void testSetOmniDataHostToConf() {
        Map<String, NdpStatusInfo> ndpStatusInfoMap = createStatusMap();
        NdpStatusManager.setOmniDataHostToConf(conf, ndpStatusInfoMap);
        Assert.assertEquals(conf.get("agent1"), "omnidata1");
        Assert.assertEquals(conf.get("agent2"), "omnidata2");
        Assert.assertEquals(conf.get("agent3"), "omnidata3");
    }

    @Test
    public void testGetRandomAvailableDataNodeHost() {
        Map<String, NdpStatusInfo> ndpStatusInfoMap = createStatusMap();
        NdpStatusManager.setOmniDataHostToConf(conf, ndpStatusInfoMap);
        Assert.assertEquals(conf.get("agent1"), "omnidata1");
        Assert.assertEquals(conf.get("agent2"), "omnidata2");
        Assert.assertEquals(conf.get("agent3"), "omnidata3");
        List<String> excludeHosts = new ArrayList<String>() {
            {
                add("agent1");
                add("agent2");
                add("agent3");
            }
        };
        String random = NdpStatusManager.getRandomAvailableDataNodeHost(conf, excludeHosts);
        Assert.assertEquals(random, "");
        excludeHosts.clear();
        excludeHosts.add("agent1");
        List<String> hosts = new ArrayList<>();
        hosts.add("agent2");
        hosts.add("agent3");
        Assert.assertTrue(hosts.contains(NdpStatusManager.getRandomAvailableDataNodeHost(conf, excludeHosts)));
    }
}