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

package org.apache.hadoop.hive.ql.omnidata.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestOmniDataConf
 *
 * @since 2022-08-24
 */
public class TestOmniDataConf {
    private Configuration mockConfiguration() {
        Configuration conf = new Configuration();
        conf.set(OmniDataConf.OMNIDATA_HIVE_ENABLED, "true");
        conf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_SELECTIVITY_ENABLED, "false");
        conf.set(OmniDataConf.OMNIDATA_HIVE_TABLE_SIZE_THRESHOLD, "1024");
        conf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_SELECTIVITY, "0.6");
        conf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_QUORUM_SERVER, "agent2:2181");
        conf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_STATUS_NODE, "/sdi/status");
        conf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_CONF_PATH, "/opt/hadoopclient/ZooKeeper/zookeeper/conf/");
        conf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_SECURITY_ENABLED, "true");
        conf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_MS, "5000");
        conf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_SESSION_TIMEOUT_MS, "10000");
        conf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_RETRY_INTERVAL_MS, "5");
        conf.set(OmniDataConf.OMNIDATA_HIVE_OPTIMIZED_THREAD_NUMS, "32");
        conf.set(OmniDataConf.OMNIDATA_HIVE_AGG_OPTIMIZED_ENABLED, "true");
        conf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_OPTIMIZED_ENABLED, "true");
        conf.set(OmniDataConf.OMNIDATA_HIVE_REDUCE_OPTIMIZED_ENABLED, "true");
        conf.set(OmniDataConf.OMNIDATA_HIVE_EXISTS_TABLE_PUSHDOWN, "true");
        conf.set(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_COEFFICIENT, "3");
        conf.set(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_ENABLED, "true");
        conf.set(OmniDataConf.OMNIDATA_HIVE_TABLE_OPTIMIZED_SELECTIVITY, "0.5");
        conf.set(OmniDataConf.DFS_REPLICATION, "4");
        return conf;
    }

    @Test
    public void testGet() {
        Configuration conf = mockConfiguration();

        Boolean isNdpEnabled = OmniDataConf.getOmniDataEnabled(conf);
        Assert.assertTrue(isNdpEnabled);

        Boolean isNdpFilterSelectivityEnable = OmniDataConf.getOmniDataFilterSelectivityEnabled(conf);
        Assert.assertFalse(isNdpFilterSelectivityEnable);

        int ndpTablesSizeThreshold = OmniDataConf.getOmniDataTablesSizeThreshold(conf);
        Assert.assertEquals(ndpTablesSizeThreshold, 1024);

        String ndpFilterSelectivity = OmniDataConf.getOmniDataFilterSelectivity(conf).toString();
        Assert.assertEquals(ndpFilterSelectivity, "0.6");

        String ndpZookeeperQuorumServer = OmniDataConf.getOmniDataZookeeperQuorumServer(conf);
        Assert.assertEquals(ndpZookeeperQuorumServer, "agent2:2181");

        String ndpZookeeperStatusNode = OmniDataConf.getOmniDataZookeeperStatusNode(conf);
        Assert.assertEquals(ndpZookeeperStatusNode, "/sdi/status");

        String ndpZookeeperConfPath = OmniDataConf.getOmniDataZookeeperConfPath(conf);
        Assert.assertEquals(ndpZookeeperConfPath, "/opt/hadoopclient/ZooKeeper/zookeeper/conf/");

        Boolean isNdpZookeeperSecurityEnabled = OmniDataConf.getOmniDataZookeeperSecurityEnabled(conf);
        Assert.assertTrue(isNdpZookeeperSecurityEnabled);

        int connectionTimeout = OmniDataConf.getOmniDataZookeeperConnectionTimeout(conf);
        Assert.assertEquals(connectionTimeout, 5000);

        int sessionTimeout = OmniDataConf.getOmniDataZookeeperSessionTimeout(conf);
        Assert.assertEquals(sessionTimeout, 10000);

        int retryInterval = OmniDataConf.getOmniDataZookeeperRetryInterval(conf);
        Assert.assertEquals(retryInterval, 5);

        int omniDataOptimizedThreadNums = OmniDataConf.getOmniDataOptimizedThreadNums(conf);
        Assert.assertEquals(omniDataOptimizedThreadNums, 32);

        Boolean isNdpAggOptimizedEnabled = OmniDataConf.getOmniDataAggOptimizedEnabled(conf);
        Assert.assertTrue(isNdpAggOptimizedEnabled);
        OmniDataConf.setOmniDataAggOptimizedEnabled(conf, false);
        Assert.assertFalse(OmniDataConf.getOmniDataAggOptimizedEnabled(conf));

        Boolean isNdpFilterOptimizedEnabled = OmniDataConf.getOmniDataFilterOptimizedEnabled(conf);
        Assert.assertTrue(isNdpFilterOptimizedEnabled);
        OmniDataConf.setOmniDataFilterOptimizedEnabled(conf, false);
        Assert.assertFalse(OmniDataConf.getOmniDataFilterOptimizedEnabled(conf));

        Boolean isNdpReduceOptimizedEnabled = OmniDataConf.getOmniDataReduceOptimizedEnabled(conf);
        Assert.assertTrue(isNdpReduceOptimizedEnabled);

        Boolean isNdpExistsTablePushDown = OmniDataConf.getOmniDataExistsTablePushDown(conf);
        Assert.assertTrue(isNdpExistsTablePushDown);
        OmniDataConf.setOmniDataExistsTablePushDown(conf, false);
        Assert.assertFalse(OmniDataConf.getOmniDataExistsTablePushDown(conf));

        String omniDataGroupOptimizedCoefficient = "" + OmniDataConf.getOmniDataGroupOptimizedCoefficient(conf);
        Assert.assertEquals(omniDataGroupOptimizedCoefficient, "3.0");

        Boolean isOmniDataGroupOptimizedEnabled = OmniDataConf.getOmniDataGroupOptimizedEnabled(conf);
        Assert.assertTrue(isOmniDataGroupOptimizedEnabled);

        String ndpTableOptimizedSelectivity = OmniDataConf.getOmniDataTableOptimizedSelectivity(conf).toString();
        Assert.assertEquals(ndpTableOptimizedSelectivity, "0.5");
        OmniDataConf.setOmniDataTableOptimizedSelectivity(conf, 0.6);
        Assert.assertEquals("" + OmniDataConf.getOmniDataTableOptimizedSelectivity(conf), "0.6");

        int ndpReplicationNum = OmniDataConf.getOmniDataReplicationNum(conf);
        Assert.assertEquals(ndpReplicationNum, 4);
    }
}