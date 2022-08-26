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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.tez.MapRecordSource;

/**
 * OmniData hive configuration
 *
 * @since 2021-11-09
 */
public class OmniDataConf {
    /**
     * Whether to enable OmniData
     */
    public static final String OMNIDATA_HIVE_ENABLED = "omnidata.hive.enabled";

    /**
     * Whether to enable filter selectivity
     */
    public static final String OMNIDATA_HIVE_FILTER_SELECTIVITY_ENABLED = "omnidata.hive.filter.selectivity.enabled";

    /**
     * Threshold of table size
     */
    public static final String OMNIDATA_HIVE_TABLE_SIZE_THRESHOLD = "omnidata.hive.table.size.threshold";

    /**
     * Filter selectivity
     */
    public static final String OMNIDATA_HIVE_FILTER_SELECTIVITY = "omnidata.hive.filter.selectivity";

    /**
     * Zookeeper quorum server
     */
    public static final String OMNIDATA_HIVE_ZOOKEEPER_QUORUM_SERVER = "omnidata.hive.zookeeper.quorum.server";

    /**
     * Zookeeper status node info
     */
    public static final String OMNIDATA_HIVE_ZOOKEEPER_STATUS_NODE = "omnidata.hive.zookeeper.status.node";

    /**
     * Zookeeper conf path
     */
    public static final String OMNIDATA_HIVE_ZOOKEEPER_CONF_PATH = "omnidata.hive.zookeeper.conf.path";

    /**
     * Whether to enable Zookeeper security
     */
    public static final String OMNIDATA_HIVE_ZOOKEEPER_SECURITY_ENABLED = "omnidata.hive.zookeeper.security.enabled";

    /**
     * Zookeeper connection timeout interval
     */
    public static final String OMNIDATA_HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_MS =
            "omnidata.hive.zookeeper.connection.timeoutMs";

    /**
     * Zookeeper session timeout interval
     */
    public static final String OMNIDATA_HIVE_ZOOKEEPER_SESSION_TIMEOUT_MS = "omnidata.hive.zookeeper.session.timeoutMs";

    /**
     * Zookeeper retry interval
     */
    public static final String OMNIDATA_HIVE_ZOOKEEPER_RETRY_INTERVAL_MS = "omnidata.hive.zookeeper.retry.intervalMs";

    /**
     * OmniData optimized thread numbers
     */
    public static final String OMNIDATA_HIVE_OPTIMIZED_THREAD_NUMS = "omnidata.hive.optimized.thread.nums";

    /**
     * Whether to enable OmniData agg optimized
     */
    public static final String OMNIDATA_HIVE_AGG_OPTIMIZED_ENABLED = "omnidata.hive.agg.optimized.enabled";

    /**
     * Whether to enable OmniData filter optimized
     */
    public static final String OMNIDATA_HIVE_FILTER_OPTIMIZED_ENABLED = "omnidata.hive.filter.optimized.enabled";

    /**
     * Whether to enable OmniData reduce optimized
     */
    public static final String OMNIDATA_HIVE_REDUCE_OPTIMIZED_ENABLED = "omnidata.hive.reduce.optimized.enabled";

    /**
     * Whether there are tables that can be pushed down
     */
    public static final String OMNIDATA_HIVE_EXISTS_TABLE_PUSHDOWN = "omnidata.hive.exists.table.pushdown";

    /**
     * OmniData group optimized coefficient,
     * If the filter type is string or char, the selectivity is inaccurate.
     * You can manually modify this parameter to optimize the selectivity.
     */
    public static final String OMNIDATA_HIVE_GROUP_OPTIMIZED_COEFFICIENT = "omnidata.hive.group.optimized.coefficient";

    /**
     * Whether to enable OmniData group optimized
     */
    public static final String OMNIDATA_HIVE_GROUP_OPTIMIZED_ENABLED = "omnidata.hive.group.optimized.enabled";

    /**
     * OmniData table optimized selectivity
     */
    public static final String OMNIDATA_HIVE_TABLE_OPTIMIZED_SELECTIVITY = "omnidata.hive.table.optimized.selectivity";

    /**
     * Hdfs's replication, default:3
     */
    public static final String DFS_REPLICATION = "dfs.replication";

    /**
     * get the value of a parameter: omnidata.hive.enabled
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataEnabled(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_ENABLED, false);
    }

    /**
     * get the value of a parameter: omnidata.hive.filter.selectivity.enabled
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataFilterSelectivityEnabled(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_FILTER_SELECTIVITY_ENABLED, true);
    }

    /**
     * get the value of a parameter: omnidata.hive.table.size.threshold
     *
     * @param conf hive conf
     * @return threshold
     */
    public static int getOmniDataTablesSizeThreshold(Configuration conf) {
        return conf.getInt(OMNIDATA_HIVE_TABLE_SIZE_THRESHOLD, 102400);
    }

    /**
     * get the value of a parameter: omnidata.hive.filter.selectivity
     *
     * @param conf hive conf
     * @return filter selectivity
     */
    public static Double getOmniDataFilterSelectivity(Configuration conf) {
        double selectivity = conf.getDouble(OMNIDATA_HIVE_FILTER_SELECTIVITY, 0.2);
        checkArgument(selectivity >= 0 && selectivity <= 1.0,
                String.format("The %s value must be in [0.0, 1.0].", OMNIDATA_HIVE_FILTER_SELECTIVITY));
        return selectivity;
    }

    /**
     * get the value of a parameter: omnidata.hive.zookeeper.quorum.server
     *
     * @param conf hive conf
     * @return Zookeeper quorum server
     */
    public static String getOmniDataZookeeperQuorumServer(Configuration conf) {
        return conf.get(OMNIDATA_HIVE_ZOOKEEPER_QUORUM_SERVER);
    }

    /**
     * get the value of a parameter: omnidata.hive.zookeeper.status.node
     *
     * @param conf hive conf
     * @return status node info
     */
    public static String getOmniDataZookeeperStatusNode(Configuration conf) {
        return conf.get(OMNIDATA_HIVE_ZOOKEEPER_STATUS_NODE);
    }

    /**
     * get the value of a parameter: omnidata.hive.zookeeper.conf.path
     *
     * @param conf hive conf
     * @return conf path
     */
    public static String getOmniDataZookeeperConfPath(Configuration conf) {
        return conf.get(OMNIDATA_HIVE_ZOOKEEPER_CONF_PATH);
    }

    /**
     * get the value of a parameter: omnidata.hive.zookeeper.security.enabled
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataZookeeperSecurityEnabled(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_ZOOKEEPER_SECURITY_ENABLED, true);
    }

    /**
     * get the value of a parameter: omnidata.hive.zookeeper.connection.timeoutMs
     *
     * @param conf hive conf
     * @return connection timeout interval
     */
    public static int getOmniDataZookeeperConnectionTimeout(Configuration conf) {
        int timeout = conf.getInt(OMNIDATA_HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_MS, 15000);
        checkArgument(timeout > 0,
                String.format("The %s value must be positive", OMNIDATA_HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_MS));
        return timeout;
    }

    /**
     * get the value of a parameter: omnidata.hive.zookeeper.session.timeoutMs
     *
     * @param conf hive conf
     * @return session timeout interval
     */
    public static int getOmniDataZookeeperSessionTimeout(Configuration conf) {
        int timeout = conf.getInt(OMNIDATA_HIVE_ZOOKEEPER_SESSION_TIMEOUT_MS, 60000);
        checkArgument(timeout > 0,
                String.format("The %s value must be positive", OMNIDATA_HIVE_ZOOKEEPER_SESSION_TIMEOUT_MS));
        return timeout;
    }

    /**
     * get the value of a parameter: omnidata.hive.zookeeper.retry.intervalMs
     *
     * @param conf hive conf
     * @return retry interval
     */
    public static int getOmniDataZookeeperRetryInterval(Configuration conf) {
        int retryInterval = conf.getInt(OMNIDATA_HIVE_ZOOKEEPER_RETRY_INTERVAL_MS, 1000);
        checkArgument(retryInterval > 0,
                String.format("The %s value must be positive", OMNIDATA_HIVE_ZOOKEEPER_RETRY_INTERVAL_MS));
        return retryInterval;
    }

    /**
     * get the value of a parameter: omnidata.hive.optimized.thread.nums
     *
     * @param conf hive conf
     * @return MAX_THREAD_NUMS
     */
    public static int getOmniDataOptimizedThreadNums(Configuration conf) {
        return conf.getInt(OMNIDATA_HIVE_OPTIMIZED_THREAD_NUMS, MapRecordSource.MAX_THREAD_NUMS);
    }

    /**
     * get the value of a parameter: omnidata.hive.agg.optimized.enabled
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataAggOptimizedEnabled(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_AGG_OPTIMIZED_ENABLED, false);
    }

    /**
     * set the value of a parameter: omnidata.hive.agg.optimized.enabled
     *
     * @param conf hive conf
     * @param isOptimized true or false
     */
    public static void setOmniDataAggOptimizedEnabled(Configuration conf, boolean isOptimized) {
        conf.setBoolean(OmniDataConf.OMNIDATA_HIVE_AGG_OPTIMIZED_ENABLED, isOptimized);
    }

    /**
     * get the value of a parameter: omnidata.hive.filter.optimized.enabled
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataFilterOptimizedEnabled(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_FILTER_OPTIMIZED_ENABLED, false);
    }

    /**
     * set the value of a parameter: omnidata.hive.filter.optimized.enabled
     *
     * @param conf hive conf
     * @param isOptimized true or false
     */
    public static void setOmniDataFilterOptimizedEnabled(Configuration conf, boolean isOptimized) {
        conf.setBoolean(OmniDataConf.OMNIDATA_HIVE_FILTER_OPTIMIZED_ENABLED, isOptimized);
    }

    /**
     * get the value of a parameter: omnidata.hive.reduce.optimized.enabled
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataReduceOptimizedEnabled(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_REDUCE_OPTIMIZED_ENABLED, false);
    }

    /**
     * get the value of a parameter: omnidata.hive.exists.table.pushdown
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataExistsTablePushDown(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_EXISTS_TABLE_PUSHDOWN, false);
    }

    /**
     * set the value of a parameter: omnidata.hive.exists.table.pushdown
     *
     * @param conf hive conf
     * @param isExists true or false
     */
    public static void setOmniDataExistsTablePushDown(Configuration conf, boolean isExists) {
        conf.setBoolean(OmniDataConf.OMNIDATA_HIVE_EXISTS_TABLE_PUSHDOWN, isExists);
    }

    /**
     * get the value of a parameter: omnidata.hive.group.optimized.enabled
     *
     * @param conf hive conf
     * @return true or false
     */
    public static Boolean getOmniDataGroupOptimizedEnabled(Configuration conf) {
        return conf.getBoolean(OMNIDATA_HIVE_GROUP_OPTIMIZED_ENABLED, false);
    }

    /**
     * get the value of a parameter: omnidata.hive.group.optimized.coefficient
     *
     * @param conf hive conf
     * @return true or false
     */
    public static double getOmniDataGroupOptimizedCoefficient(Configuration conf) {
        return conf.getDouble(OMNIDATA_HIVE_GROUP_OPTIMIZED_COEFFICIENT, -1);
    }

    /**
     * get the value of a parameter: omnidata.hive.table.optimized.selectivity
     *
     * @param conf hive conf
     * @return optimized selectivity
     */
    public static Double getOmniDataTableOptimizedSelectivity(Configuration conf) {
        double selectivity = conf.getDouble(OMNIDATA_HIVE_TABLE_OPTIMIZED_SELECTIVITY, 1.0);
        checkArgument(selectivity >= 0 && selectivity <= 1.0,
                String.format("The %s value must be in [0.0, 1.0].", OMNIDATA_HIVE_TABLE_OPTIMIZED_SELECTIVITY));
        return selectivity;
    }

    /**
     * set the value of a parameter: omnidata.hive.table.optimized.selectivity
     *
     * @param conf hive conf
     * @param value optimized selectivity
     */
    public static void setOmniDataTableOptimizedSelectivity(Configuration conf, double value) {
        checkArgument(value >= 0 && value <= 1.0,
                String.format("The %s value must be in [0.0, 1.0].", OMNIDATA_HIVE_TABLE_OPTIMIZED_SELECTIVITY));
        conf.setDouble(OmniDataConf.OMNIDATA_HIVE_TABLE_OPTIMIZED_SELECTIVITY, value);
    }

    /**
     * get the value of a parameter: dfs.replication
     *
     * @param conf hive conf
     * @return replication
     */
    public static int getOmniDataReplicationNum(Configuration conf) {
        int replicationNum = conf.getInt(DFS_REPLICATION, 3);
        checkArgument(replicationNum > 0, String.format("The %s value must be positive", DFS_REPLICATION));
        return replicationNum;
    }
}