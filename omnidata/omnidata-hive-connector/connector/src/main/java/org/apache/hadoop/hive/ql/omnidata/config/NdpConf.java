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

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Ndp hive configuration
 *
 * @since 2021-11-09
 */
public class NdpConf {
    public final static String NDP_ENABLED = "hive.sql.ndp.enabled";

    public final static String NDP_FILTER_SELECTIVITY_ENABLE = "hive.sql.ndp.filter.selectivity.enable";

    public final static String NDP_TABLE_SIZE_THRESHOLD = "hive.sql.ndp.table.size.threshold";

    public final static String NDP_FILTER_SELECTIVITY = "hive.sql.ndp.filter.selectivity";

    public final static String NDP_UDF_WHITELIST = "hive.sql.ndp.udf.whitelist";

    public final static String NDP_ZOOKEEPER_QUORUM_SERVER = "hive.sql.ndp.zookeeper.quorum.server";

    public final static String NDP_ZOOKEEPER_STATUS_NODE = "hive.sql.ndp.zookeeper.status.node";

    public final static String NDP_ZOOKEEPER_CONF_PATH = "hive.sql.ndp.zookeeper.conf.path";

    public final static String NDP_ZOOKEEPER_SECURITY_ENABLED = "hive.sql.ndp.zookeeper.security.enabled";

    public final static String NDP_ZOOKEEPER_CONNECTION_TIMEOUT = "hive.sql.ndp.zookeeper.connection.timeoutMs";

    public final static String NDP_ZOOKEEPER_SESSION_TIMEOUT = "hive.sql.ndp.zookeeper.session.timeoutMs";

    public final static String NDP_ZOOKEEPER_RETRY_INTERVAL = "hive.sql.ndp.zookeeper.retry.intervalMs";

    public final static String NDP_REPLICATION_NUM = "hive.sql.ndp.replication.num";

    public static final String NDP_AGG_OPTIMIZED_ENABLE = "hive.sql.ndp.agg.optimized.enable";

    private final Properties ndpProperties = new Properties();

    public NdpConf(Configuration conf) {
        init(conf);
    }

    private void init(Configuration conf) {
        String ndpEnabled = conf.get(NDP_ENABLED);
        ndpProperties.setProperty(NDP_ENABLED, (ndpEnabled == null ? "true" : ndpEnabled));
        String ndpFilterSelectivityEnable = conf.get(NDP_FILTER_SELECTIVITY_ENABLE);
        ndpProperties.setProperty(NDP_FILTER_SELECTIVITY_ENABLE,
                (ndpFilterSelectivityEnable == null ? "true" : ndpFilterSelectivityEnable));
        String ndpTablesSizeThreshold = conf.get(NDP_TABLE_SIZE_THRESHOLD);
        ndpProperties.setProperty(NDP_TABLE_SIZE_THRESHOLD,
                (ndpTablesSizeThreshold == null ? "10240" : ndpTablesSizeThreshold));
        String ndpFilterSelectivity = conf.get(NDP_FILTER_SELECTIVITY);
        ndpProperties.setProperty(NDP_FILTER_SELECTIVITY,
                (ndpFilterSelectivity == null ? "0.5" : ndpFilterSelectivity));
        String ndpZookeeperQuorumServer = conf.get(NDP_ZOOKEEPER_QUORUM_SERVER);
        ndpProperties.setProperty(NDP_ZOOKEEPER_QUORUM_SERVER,
                (ndpZookeeperQuorumServer == null ? "localhost:2181" : ndpZookeeperQuorumServer));
        String ndpZookeeperConnectionTimeout = conf.get(NDP_ZOOKEEPER_CONNECTION_TIMEOUT);
        ndpProperties.setProperty(NDP_ZOOKEEPER_CONNECTION_TIMEOUT,
                (ndpZookeeperConnectionTimeout == null ? "15000" : ndpZookeeperConnectionTimeout));
        String ndpZookeeperSessionTimeout = conf.get(NDP_ZOOKEEPER_SESSION_TIMEOUT);
        ndpProperties.setProperty(NDP_ZOOKEEPER_SESSION_TIMEOUT,
                (ndpZookeeperSessionTimeout == null ? "60000" : ndpZookeeperSessionTimeout));
        String ndpZookeeperRetryInterval = conf.get(NDP_ZOOKEEPER_RETRY_INTERVAL);
        ndpProperties.setProperty(NDP_ZOOKEEPER_RETRY_INTERVAL,
                (ndpZookeeperRetryInterval == null ? "1000" : ndpZookeeperRetryInterval));
        String ndpZookeeperConfPath = conf.get(NDP_ZOOKEEPER_CONF_PATH);
        ndpProperties.setProperty(NDP_ZOOKEEPER_CONF_PATH,
                (ndpZookeeperConfPath == null ? "/usr/local/zookeeper/conf" : ndpZookeeperConfPath));
        String ndpZookeeperSecurityEnabled = conf.get(NDP_ZOOKEEPER_SECURITY_ENABLED);
        ndpProperties.setProperty(NDP_ZOOKEEPER_SECURITY_ENABLED,
                (ndpZookeeperSecurityEnabled == null ? "true" : ndpZookeeperSecurityEnabled));
        String ndpZookeeperStatusNode = conf.get(NDP_ZOOKEEPER_STATUS_NODE);
        ndpProperties.setProperty(NDP_ZOOKEEPER_STATUS_NODE,
                (ndpZookeeperStatusNode == null ? "/sdi/status" : ndpZookeeperStatusNode));
    }

    public Boolean getNdpEnabled() {
        return Boolean.valueOf(ndpProperties.getProperty(NDP_ENABLED, "true").toLowerCase());
    }

    public Boolean getNdpFilterSelectivityEnable() {
        return Boolean.valueOf(ndpProperties.getProperty(NDP_FILTER_SELECTIVITY_ENABLE, "false").toLowerCase());
    }

    public int getNdpTablesSizeThreshold() {
        return Integer.parseInt(ndpProperties.getProperty(NDP_TABLE_SIZE_THRESHOLD, "10240"));
    }

    public Double getNdpFilterSelectivity() {
        double selectivity = Double.parseDouble(ndpProperties.getProperty(NDP_FILTER_SELECTIVITY, "0.5"));
        checkArgument(selectivity >= 0 && selectivity <= 1.0,
                String.format("The %s value must be in [0.0, 1.0].", NDP_FILTER_SELECTIVITY));
        return selectivity;
    }

    public String[] getNdpUdfWhitelist() {
        String[] whiteList = ndpProperties.getProperty(NDP_UDF_WHITELIST, "").split(",");
        checkArgument(whiteList.length == 0, String.format("The %s is empty", NDP_UDF_WHITELIST));
        return whiteList;
    }

    public String getNdpZookeeperQuorumServer() {
        return ndpProperties.getProperty(NDP_ZOOKEEPER_QUORUM_SERVER, "localhost:2181");
    }

    public int getNdpZookeeperConnectionTimeout() {
        int timeout = Integer.parseInt(ndpProperties.getProperty(NDP_ZOOKEEPER_CONNECTION_TIMEOUT, "15000"));
        checkArgument(timeout > 0, String.format("The %s value must be positive", NDP_ZOOKEEPER_CONNECTION_TIMEOUT));
        return timeout;
    }

    public int getNdpZookeeperSessionTimeout() {
        int timeout = Integer.parseInt(ndpProperties.getProperty(NDP_ZOOKEEPER_SESSION_TIMEOUT, "60000"));
        checkArgument(timeout > 0, String.format("The %s value must be positive", NDP_ZOOKEEPER_SESSION_TIMEOUT));
        return timeout;
    }

    public int getNdpZookeeperRetryInterval() {
        int retryInterval = Integer.parseInt(ndpProperties.getProperty(NDP_ZOOKEEPER_RETRY_INTERVAL, "1000"));
        checkArgument(retryInterval > 0, String.format("The %s value must be positive", NDP_ZOOKEEPER_RETRY_INTERVAL));
        return retryInterval;
    }

    public String getNdpZookeeperConfPath() {
        return ndpProperties.getProperty(NDP_ZOOKEEPER_CONF_PATH, "/usr/local/zookeeper/conf");
    }

    public Boolean getNdpZookeeperSecurityEnabled() {
        return Boolean.valueOf(ndpProperties.getProperty(NDP_ZOOKEEPER_SECURITY_ENABLED, "true").toLowerCase());
    }

    public String getNdpZookeeperStatusNode() {
        return ndpProperties.getProperty(NDP_ZOOKEEPER_STATUS_NODE, "/sdi/status");
    }

    public static int getNdpReplicationNum(Configuration conf) {
        String val = conf.get(NDP_REPLICATION_NUM);
        int replicationNum = Integer.parseInt((val == null) ? "3" : val);
        checkArgument(replicationNum > 0, String.format("The %s value must be positive", NDP_REPLICATION_NUM));
        return replicationNum;
    }
}