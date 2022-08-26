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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Operate Ndp zookeeper data
 *
 * @since 2021-03
 */
public class NdpStatusManager {
    private NdpStatusManager() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(NdpStatusManager.class);

    public static final String NDP_DATANODE_HOSTNAMES = "hive.ndp.datanode.hostnames";

    public static final String KRB5_LOGIN_CONF_KEY = "java.security.auth.login.config";

    public static final String KRB5_CONF_KEY = "java.security.krb5.conf";

    public static final String KRB5_SASL_CLIENT_CONF_KEY = "zookeeper.sasl.client";

    public static final String LOGIN_CONFIG_FILE = "jaas.conf";

    public static final String KRB5_CONFIG_FILE = "krb5.conf";

    public static final String NDP_DATANODE_HOSTNAME_SEPARATOR = ",";

    /**
     * Get OmniData host resources data from ZooKeeper
     *
     * @param conf hive conf
     * @return hostname -> ndp status
     */
    public static Map<String, NdpStatusInfo> getNdpZookeeperData(Configuration conf) {
        Map<String, NdpStatusInfo> ndpMap = new HashMap<>();
        String parentPath = OmniDataConf.getOmniDataZookeeperStatusNode(conf);
        String quorumServer = OmniDataConf.getOmniDataZookeeperQuorumServer(conf);
        String confPath = OmniDataConf.getOmniDataZookeeperConfPath(conf);
        if (parentPath == null || quorumServer == null || confPath == null) {
            LOG.error("OmniData Hive failed to get Zookeeper parameters, "
                            + "please set the following parameters: {} {} {}",
                    OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_QUORUM_SERVER, OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_STATUS_NODE,
                    OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_CONF_PATH);
            return ndpMap;
        }
        if (OmniDataConf.getOmniDataZookeeperSecurityEnabled(conf)) {
            enableKrb5(confPath);
        }
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(quorumServer)
                .sessionTimeoutMs(OmniDataConf.getOmniDataZookeeperSessionTimeout(conf))
                .connectionTimeoutMs(OmniDataConf.getOmniDataZookeeperConnectionTimeout(conf))
                .retryPolicy(new RetryForever(OmniDataConf.getOmniDataZookeeperRetryInterval(conf)))
                .build();
        zkClient.start();
        if (!verifyZookeeperPath(zkClient, parentPath)) {
            return ndpMap;
        }
        InterProcessMutex lock = new InterProcessMutex(zkClient, parentPath);
        try {
            if (lock.acquire(OmniDataConf.getOmniDataZookeeperRetryInterval(conf), TimeUnit.MILLISECONDS)) {
                List<String> childrenPaths = zkClient.getChildren().forPath(parentPath);
                ObjectMapper mapper = new ObjectMapper();
                for (String path : childrenPaths) {
                    if (path.contains("-lock-")) {
                        continue;
                    }
                    byte[] data = zkClient.getData().forPath(parentPath + "/" + path);
                    NdpStatusInfo statusInfo = mapper.readValue(data, NdpStatusInfo.class);
                    ndpMap.put(path, statusInfo);
                }
            }
        } catch (Exception e) {
            LOG.error("OmniData Hive failed to get host resources data from ZooKeeper", e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                LOG.error("OmniData Hive failed to release OmniData lock from ZooKeeper", e);
            }
            zkClient.close();
        }
        return ndpMap;
    }

    private static boolean verifyZookeeperPath(CuratorFramework zkClient, String path) {
        try {
            // verify the path from ZooKeeper
            Stat stat = zkClient.checkExists().forPath(path);
            if (stat == null) {
                LOG.error("OmniData Hive failed to get parent node from ZooKeeper");
                return false;
            }
        } catch (Exception e) {
            LOG.error("OmniData Hive failed to get host resources data from ZooKeeper", e);
            return false;
        }
        return true;
    }

    private static void enableKrb5(String confPath) {
        System.setProperty(KRB5_LOGIN_CONF_KEY, confPath + "/" + LOGIN_CONFIG_FILE);
        System.setProperty(KRB5_CONF_KEY, confPath + "/" + KRB5_CONFIG_FILE);
        System.setProperty(KRB5_SASL_CLIENT_CONF_KEY, "true");
    }

    /**
     * Add the OmniData mapping relationship to the conf
     *
     * @param conf hive conf
     * @param ndpStatusInfoMap hostname map:
     * key: OmniData host
     * value: datanode host
     */
    public static void setOmniDataHostToConf(Configuration conf, Map<String, NdpStatusInfo> ndpStatusInfoMap) {
        List<String> dataNodeHosts = new ArrayList<>();
        for (Map.Entry<String, NdpStatusInfo> info : ndpStatusInfoMap.entrySet()) {
            String dataNodeHost = info.getValue().getDatanodeHost();
            String omniDataHost = info.getKey();
            // datanode host -> OmniData host
            conf.set(dataNodeHost, omniDataHost);
            dataNodeHosts.add(dataNodeHost);
        }
        conf.set(NDP_DATANODE_HOSTNAMES, String.join(NDP_DATANODE_HOSTNAME_SEPARATOR, dataNodeHosts));
    }

    /**
     * One DataNode needs to be randomly selected from the available DataNodes and is not included in the excludeHosts.
     *
     * @param conf hive conf
     * @param excludeDataNodeHosts excluded DataNode host
     * @return random DataNode host
     */
    public static String getRandomAvailableDataNodeHost(Configuration conf, List<String> excludeDataNodeHosts) {
        List<String> dataNodeHosts = new ArrayList<>(
                Arrays.asList(conf.get(NdpStatusManager.NDP_DATANODE_HOSTNAMES).split(NDP_DATANODE_HOSTNAME_SEPARATOR)));
        if (excludeDataNodeHosts.size() >= dataNodeHosts.size()) {
            return "";
        }
        Iterator<String> dataNodeIt = dataNodeHosts.iterator();
        while (dataNodeIt.hasNext()) {
            String dataNode = dataNodeIt.next();
            excludeDataNodeHosts.forEach(edn -> {
                if (dataNode.equals(edn)) {
                    dataNodeIt.remove();
                }
            });
        }
        int randomIndex = (int) (Math.random() * dataNodeHosts.size());
        return dataNodeHosts.get(randomIndex);
    }

    /**
     * If the number of Replication is not specified, the default value is 3
     *
     * @param conf hive config
     * @param fileSplit fileSplit
     * @return OmniData hosts
     */
    public static List<String> getOmniDataHosts(Configuration conf, FileSplit fileSplit) {
        return getOmniDataHosts(conf, fileSplit, 3);
    }

    /**
     * get the OmniData hosts.
     *
     * @param conf hive config
     * @param fileSplit fileSplit
     * @param ndpReplicationNum hdfs replication
     * @return OmniData hosts
     */
    public static List<String> getOmniDataHosts(Configuration conf, FileSplit fileSplit, int ndpReplicationNum) {
        List<String> omniDataHosts = new ArrayList<>();
        List<String> dataNodeHosts = getDataNodeHosts(conf, fileSplit, ndpReplicationNum);
        // shuffle
        Collections.shuffle(dataNodeHosts);
        dataNodeHosts.forEach(dn -> {
            // possibly null
            if (conf.get(dn) != null) {
                omniDataHosts.add(conf.get(dn));
            }
        });
        // add a random available datanode
        String randomDataNodeHost = NdpStatusManager.getRandomAvailableDataNodeHost(conf, dataNodeHosts);
        if (randomDataNodeHost.length() > 0 && conf.get(randomDataNodeHost) != null) {
            omniDataHosts.add(conf.get(randomDataNodeHost));
        }
        return omniDataHosts;
    }

    /**
     * get the DataNode hosts.
     *
     * @param conf hive config
     * @param fileSplit fileSplit
     * @param ndpReplicationNum hdfs replication
     * @return DataNode hosts
     */
    public static List<String> getDataNodeHosts(Configuration conf, FileSplit fileSplit, int ndpReplicationNum) {
        List<String> hosts = new ArrayList<>();
        try {
            BlockLocation[] blockLocations = fileSplit.getPath()
                    .getFileSystem(conf)
                    .getFileBlockLocations(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength());
            for (BlockLocation block : blockLocations) {
                addHostsByBlock(conf, ndpReplicationNum, block, hosts);
            }
        } catch (IOException e) {
            LOG.error("NdpStatusManager getDataNodeHosts() failed", e);
        }
        return hosts;
    }

    /**
     * Add datanode by block
     *
     * @param conf hive config
     * @param ndpReplicationNum hdfs replication
     * @param block hdfs block
     * @param hosts DataNode hosts
     * @throws IOException block.getHosts()
     */
    public static void addHostsByBlock(Configuration conf, int ndpReplicationNum, BlockLocation block,
                                       List<String> hosts) throws IOException {
        for (String host : block.getHosts()) {
            if ("localhost".equals(host)) {
                List<String> dataNodeHosts = new ArrayList<>(Arrays.asList(
                        conf.get(NdpStatusManager.NDP_DATANODE_HOSTNAMES)
                                .split(NdpStatusManager.NDP_DATANODE_HOSTNAME_SEPARATOR)));
                if (dataNodeHosts.size() > ndpReplicationNum) {
                    hosts.addAll(dataNodeHosts.subList(0, ndpReplicationNum));
                } else {
                    hosts.addAll(dataNodeHosts);
                }
            } else {
                hosts.add(host);
            }
            if (ndpReplicationNum == hosts.size()) {
                return;
            }
        }
    }
}