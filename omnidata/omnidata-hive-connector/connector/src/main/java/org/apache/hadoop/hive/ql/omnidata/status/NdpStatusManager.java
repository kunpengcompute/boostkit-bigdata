package org.apache.hadoop.hive.ql.omnidata.status;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.omnidata.config.NdpConf;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
     * @param ndpConf hive conf
     * @return hostname -> ndp status
     */
    public static Map<String, NdpStatusInfo> getNdpZookeeperData(NdpConf ndpConf) {
        if (ndpConf.getNdpZookeeperSecurityEnabled()) {
            enableKrb5(ndpConf);
        }
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(ndpConf.getNdpZookeeperQuorumServer())
                .sessionTimeoutMs(ndpConf.getNdpZookeeperSessionTimeout())
                .connectionTimeoutMs(ndpConf.getNdpZookeeperConnectionTimeout())
                .retryPolicy(new RetryForever(ndpConf.getNdpZookeeperRetryInterval()))
                .build();
        zkClient.start();
        Map<String, NdpStatusInfo> ndpMap = new HashMap<>();
        String parentPath = ndpConf.getNdpZookeeperStatusNode();
        try {
            // verify the path from ZooKeeper
            Stat stat = zkClient.checkExists().forPath(parentPath);
            if (stat == null) {
                LOG.error("OmniData Hive failed to get parent node from ZooKeeper");
                return ndpMap;
            }
        } catch (Exception e) {
            LOG.error("OmniData Hive failed to get host resources data from ZooKeeper", e);
            return ndpMap;
        }
        InterProcessMutex lock = new InterProcessMutex(zkClient, parentPath);
        try {
            if (lock.acquire(ndpConf.getNdpZookeeperRetryInterval(), TimeUnit.MILLISECONDS)) {
                List<String> childrenPaths = zkClient.getChildren().forPath(parentPath);
                ObjectMapper mapper = new ObjectMapper();
                for (String path : childrenPaths) {
                    if (!path.contains("-lock-")) {
                        byte[] data = zkClient.getData().forPath(parentPath + "/" + path);
                        NdpStatusInfo statusInfo = mapper.readValue(data, NdpStatusInfo.class);
                        ndpMap.put(path, statusInfo);
                    }
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

    private static void enableKrb5(NdpConf ndpConf) {
        System.setProperty(KRB5_LOGIN_CONF_KEY, ndpConf.getNdpZookeeperConfPath() + "/" + LOGIN_CONFIG_FILE);
        System.setProperty(KRB5_CONF_KEY, ndpConf.getNdpZookeeperConfPath() + "/" + KRB5_CONFIG_FILE);
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

}