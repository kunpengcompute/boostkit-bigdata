package org.apache.spark.sql;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConverters;
import scala.collection.Map$;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * PushDownManager Operate zookeeper data
 */
public class PushDownManager {
    private static final Logger LOG = LoggerFactory.getLogger(PushDownManager.class);

    private static final double TASK_THRESHOLD = 0.8;

    private static final int ZOOKEEPER_RETRY_INTERVAL_MS = 1000;

    public scala.collection.Map<String, String> getZookeeperData(
        int timeOut, String parentPath, String zkAddress) throws Exception {
        Map<String, String> fpuMap = new HashMap<>();
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkAddress)
                .sessionTimeoutMs(timeOut)
                .connectionTimeoutMs(timeOut)
                .retryPolicy(new RetryForever(ZOOKEEPER_RETRY_INTERVAL_MS))
                .build();
        zkClient.start();
        Map<String, PushDownData> pushDownInfoMap = new HashMap<>();
        InterProcessMutex lock = new InterProcessMutex(zkClient, parentPath);
        if (lock.acquire(ZOOKEEPER_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
            try {
                List<String> childrenPaths = zkClient.getChildren().forPath(parentPath);
                ObjectMapper mapper = new ObjectMapper();
                for (String path : childrenPaths) {
                    if (!path.contains("-lock-")) {
                        byte[] data = zkClient.getData().forPath(parentPath + "/" + path);
                        PushDownData statusInfo = mapper.readValue(data, PushDownData.class);
                        fpuMap.put(path, statusInfo.getDatanodeHost());
                        pushDownInfoMap.put(path, statusInfo);
                    }
                }
                if (checkAllPushDown(pushDownInfoMap)) {
                    return javaMapToScala(fpuMap);
                } else {
                    return javaMapToScala(new HashMap<>());
                }
            } catch (InterruptedException | IOException | KeeperException e) {
                LOG.error("Fail to connect ZooKeeper", e);
            } finally {
                lock.release();
                zkClient.close();
            }
        } return javaMapToScala(new HashMap<>());
    }

    /**
     * OmniDataServer: Determine whether push-down is required.
     */
    private boolean checkAllPushDown(Map<String, PushDownData> fpuStatusInfoMap) {
        if (fpuStatusInfoMap.size() == 0) {
            LOG.info("Fail to Push Down, the number of omni-data-server is 0.");
            return false;
        }
        for (Map.Entry<String, PushDownData> fpuStatusInfo : fpuStatusInfoMap.entrySet()) {
            if (!checkPushDown(fpuStatusInfo.getValue())) {
                return false;
            }
        }
        return true;
    }

    private boolean checkPushDown(PushDownData pushDownData) {
        int runningTask = pushDownData.getRunningTasks();
        if (runningTask > pushDownData.getMaxTasks() * TASK_THRESHOLD) {
            LOG.info("Fail to Push Down, the number of runningTask is {}.", runningTask);
            return false;
        }
        return true;
    }

    private static scala.collection.Map<String, String> javaMapToScala(Map kafkaParams) {
        scala.collection.Map scalaMap = JavaConverters.mapAsScalaMap(kafkaParams);
        Object objTest = Map$.MODULE$.<String, String>newBuilder().$plus$plus$eq(scalaMap.toSeq());
        Object resultTest = ((scala.collection.mutable.Builder) objTest).result();
        scala.collection.Map<String, String> retMap = (scala.collection.Map) resultTest;
        return retMap;
    }
}
