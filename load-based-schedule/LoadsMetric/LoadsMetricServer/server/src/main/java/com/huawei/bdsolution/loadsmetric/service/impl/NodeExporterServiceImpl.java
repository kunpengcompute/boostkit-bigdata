/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.service.impl;

import com.huawei.bdsolution.loadsmetric.dto.FixedSizeRingBuffer;
import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import com.huawei.bdsolution.loadsmetric.service.LoadsRecordsCacheService;
import com.huawei.bdsolution.loadsmetric.service.NodeExporterService;
import com.huawei.bdsolution.loadsmetric.service.NodeHeartbeatService;
import com.huawei.bdsolution.loadsmetric.util.NodeExporterPullThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class NodeExporterServiceImpl implements NodeExporterService {

    private static final Logger LOG = LoggerFactory.getLogger(NodeExporterServiceImpl.class);
    @Autowired
    NodeHeartbeatService nodeHeartbeatService;

    @Autowired
    LoadsRecordsCacheService loadsRecordsCacheService;

    @Value("${client.heartbeat.interval:1000}")
    private int heartbeatInterval;

    @Value("${net.compute.type:max}")
    private String netComputeType;

    @Value("${node-exporter.pull.enable:false}")
    private boolean nodeExporterPullEnable;

    @Value("${node-exporter.pull.protocol}")
    private String nodeExporterPullProtocol;

    @Value("${node-exporter.pull.port}")
    private String nodeExporterPullPort;

    @Value("5")
    private int windowSize;

    private final String NODE_EXPORTER_PULL_PATH = "/metrics?" +
            "collect[]=cpu&collect[]=meminfo&collect[]=diskstats&collect[]=filesystem" +
            "&collect[]=netclass&collect[]=netdev";

    @Value("${node-exporter.pull.thread-pool.size:1000}")
    private int nodeExporterPullThreadPollSize;

    private final List<NodeExporterPullThread> nodeExporterPullThreads = new ArrayList<>();

    private ExecutorService executorService;

    @PostConstruct
    public void init() {
        if (!nodeExporterPullEnable) {
            return;
        }

        if (!StringUtils.hasLength(nodeExporterPullProtocol)
                || !nodeExporterPullProtocol.matches("http")) {
            throw new RuntimeException(nodeExporterPullProtocol + " is invalid, eg: http");
        }
        if (!StringUtils.hasLength(nodeExporterPullPort)
                || !nodeExporterPullPort.matches("\\d+")) {
            throw new RuntimeException(nodeExporterPullPort + " is invalid, eg: 9100");
        }

        Set<String> hostSet = nodeHeartbeatService.getAllNodeHosts();
        executorService = Executors.newFixedThreadPool(nodeExporterPullThreadPollSize);
        for (String host : hostSet) {
            nodeExporterPullThreads.add(
                    new NodeExporterPullThread(
                            // eg: http://server1:9100/metrics
                            String.format("%s://%s:%s/%s",
                                    nodeExporterPullProtocol, host, nodeExporterPullPort, NODE_EXPORTER_PULL_PATH),
                            host, heartbeatInterval, netComputeType));
        }
    }

    @Scheduled(fixedRateString = "${client.heartbeat.interval:1000}", initialDelay = 1000)
    @Override
    public void pullLoadsRecords() {
        if (!nodeExporterPullEnable) {
            return;
        }

        for (NodeExporterPullThread thread : nodeExporterPullThreads) {
            Future<LoadsRecords> future = executorService.submit(thread);
            try {
                LoadsRecords loadsRecords = future.get();
                if (!loadsRecordsCacheService.getLoadsRecordsCacheMap().containsKey(loadsRecords.getHostName())){
                    loadsRecordsCacheService.getLoadsRecordsCacheMap()
                            .put(loadsRecords.getHostName(), new FixedSizeRingBuffer<>(windowSize));
                }
                loadsRecordsCacheService.addLoadsRecordsCache(loadsRecords);
                nodeHeartbeatService.updateNodeHeartbeatTime(loadsRecords);

            } catch (Exception e) {
                LOG.error("future get failed for :", e);
                loadsRecordsCacheService.getLoadsRecordsCacheMap().remove(thread.getHostName());
            }
        }
    }
}
