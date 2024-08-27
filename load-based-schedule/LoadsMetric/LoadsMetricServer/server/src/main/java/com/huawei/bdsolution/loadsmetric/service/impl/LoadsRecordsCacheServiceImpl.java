package com.huawei.bdsolution.loadsmetric.service.impl;

import com.huawei.bdsolution.loadsmetric.dto.FixedSizeRingBuffer;
import com.huawei.bdsolution.loadsmetric.dto.NodeLogicalResource;
import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import com.huawei.bdsolution.loadsmetric.service.LoadsRecordsCacheService;
import com.huawei.bdsolution.loadsmetric.service.NodeHeartbeatService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LoadsRecordsCacheServiceImpl implements LoadsRecordsCacheService {

    @Autowired
    NodeHeartbeatService nodeHeartbeatService;

    @Value("${time.windowsize:5}")
    private int windowSize;

    @Value("${logical-resource-usage.levels.mem:10}")
    private int memUsageLevels;

    @Value("${logical-resource-usage.levels.vcores:10}")
    private int vcoresUsageLevels;

    private Map<String, FixedSizeRingBuffer<LoadsRecords>> loadsRecordsCacheMap = new ConcurrentHashMap<>();

    private Map<String, NodeLogicalResource> nodeLogicalResourceMap = new ConcurrentHashMap<>();

    /**
     * add loadsRecords to loadsRecordsCacheMap
     * @param loadsRecords
     */
    @Override
    public void addLoadsRecordsCache(LoadsRecords loadsRecords) {
        String hostName = loadsRecords.getHostName();
        loadsRecordsCacheMap.get(hostName).add(loadsRecords);
    }

    /**
     * return loadsRecordsCacheMap
     * @return loadsRecordsCacheMap
     */
    @Override
    public Map<String, FixedSizeRingBuffer<LoadsRecords>> getLoadsRecordsCacheMap() {
        return loadsRecordsCacheMap;
    }

    /**
     * calcuMemAndVcoresUsageLevel and add to  nodeLogicalResourceMap
     * @param nodeLogicalResource
     */
    @Override
    public void addNodeLogicalResource(NodeLogicalResource nodeLogicalResource) {
        nodeLogicalResource.calcuMemAndVcoresUsageLevel(memUsageLevels, vcoresUsageLevels);
        nodeLogicalResourceMap.put(nodeLogicalResource.getHostName(), nodeLogicalResource);
    }

    /**
     * return nodeLogicalResourceMap
     * @return nodeLogicalResourceMap
     */
    @Override
    public Map<String, NodeLogicalResource> getNodeLogicalResourceMap() {
        return nodeLogicalResourceMap;
    }


    /**
     * get every host's loads metric usage info for Prometheus
     *
     * @return loads metric usage info
     */
    @Override
    public String getRecordsByCurrTimeForPrometheus() {
        Collection<FixedSizeRingBuffer<LoadsRecords>> fixedSizeRingBuffers = getLoadsRecordsCacheMap().values();
        StringBuilder res = new StringBuilder();
        res.append("# HELP loads metrics detail:curr usage for host\n");
        res.append("# TYPE loads_metrics_detail gauge\n");
        for (FixedSizeRingBuffer<LoadsRecords> fixedSizeRingBuffer : fixedSizeRingBuffers) {
            int size = fixedSizeRingBuffer.getSize();
            if (size <= 0) {
                continue;
            }
            LoadsRecords loadsRecords = fixedSizeRingBuffer.get(size - 1);
            res.append(String.format("loads_metrics_detail{host=\"%s\",type=\"%s\"} %s\n",
                    loadsRecords.getHostName(), "cpu", loadsRecords.getCpuUsage()));
            res.append(String.format("loads_metrics_detail{host=\"%s\",type=\"%s\"} %s\n",
                    loadsRecords.getHostName(), "mem", loadsRecords.getMemUsage()));
            res.append(String.format("loads_metrics_detail{host=\"%s\",type=\"%s\"} %s\n",
                    loadsRecords.getHostName(), "disk_io", loadsRecords.getDiskIoUsage()));
            res.append(String.format("loads_metrics_detail{host=\"%s\",type=\"%s\"} %s\n",
                    loadsRecords.getHostName(), "net_io", loadsRecords.getNetIoUsage()));
        }
        res.append("# EOF");
        return res.toString();
    }

    @PostConstruct
    public void initLoadsRecordsCacheMap() {
        Set<String> hostSet = nodeHeartbeatService.getAllNodeHosts();
        for (String hostName : hostSet) {
            loadsRecordsCacheMap.put(hostName, new FixedSizeRingBuffer<>(windowSize));
        }
    }
}
