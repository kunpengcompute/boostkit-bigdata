package com.huawei.bdsolution.loadsmetric.util;

import com.huawei.bdsolution.loadsmetric.dto.FixedSizeRingBuffer;
import com.huawei.bdsolution.loadsmetric.dto.LoadsRecordAverage;
import com.huawei.bdsolution.loadsmetric.dto.NodeLogicalResource;
import com.huawei.bdsolution.loadsmetric.dto.SortReport;
import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;

@Component
@PropertySource("${SPRING_CONFIG_LOCATION}")
public class MultiLoadResourceUsageSortPolicy {

    @Autowired
    UsageMetricsAggregator usageMetricsAggregator;

    @Value("${load.limit.cpu:-1.0}")
    private float cpuLimit;

    @Value("${load.limit.mem:-1.0}")
    private float memLimit;

    @Value("${load.limit.diskio:-1.0}")
    private float diskIoLimit;

    @Value("${load.limit.netio:-1.0}")
    private float netIoLimit;

    @Value("${load.weight.cpu:0}")
    private float cpuWeight;

    @Value("${load.weight.mem:0}")
    private float memWeight;

    @Value("${load.weight.diskio:0}")
    private float diskIoWeight;

    @Value("${load.weight.netio:0}")
    private float netIoWeight;

    @Value("${expiration.time:60}")
    private int expirationTime;

    @Value("${overload.filter.able:false}")
    private boolean isOverLoadAble;

    /**
     * Sort nodes by weightedUsage
     *
     * @param loadsRecordAverageList
     * @return LoadReport, contain sortedNodeHosts, overloadNodeHosts and expirationTime
     */
    public SortReport sortNodesWithLogical(List<LoadsRecordAverage> loadsRecordAverageList,
                                           Map<String, NodeLogicalResource> nodeLogicalResourceMap) {
        return sortNodes(loadsRecordAverageList, new LoadsRecordAverageWithLogicalComparator(nodeLogicalResourceMap));
    }

    /**
     * Sort nodes by weightedUsage
     *
     * @param loadsRecordAverageList
     * @param comparator
     * @return LoadReport, contain sortedNodeHosts, overloadNodeHosts and expirationTime
     */
    public SortReport sortNodes(List<LoadsRecordAverage> loadsRecordAverageList,
                                Comparator<LoadsRecordAverage> comparator) {
        List<String> overLoadHosts = new ArrayList<>();
        List<String> sortedHosts = new ArrayList<>();
        Collections.sort(loadsRecordAverageList, comparator);
        if (isOverLoadAble) {
            for (LoadsRecordAverage average : loadsRecordAverageList) {
                if (isOverLoad(average)) {
                    overLoadHosts.add(average.getHostName());
                } else {
                    sortedHosts.add(average.getHostName());
                }
            }
        } else {
            for (LoadsRecordAverage average : loadsRecordAverageList) {
                sortedHosts.add(average.getHostName());
            }
        }
        Instant expirationTimestamp = Instant.now().plusSeconds(expirationTime);
        SortReport report = new SortReport(sortedHosts, overLoadHosts, expirationTimestamp);
        return report;
    }

    /**
     * Determine whether the host is overloaded.
     *
     * @param loadsRecordAverage
     * @return
     */
    public boolean isOverLoad(LoadsRecordAverage loadsRecordAverage) {
        if (cpuLimit > 0 && loadsRecordAverage.getAvgCpuUsage() > cpuLimit) {
            return true;
        }
        if (memLimit > 0 && loadsRecordAverage.getAvgMemUsage() > memLimit) {
            return true;
        }
        if (diskIoLimit > 0 && loadsRecordAverage.getAvgDiskIoUsage() > diskIoLimit) {
            return true;
        }
        if (netIoLimit > 0 && loadsRecordAverage.getAvgNetIoUsage() > netIoLimit) {
            return true;
        }
        return false;
    }

    /**
     * Calculate the weighted usage.
     *
     * @param averageList
     * @return
     */
    public List<LoadsRecordAverage> calculateWeight(List<LoadsRecordAverage> averageList) {
        if (averageList.isEmpty()) {
            return averageList;
        }
        for (LoadsRecordAverage average : averageList) {
            float weightedUsage = average.getAvgCpuUsage() * cpuWeight +
                    average.getAvgMemUsage() * memWeight +
                    average.getAvgDiskIoUsage() * diskIoWeight +
                    average.getAvgNetIoUsage() * netIoWeight;
            average.setWeightedUsage(weightedUsage);
        }
        return averageList;
    }

    /**
     * calculate average usages for all host in given window size
     *
     * @param loadsRecordsCacheMap
     * @return
     */
    public List<LoadsRecordAverage> calculateAverage(Map<String, FixedSizeRingBuffer<LoadsRecords>> loadsRecordsCacheMap) {
        List<LoadsRecordAverage> loadsRecordAverageList = new ArrayList<>();
        for (Map.Entry<String, FixedSizeRingBuffer<LoadsRecords>> entry : loadsRecordsCacheMap.entrySet()) {
            String hostName = entry.getKey();
            FixedSizeRingBuffer<LoadsRecords> recordsBuffer = entry.getValue();
            Float avgCpuUsage = average(recordsBuffer, LoadsRecords::getCpuUsage);
            Float avgMemUsage = average(recordsBuffer, LoadsRecords::getMemUsage);
            Float avgDiskIoUsage = average(recordsBuffer, LoadsRecords::getDiskIoUsage);
            Float avgNetIoUsage = average(recordsBuffer, LoadsRecords::getNetIoUsage);
            LoadsRecordAverage averageRecord = new LoadsRecordAverage(
                    hostName,
                    avgCpuUsage,
                    avgMemUsage,
                    avgDiskIoUsage,
                    avgNetIoUsage
            );
            loadsRecordAverageList.add(averageRecord);
        }
        return loadsRecordAverageList;
    }

    /**
     * calculate average for given usage type and given host
     *
     * @param recordsBuffer
     * @param valueExtractor
     * @return
     */
    private Float average(FixedSizeRingBuffer<LoadsRecords> recordsBuffer, Function<LoadsRecords, Float> valueExtractor) {
        float sum = 0f;
        int count = recordsBuffer.getSize();
        for (int i = 0; i < count; i++) {
            LoadsRecords record = recordsBuffer.get(i);
            sum += valueExtractor.apply(record);
        }
        return count > 0 ? sum / count : 0f;
    }
}

class LoadsRecordAverageComparator implements Comparator<LoadsRecordAverage> {
    @Override
    public int compare(LoadsRecordAverage record1, LoadsRecordAverage record2) {
        return Float.compare(record1.getWeightedUsage(), record2.getWeightedUsage());
    }
}

class LoadsRecordAverageWithLogicalComparator implements Comparator<LoadsRecordAverage> {

    private final Map<String, NodeLogicalResource> nodeLogicalResourceMap;

    private static final NodeLogicalResource DEFAULT_NODE_LOGICAL_RESOURCE = new NodeLogicalResource();

    public LoadsRecordAverageWithLogicalComparator(Map<String, NodeLogicalResource> nodeLogicalResourceMap) {
        this.nodeLogicalResourceMap = nodeLogicalResourceMap;
    }

    @Override
    public int compare(LoadsRecordAverage record1, LoadsRecordAverage record2) {
        int cmp = 0;
        NodeLogicalResource logicalResource1 = nodeLogicalResourceMap.getOrDefault(record1.getHostName()
                , DEFAULT_NODE_LOGICAL_RESOURCE);
        NodeLogicalResource logicalResource2 = nodeLogicalResourceMap.getOrDefault(record2.getHostName()
                , DEFAULT_NODE_LOGICAL_RESOURCE);
        cmp = logicalResource1.compareTo(logicalResource2);
        if (cmp != 0) {
            return cmp;
        }
        return Float.compare(record1.getWeightedUsage(), record2.getWeightedUsage());
    }
}