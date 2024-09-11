/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.service.impl;

import com.huawei.bdsolution.loadsmetric.dto.FixedSizeRingBuffer;
import com.huawei.bdsolution.loadsmetric.dto.LoadsRecordAverage;
import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import com.huawei.bdsolution.loadsmetric.entity.UsageMetrics;
import com.huawei.bdsolution.loadsmetric.service.LoadsRecordsCacheService;
import com.huawei.bdsolution.loadsmetric.service.UsageMetricsService;
import com.huawei.bdsolution.loadsmetric.util.MultiLoadResourceUsageSortPolicy;
import com.huawei.bdsolution.loadsmetric.util.UsageMetricsAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@PropertySource("${SPRING_CONFIG_LOCATION}")
public class UsageMetricsServiceImpl implements UsageMetricsService {

    @Autowired
    private UsageMetricsAggregator usageMetricsAggregator;

    @Autowired
    private MultiLoadResourceUsageSortPolicy multiLoadResourceUsageSortPolicy;

    @Autowired
    private LoadsRecordsCacheService loadsRecordsCacheService;

    private static final Logger LOG = LoggerFactory.getLogger(UsageMetricsServiceImpl.class);


    /**
     * get cluster's usage metric info for Prometheus
     *
     * @return loads metric usage info
     */
    @Override
    public String getUsageMetricsForPrometheus() {
        Map<String, FixedSizeRingBuffer<LoadsRecords>> loadsRecordsCacheMap = loadsRecordsCacheService.getLoadsRecordsCacheMap();
        UsageMetrics usageMetrics = calcuUsageMetrics(loadsRecordsCacheMap);
        StringBuilder res = new StringBuilder();
        res.append("# HELP loads metrics:curr usage avg/var for cluster\n");
        res.append("# TYPE loads_metrics gauge\n");

        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "cpu_avg", usageMetrics.getCpuAvg()));
        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "cpu_var", usageMetrics.getCpuVar()));
        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "mem_avg", usageMetrics.getMemAvg()));
        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "mem_var", usageMetrics.getMemVar()));
        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "disk_io_avg", usageMetrics.getDiskIoAvg()));
        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "disk_io_var", usageMetrics.getDiskIoVar()));
        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "net_io_avg", usageMetrics.getNetIoAvg()));
        res.append(String.format("loads_metrics{type=\"%s\"} %s\n", "net_io_var", usageMetrics.getNetIoVar()));
        res.append("# EOF");
        return res.toString();
    }

    @Override
    public UsageMetrics calcuUsageMetrics(Map<String, FixedSizeRingBuffer<LoadsRecords>> loadsRecordsCacheMap) {
        try {
            List<LoadsRecordAverage> loadsRecordAverageList = multiLoadResourceUsageSortPolicy.calculateAverage(loadsRecordsCacheMap);
            UsageMetrics usageMetrics = usageMetricsAggregator.usageMetricsCalculate(loadsRecordAverageList);
            return usageMetrics;
        } catch (NumberFormatException e) {
            LOG.warn("NodeExporter is stopped.");
            return new UsageMetrics();
        }
    }
}
