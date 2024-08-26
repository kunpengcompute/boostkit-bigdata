package com.huawei.bdsolution.loadsmetric.util;

import com.huawei.bdsolution.loadsmetric.LoadsMetricApplication;
import com.huawei.bdsolution.loadsmetric.dto.LoadsRecordAverage;
import com.huawei.bdsolution.loadsmetric.entity.UsageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
public class UsageMetricsAggregator {

    @Value("${client.heartbeat.interval:1000}")
    private int heartbeatInterval;

    private static final Logger LOG = LoggerFactory.getLogger(LoadsMetricApplication.class);
    private static final String DISK_IO_PERCENTAGE = "diskIoUsage";
    private static final String NET_IO_PERCENTAGE = "netIoUsage";
    private static final String CPU_PERCENTAGE = "cpuUsage";
    private static final String MEM_PERCENTAGE = "memUsage";
    private static final int MINUTE_SECONDS = 60;
    private static final int HOUR_SECONDS = 3600;
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    DecimalFormat df = new DecimalFormat("#.##");


    /**
     * calculate UsageMetrics from loadsRecordAverageList for all hosts
     * @param loadsRecordAverageList
     * @return
     */
    public UsageMetrics usageMetricsCalculate(List<LoadsRecordAverage> loadsRecordAverageList) {
        float cpuAvg = calculateGlobalAverage(loadsRecordAverageList, CPU_PERCENTAGE);
        float cpuVar = calculateGlobalVariance(loadsRecordAverageList, CPU_PERCENTAGE, cpuAvg);
        float memAvg = calculateGlobalAverage(loadsRecordAverageList, MEM_PERCENTAGE);
        float memVar = calculateGlobalVariance(loadsRecordAverageList, MEM_PERCENTAGE, memAvg);
        float diskIoAvg = calculateGlobalAverage(loadsRecordAverageList, DISK_IO_PERCENTAGE);
        float diskIoVar = calculateGlobalVariance(loadsRecordAverageList, DISK_IO_PERCENTAGE, diskIoAvg);
        float netIoAvg = calculateGlobalAverage(loadsRecordAverageList, NET_IO_PERCENTAGE);
        float netIoVar = calculateGlobalVariance(loadsRecordAverageList, NET_IO_PERCENTAGE, netIoAvg);

        return new UsageMetrics(
                Float.valueOf(df.format(cpuAvg)),
                Float.valueOf(df.format(cpuVar)),
                Float.valueOf(df.format(memAvg)),
                Float.valueOf(df.format(memVar)),
                Float.valueOf(df.format(diskIoAvg)),
                Float.valueOf(df.format(diskIoVar)),
                Float.valueOf(df.format(netIoAvg)),
                Float.valueOf(df.format(netIoVar)),
                roundTime());
    }

    private String roundTime() {
        LocalDateTime time = LocalDateTime.now();
        int intervalSeconds = heartbeatInterval / 1000;
        // when intervalSeconds in (1,3600] do round time
        // such as intervalSeconds is 10, now is 10:00:03, after round time is 10:00:00 for ui present
        // such as intervalSeconds is 600, now is 10:12:03, after round time is 10:10:00 for ui present
        if (intervalSeconds > 1 && intervalSeconds <= HOUR_SECONDS) {
            int second = time.getSecond();
            int minute = time.getMinute();
            second = second + minute * MINUTE_SECONDS;
            int roundedSecond = second / intervalSeconds * intervalSeconds;
            minute = roundedSecond / MINUTE_SECONDS;
            second = roundedSecond % MINUTE_SECONDS;
            time = time.withMinute(minute).withSecond(second);
        }
        return time.format(formatter);
    }

    /**
     * Calculate the global average value at a time point.
     * @param loadsRecordAverageList
     * @param resourceType
     * @return
     */
    private float calculateGlobalAverage(List<LoadsRecordAverage> loadsRecordAverageList, String resourceType) {
        float sum = 0.0f;
        for (LoadsRecordAverage recordAverage : loadsRecordAverageList) {
            sum += getUsageByType(resourceType, recordAverage);
        }
        return sum / loadsRecordAverageList.size();
    }

    /**
     * Calculate the global variance value at a time point
     * @param loadsRecordAverageList
     * @param resourceType
     * @param average
     * @return
     */
    private float calculateGlobalVariance(List<LoadsRecordAverage> loadsRecordAverageList, String resourceType, float average) {
        float varianceSum = 0.0f;
        for (LoadsRecordAverage recordAverage : loadsRecordAverageList) {
            float usage = getUsageByType(resourceType, recordAverage);
            varianceSum += Math.pow(usage - average, 2);
        }
        return varianceSum / loadsRecordAverageList.size();
    }

    private float getUsageByType(String resourceType, LoadsRecordAverage loadsRecordAverage) {
        switch (resourceType) {
            case DISK_IO_PERCENTAGE:
                return loadsRecordAverage.getAvgDiskIoUsage();
            case NET_IO_PERCENTAGE:
                return loadsRecordAverage.getAvgNetIoUsage();
            case CPU_PERCENTAGE:
                return loadsRecordAverage.getAvgCpuUsage();
            case MEM_PERCENTAGE:
                return loadsRecordAverage.getAvgMemUsage();
            default:
                return 0.0f;
        }
    }

}
