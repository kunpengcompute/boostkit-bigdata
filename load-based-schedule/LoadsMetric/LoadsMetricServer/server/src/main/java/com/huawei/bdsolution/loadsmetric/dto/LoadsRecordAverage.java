/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.dto;

import lombok.Data;

import java.beans.ConstructorProperties;

@Data
public class LoadsRecordAverage {
    private String hostName;
    private Float avgCpuUsage;
    private Float avgMemUsage;
    private Float avgDiskIoUsage;
    private Float avgNetIoUsage;
    private Float weightedUsage;
    private String timeStamp;

    @ConstructorProperties({"host_name","avg_cpu_usage","avg_mem_usage","avg_disk_io_usage","avg_net_io_usage"})
    public LoadsRecordAverage(String hostName, Float avgCpuUsage, Float avgMemUsage, Float avgDiskIoUsage, Float avgNetIoUsage) {
        this.hostName = hostName;
        this.avgCpuUsage = avgCpuUsage;
        this.avgMemUsage = avgMemUsage;
        this.avgDiskIoUsage = avgDiskIoUsage;
        this.avgNetIoUsage = avgNetIoUsage;
    }
}
