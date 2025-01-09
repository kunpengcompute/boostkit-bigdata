package com.huawei.bdsolution.loadsmetric.entity;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;

@Component
@ConfigurationProperties(prefix = "")
@Validated
public class AppConfig {

    @Value("${time.windowsize:5}")
    @Max(value = 10, message = "The maximum value of windowSize is 10")
    private int windowSize;

    @Value("${logical-resource-usage.levels.mem:10}")
    @Max(value = 10, message = "The maximum value of memUsageLevels is 10")
    @Min(value = 1, message = "The minimum value of memUsageLevels is 1")
    private int memUsageLevels;

    @Value("${logical-resource-usage.levels.vcores:10}")
    @Max(value = 10, message = "The maximum value of vcoresUsageLevels is 10")
    @Min(value = 1, message = "The minimum value of vcoresUsageLevels is 1")
    private int vcoresUsageLevels;

    @Value("${load.limit.cpu:-1.0}")
    @Max(value = 100, message = "The maximum value of cpuLimit is 100")
    private float cpuLimit;

    @Value("${load.limit.mem:-1.0}")
    @Max(value = 100, message = "The maximum value of memLimit is 100")
    private float memLimit;

    @Value("${load.limit.diskio:-1.0}")
    @Max(value = 100, message = "The maximum value of diskIoLimit is 100")
    private float diskIoLimit;

    @Value("${load.limit.netio:-1.0}")
    @Max(value = 100, message = "The maximum value of netIoLimit is 100")
    private float netIoLimit;

    @Value("${load.weight.cpu:0}")
    @Max(value = 1, message = "The maximum value of cpuWeight is 1")
    @Min(value = 0, message = "The minimum value of cpuWeight is 0")
    private float cpuWeight;

    @Value("${load.weight.mem:0}")
    @Max(value = 1, message = "The maximum value of memWeight is 1")
    @Min(value = 0, message = "The minimum value of memWeight is 0")
    private float memWeight;

    @Value("${load.weight.diskio:0}")
    @Max(value = 1, message = "The maximum value of diskIoWeight is 1")
    @Min(value = 0, message = "The minimum value of diskIoWeight is 0")
    private float diskIoWeight;

    @Value("${load.weight.netio:0}")
    @Max(value = 1, message = "The maximum value of netIoWeight is 1")
    @Min(value = 0, message = "The minimum value of netIoWeight is 0")
    private float netIoWeight;

    @Value("${node-exporter.pull.port}")
    @Pattern(regexp = "\\d+", message = "The port number can contain only digits")
    private String nodeExporterPullPort;

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int getMemUsageLevels() {
        return memUsageLevels;
    }

    public void setMemUsageLevels(int memUsageLevels) {
        this.memUsageLevels = memUsageLevels;
    }

    public int getVcoresUsageLevels() {
        return vcoresUsageLevels;
    }

    public void setVcoresUsageLevels(int vcoresUsageLevels) {
        this.vcoresUsageLevels = vcoresUsageLevels;
    }

    public float getCpuLimit() {
        return cpuLimit;
    }

    public void setCpuLimit(float cpuLimit) {
        this.cpuLimit = cpuLimit;
    }

    public float getMemLimit() {
        return memLimit;
    }

    public void setMemLimit(float memLimit) {
        this.memLimit = memLimit;
    }

    public float getDiskIoLimit() {
        return diskIoLimit;
    }

    public void setDiskIoLimit(float diskIoLimit) {
        this.diskIoLimit = diskIoLimit;
    }

    public float getNetIoLimit() {
        return netIoLimit;
    }

    public void setNetIoLimit(float netIoLimit) {
        this.netIoLimit = netIoLimit;
    }

    public float getCpuWeight() {
        return cpuWeight;
    }

    public void setCpuWeight(float cpuWeight) {
        this.cpuWeight = cpuWeight;
    }

    public float getMemWeight() {
        return memWeight;
    }

    public void setMemWeight(float memWeight) {
        this.memWeight = memWeight;
    }

    public float getDiskIoWeight() {
        return diskIoWeight;
    }

    public void setDiskIoWeight(float diskIoWeight) {
        this.diskIoWeight = diskIoWeight;
    }

    public float getNetIoWeight() {
        return netIoWeight;
    }

    public void setNetIoWeight(float netIoWeight) {
        this.netIoWeight = netIoWeight;
    }

    public String getNodeExporterPullPort() {
        return nodeExporterPullPort;
    }

    public void setNodeExporterPullPort(String nodeExporterPullPort) {
        this.nodeExporterPullPort = nodeExporterPullPort;
    }
}
