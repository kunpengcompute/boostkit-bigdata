package com.huawei.bdsolution.loadsmetric.util;

import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import lombok.Data;

import java.util.concurrent.Callable;

@Data
public class NodeExporterPullThread implements Callable<LoadsRecords> {
    private String nodeExporterUrl;
    private String hostName;
    private int heartbeatInterval;
    private String netComputeType;
    private SysInfoNodeExporter sysInfo;

    public NodeExporterPullThread(String nodeExporterUrl,
                                  String hostName,
                                  int heartbeatInterval,
                                  String netComputeType) {
        this.nodeExporterUrl = nodeExporterUrl;
        this.hostName = hostName;
        this.heartbeatInterval = heartbeatInterval;
        this.netComputeType = netComputeType;
        this.sysInfo = new SysInfoNodeExporter(this.nodeExporterUrl);
    }

    @Override
    public LoadsRecords call() throws Exception {
        sysInfo.requestNodeExporter();
        Float cpuUsagePercentage = sysInfo.formatFloatUsage(sysInfo.getCpuUsagePercentage());
        Float phyMemUsagePercentage = sysInfo.formatFloatUsage(sysInfo.getPhyMemUsagePercentage());
        Float netUsagePercentage = sysInfo.formatFloatUsage(sysInfo.getNetUsagePercentage(netComputeType));
        Float ioUsagePercentage = sysInfo.formatFloatUsage(sysInfo.getIoUsagePercentage(null));
        String roundTime = sysInfo.roundTime(heartbeatInterval);
        LoadsRecords loadsRecords = new LoadsRecords();
        loadsRecords.setHostName(hostName);
        loadsRecords.setCpuUsage(cpuUsagePercentage);
        loadsRecords.setMemUsage(phyMemUsagePercentage);
        loadsRecords.setDiskIoUsage(ioUsagePercentage);
        loadsRecords.setNetIoUsage(netUsagePercentage);
        loadsRecords.setTimeStamp(roundTime);
        return loadsRecords;
    }
}
