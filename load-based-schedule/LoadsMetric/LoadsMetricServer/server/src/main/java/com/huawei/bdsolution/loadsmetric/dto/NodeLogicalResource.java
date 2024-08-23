package com.huawei.bdsolution.loadsmetric.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NodeLogicalResource implements Comparable<NodeLogicalResource>{
    @JsonProperty("host_name")
    private String hostName;

    @JsonProperty("allocated_mem")
    private long allocatedMem;

    @JsonProperty("total_mem")
    private long totalMem;

    @JsonProperty("allocated_vcores")
    private long allocatedVcores;

    @JsonProperty("total_vcores")
    private long totalVcores;

    private int memUsageLevel;

    private int vcoresUsageLevel;

    public void calcuMemAndVcoresUsageLevel(int memUsageLevels,int vcoresUsageLevels){
        memUsageLevel = (int) (allocatedMem * memUsageLevels / totalMem);
        vcoresUsageLevel = (int) (allocatedVcores * vcoresUsageLevels /totalVcores);
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public long getAllocatedMem() {
        return allocatedMem;
    }

    public void setAllocatedMem(long allocatedMem) {
        this.allocatedMem = allocatedMem;
    }

    public long getTotalMem() {
        return totalMem;
    }

    public void setTotalMem(long totalMem) {
        this.totalMem = totalMem;
    }

    public long getAllocatedVcores() {
        return allocatedVcores;
    }

    public void setAllocatedVcores(long allocatedVcores) {
        this.allocatedVcores = allocatedVcores;
    }

    public long getTotalVcores() {
        return totalVcores;
    }

    public void setTotalVcores(long totalVcores) {
        this.totalVcores = totalVcores;
    }

    public int getMemUsageLevel() {
        return memUsageLevel;
    }

    public void setMemUsageLevel(int memUsageLevel) {
        this.memUsageLevel = memUsageLevel;
    }

    public int getVcoresUsageLevel() {
        return vcoresUsageLevel;
    }

    public void setVcoresUsageLevel(int vcoresUsageLevel) {
        this.vcoresUsageLevel = vcoresUsageLevel;
    }


    @Override
    public int compareTo(NodeLogicalResource other) {
        if (other == null){
            return 1;
        }
        int cmp = getMemUsageLevel() - other.getMemUsageLevel();
        if (cmp == 0){
            cmp = getVcoresUsageLevel() - other.getVcoresUsageLevel();
        }
        return cmp;
    }
}
