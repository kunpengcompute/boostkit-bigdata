package com.huawei.bdsolution.loadsmetric.entity;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.GenerationType;
import javax.persistence.Column;

@Entity
@Data
@Table(name = "usagemetrics")
public class UsageMetrics {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "cpu_avg")
    private String cpuAvg;

    @Column(name = "cpu_var")
    private String cpuVar;

    @Column(name = "mem_avg")
    private Float memAvg;

    @Column(name = "mem_var")
    private Float memVar;

    @Column(name = "disk_io_avg")
    private Float diskIoAvg;

    @Column(name = "disk_io_var")
    private Float diskIoVar;

    @Column(name = "net_io_avg")
    private Float netIoAvg;

    @Column(name = "net_io_var")
    private Float netIoVar;

    @Column(name = "time_stamp")
    private String timeStamp;

    public UsageMetrics() {
    }

    public UsageMetrics(String cpuAvg, String cpuVar,
                        Float memAvg, Float memVar,
                        Float diskIoAvg, Float diskIoVar,
                        Float netIoAvg, Float netIoVar,
                        String timeStamp) {
        this.cpuAvg = cpuAvg;
        this.cpuVar = cpuVar;
        this.memAvg = memAvg;
        this.memVar = memVar;
        this.diskIoAvg = diskIoAvg;
        this.diskIoVar = diskIoVar;
        this.netIoAvg = netIoAvg;
        this.netIoVar = netIoVar;
        this.timeStamp = timeStamp;
    }

}
