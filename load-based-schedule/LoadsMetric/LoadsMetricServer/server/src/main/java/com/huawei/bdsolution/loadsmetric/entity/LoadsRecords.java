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
@Table(name = "loadsrecords")
public class LoadsRecords {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "ip_addr")
    private String ipAddr;

    @Column(name = "host_name")
    private String hostName;

    @Column(name = "cpu_usage")
    private Float cpuUsage;

    @Column(name = "mem_usage")
    private Float memUsage;

    @Column(name = "disk_io_usage")
    private Float diskIoUsage;

    @Column(name = "net_io_usage")
    private Float netIoUsage;

    @Column(name = "time_stamp")
    private String timeStamp;

    public LoadsRecords() {
    }

    public LoadsRecords(Float cpuUsage, Float memUsage, Float diskIoUsage, Float netIoUsage, String timeStamp) {
        this.cpuUsage = cpuUsage;
        this.memUsage = memUsage;
        this.diskIoUsage = diskIoUsage;
        this.netIoUsage = netIoUsage;
        this.timeStamp = timeStamp;
    }
}
