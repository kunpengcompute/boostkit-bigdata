package com.huawei.boostkit.omnituning.tez.data;

public enum GroupName {
    FileSystemCounters("org.apache.tez.common.counters.FileSystemCounter"),
    TezTask("org.apache.tez.common.counters.TaskCounter"),
    TezDag("org.apache.tez.common.counters.DAGCounter");

    final String name;
    GroupName(String name) {
        this.name = name;
    }
}
