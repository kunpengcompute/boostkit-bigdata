/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.Instant;
import java.util.List;

@Data
public class SortReport {

    @JsonProperty("sorted_node_hosts")
    private String sortedNodeHosts;

    @JsonProperty("overload_node_hosts")
    private String overloadNodeHosts;

    @JsonProperty("expiration_time")
    private long expirationTime;

    public SortReport(List<String> sortedHosts, List<String> overLoadHosts, Instant expirationTimestamp) {
        this.sortedNodeHosts = String.join(",", sortedHosts);
        this.overloadNodeHosts = String.join(",", overLoadHosts);
        this.expirationTime = expirationTimestamp.toEpochMilli();
    }
}
