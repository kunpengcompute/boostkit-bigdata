package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan;
// represent as json string in description


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;


/**
 * NexmarkFormatPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class NexmarkFormatPOJO {
    private String format;
    private int batchSize = 100;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Map<String, Object> configMap = new HashMap<>();

    public Map<String, Object> getConfigMap() {
        return configMap;
    }

    public void setConfigMap(Map<String, Object> configMap) {
        this.configMap = configMap;
    }


    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }


    @Override
    public String toString() {
        return "NexmarkFormatPOJO{"
                + "format='" + format + '\'' +
                '}';
    }
}
