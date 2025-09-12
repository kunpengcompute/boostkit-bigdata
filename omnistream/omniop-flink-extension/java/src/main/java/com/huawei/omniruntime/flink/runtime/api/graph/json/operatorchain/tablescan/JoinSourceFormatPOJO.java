/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

/**
 * JoinSourceFormatPOJO
 *
 * @since 2025/4/27
 */
public class JoinSourceFormatPOJO {
    private String format;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Map<String, Object> configMap = new HashMap<>();

    public Map<String, Object> getConfigMap() {
        return configMap;
    }

    public void setConfigMap(Map<String, Object> configMap) {
        this.configMap = configMap;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public String toString() {
        return "MTJoinSourceFormatPOJO{"
                + "format='" + format + '\''
                + '}';
    }
}
