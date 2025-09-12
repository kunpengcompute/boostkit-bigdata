/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan;

import java.util.List;
import java.util.Properties;

/**
 * KafkaFormatPOJO
 *
 * @since 2025/4/8
 */
public class KafkaFormatPojo {
    private boolean batch;

    private String format;

    private boolean emitProgressiveWatermarks;

    private Properties properties;

    private String watermarkStrategy;

    private long outOfOrdernessMillis;

    private String deserializationSchema;

    private boolean hasMetadata;

    private List<String> outputNames;

    private List<String> outputTypes;

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public boolean isEmitProgressiveWatermarks() {
        return emitProgressiveWatermarks;
    }

    public void setEmitProgressiveWatermarks(boolean emitProgressiveWatermarks) {
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getWatermarkStrategy() {
        return watermarkStrategy;
    }

    public void setWatermarkStrategy(String watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    public long getOutOfOrdernessMillis() {
        return outOfOrdernessMillis;
    }

    public void setOutOfOrdernessMillis(long outOfOrdernessMillis) {
        this.outOfOrdernessMillis = outOfOrdernessMillis;
    }

    public String getDeserializationSchema() {
        return deserializationSchema;
    }

    public void setDeserializationSchema(String deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    public boolean isHasMetadata() {
        return hasMetadata;
    }

    public void setHasMetadata(boolean hasMetadata) {
        this.hasMetadata = hasMetadata;
    }

    public List<String> getOutputNames() {
        return outputNames;
    }

    public void setOutputNames(List<String> outputNames) {
        this.outputNames = outputNames;
    }

    public List<String> getOutputTypes() {
        return outputTypes;
    }

    public void setOutputTypes(List<String> outputTypes) {
        this.outputTypes = outputTypes;
    }
}
