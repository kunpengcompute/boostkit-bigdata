/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.streaming.api.graph;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.io.IOException;

/**
 * basic interface for validate execution plan
 *
 * @since 2025/05/06
 */
public interface StreamNodeExtraDescription {
    /**
     * check & set the description
     *
     * @param streamNode   streamNode
     * @param streamConfig streamConfig
     * @return Boolean Boolean
     */
    boolean setExtraDescription(StreamNode streamNode, StreamConfig streamConfig, StreamGraph streamGraph, JobType jobType) throws Exception;
}
