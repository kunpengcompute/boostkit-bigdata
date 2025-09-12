/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json.common;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

/**
 * IntermediateDataSetIDPOJO
 *
 * @version 1.0.0
 * @since 2025/03/03
 */
public class IntermediateDataSetIDPOJO extends AbstractIDPOJO {
    public IntermediateDataSetIDPOJO() {
        super();
    }

    public IntermediateDataSetIDPOJO(long upperPart, long lowerPart) {
        super(upperPart, lowerPart);
    }

    public IntermediateDataSetIDPOJO(IntermediateDataSetID id) {
        super(id);
    }
}
