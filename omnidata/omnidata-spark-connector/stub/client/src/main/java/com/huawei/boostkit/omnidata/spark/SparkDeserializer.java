/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.spark;

import com.huawei.boostkit.omnidata.decode.Deserializer;
import com.huawei.boostkit.omnidata.type.DecodeType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;


/**
 * Deserialize serialized page to spark writableColumnVector array
 *
 * @since 2021-03-30
 */
public class SparkDeserializer implements Deserializer<WritableColumnVector[]> {

    public SparkDeserializer(DecodeType[] columnTypes, int[] columnOrders) {
    }
}

