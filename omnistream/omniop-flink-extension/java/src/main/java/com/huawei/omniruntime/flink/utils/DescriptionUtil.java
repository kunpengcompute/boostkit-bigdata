/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.utils;

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * manage the system's configuration information
 *
 * @since 2025/04/22
 * @version 1.0.0
 */
public class DescriptionUtil {
    /**
     * getFieldType
     *
     * @param fieldType fieldType
     * @return String
     */
    public static String getFieldType(LogicalType fieldType) {
        LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
        String typeName = typeRoot.toString();
        if (typeRoot == LogicalTypeRoot.VARCHAR) {
            if (fieldType instanceof VarCharType) {
                VarCharType varcharType = (VarCharType) fieldType;
                typeName += "(" + varcharType.getLength() + ")";
            }
        }
        if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE
                || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            // Cast to TimestampType to access precision
            if (fieldType instanceof TimestampType) {
                int precision = ((TimestampType) fieldType).getPrecision();
                typeName += "(" + precision + ")";
            }
        }
        if (typeRoot == LogicalTypeRoot.DECIMAL) {
            if (fieldType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) fieldType;
                Integer precision = decimalType.getPrecision();
                Integer scale = decimalType.getScale();
                if (precision > 19) {
                    typeName += "128";
                } else {
                    typeName += "64";
                }
                typeName += "(" + precision + "," + scale + ")";
            }
        }
        return typeName;
    }
}
