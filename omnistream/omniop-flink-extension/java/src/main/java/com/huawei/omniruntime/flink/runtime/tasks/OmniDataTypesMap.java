package com.huawei.omniruntime.flink.runtime.tasks;

import java.util.HashMap;
import java.util.Map;

public class OmniDataTypesMap {
    public static final Map<String, Integer> dataTypeMap = new HashMap<>();

    static {
        dataTypeMap.put("CHAR", 0);
        dataTypeMap.put("VARCHAR", 1);
        dataTypeMap.put("BOOLEAN", 2);
        dataTypeMap.put("DECIMAL", 5);
        dataTypeMap.put("TINYINT", 6);
        dataTypeMap.put("SMALLINT", 7);
        dataTypeMap.put("INTEGER", 8);
        dataTypeMap.put("BIGINT", 9);
        dataTypeMap.put("INTERVAL_DAY", 9);
        dataTypeMap.put("FLOAT", 10);
        dataTypeMap.put("DOUBLE", 11);
        dataTypeMap.put("DATE", 12);
        dataTypeMap.put("TIMESTAMP", 14);
        dataTypeMap.put("ARRAY", 17);
        dataTypeMap.put("MAP", 19);
        dataTypeMap.put("ROW", 20);
        dataTypeMap.put("INVALID", 21);
    }
}
//todo: add more datatypes to the map
//RexNode sqlTypeName:
//BOOLEAN, INTEGER, VARCHAR, DATE, TIME, TIMESTAMP, NULL, DECIMAL,
//ANY, CHAR, BINARY, VARBINARY, TINYINT, SMALLINT, BIGINT, REAL,
//DOUBLE, SYMBOL, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH,
//INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE,
//INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE,
//INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND,
//INTERVAL_SECOND, TIME_WITH_LOCAL_TIME_ZONE, TIME_TZ,
//TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_TZ,
//FLOAT, MULTISET, DISTINCT, STRUCTURED, ROW, CURSOR, COLUMN_LIST
//OMNIDataTypes:
//OMNI_NONE = 0,
//OMNI_INT = 1,
//OMNI_LONG = 2,
//OMNI_DOUBLE = 3,
//OMNI_BOOLEAN = 4,
//OMNI_SHORT = 5,
//OMNI_DECIMAL64 = 6,
//OMNI_DECIMAL128 = 7,
//OMNI_DATE32 = 8,
//OMNI_DATE64 = 9,
//OMNI_TIME32 = 10,
//OMNI_TIME64 = 11,
//OMNI_TIMESTAMP = 12,
//OMNI_INTERVAL_MONTHS = 13,
//OMNI_INTERVAL_DAY_TIME = 14,
//OMNI_VARCHAR = 15,
//OMNI_CHAR = 16,
//OMNI_CONTAINER = 17,
//OMNI_INVALID
