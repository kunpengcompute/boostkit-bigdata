package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractValidateOperatorStrategy {

    public static final Map<String, Integer> RexTypeToIdMap = new HashMap<>();
    protected static final Set<String> SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INTEGER",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "VARCHAR(2147483647)",
            "VARCHAR(2000)",
            "VARCHAR(9)",
            "STRING",
            "BOOLEAN",
            "DECIMAL64",
            "DECIMAL128",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE"));
    private static final Logger LOG = LoggerFactory.getLogger(AbstractValidateOperatorStrategy.class);

    static {
        // This map converts Calcite RexNode SqlTypeName to OmniType Id
        RexTypeToIdMap.put("INT", 1);
        RexTypeToIdMap.put("INTEGER", 1);
        RexTypeToIdMap.put("BIGINT", 2);
        RexTypeToIdMap.put("DOUBLE", 3);
        RexTypeToIdMap.put("BOOLEAN", 4);
        RexTypeToIdMap.put("TINYINT", 5);
        RexTypeToIdMap.put("SMALLINT", 5);
        RexTypeToIdMap.put("DECIMAL", 6);
        RexTypeToIdMap.put("DECIMAL128", 7);
        RexTypeToIdMap.put("DATE", 8);
        RexTypeToIdMap.put("TIME", 10);
        RexTypeToIdMap.put("TIMESTAMP", 12);
        RexTypeToIdMap.put("INTERVAL_MONTH", 13);
        RexTypeToIdMap.put("INTERVAL_DAY", 14);
        RexTypeToIdMap.put("INTERVAL_SECOND", 14);
        RexTypeToIdMap.put("VARCHAR", 15);
        RexTypeToIdMap.put("CHAR", 16);
        RexTypeToIdMap.put("ROW", 17);
        RexTypeToIdMap.put("INVALID", 18);
        RexTypeToIdMap.put("TIME_WITHOUT_TIME_ZONE", 19); // TODO: Is this the same as TIME?
        RexTypeToIdMap.put("TIMESTAMP_WITHOUT_TIME_ZONE", 20);//TODO: Omni's TIMESTAMP uses int64_t, Flink has the possibility of accuracy>3
        RexTypeToIdMap.put("TIMESTAMP_TZ", 21);//TIMESTAMP_WITH_TIMEZONE
        RexTypeToIdMap.put("TIMESTAMP_WITH_LOCAL_TIME_ZONE", 22);
        RexTypeToIdMap.put("ARRAY", 23);
        RexTypeToIdMap.put("MULTISET", 24);
        RexTypeToIdMap.put("MAP", 25);
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
    }

    public abstract boolean executeValidateOperator(Map<String, Object> operatorInfoMap);

    public boolean validateDataTypes(List<List<String>> dataTypesList) {
        return dataTypesList.stream()
                // match DECIMAL64 and DECIMAL128
                .flatMap(List::stream)
                .allMatch(type -> {
                    if (type.matches("^DECIMAL64\\([^)]*\\)$")) {
                        type = "DECIMAL64";
                        LOG.info("converted to DECIMAL64");
                    }

                    if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                        type = "DECIMAL128";
                        LOG.info("converted to DECIMAL128");
                    }
                    return SUPPORT_DATA_TYPE.contains(type);
                });
    }

    @SuppressWarnings("unchecked")
    public List<List<String>> getDataTypes(Map<String, Object> jsonMap, String... names) {
        List<List<String>> dataTypes = new ArrayList<>();
        for (String name : names) {
            dataTypes.add((List<String>) jsonMap.get(name));
        }
        return dataTypes;
    }
}
