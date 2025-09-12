package org.apache.flink.table.planner.plan.nodes.exec.util;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * manage the system's configuration information
 *
 * @since 2025/04/22
 * @version 1.0.0
 */
public class DescriptionUtil {
    /**
     * getFieldTypeList
     *
     * @param fields fields
     * @return List<String>
     */
    public static List<String> getFieldTypeList(List<RowType.RowField> fields) {
        List<String> fieldTypeList = new ArrayList<>();
        for (RowType.RowField field : fields) {
            LogicalType fieldType = field.getType();
            fieldTypeList.add(getFieldType(fieldType));
        }
        return fieldTypeList;
    }

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
                typeName += "(" + precision.toString() + "," + scale.toString() + ")";
            }
        }
        return typeName;
    }

    /**
     * getAggregateCalls
     *
     * @param aggInfos aggInfos
     * @return List<Map<String,Object>>
     */
    public static List<Map<String, Object>> getAggregateCalls(AggregateInfo[] aggInfos) {
        List<Map<String, Object>> aggregateCalls = new ArrayList<>();
        // Iterate over the aggInfos array and print each element
        for (AggregateInfo aggInfo : aggInfos) {
            Map<String, Object> aggregateCallMap = new LinkedHashMap<>();
            AggregateCall aggCall = aggInfo.agg();
            aggregateCallMap.put("name", (aggCall != null ? aggCall.toString() : "null"));
            UserDefinedFunction function = aggInfo.function();
            aggregateCallMap.put("aggregationFunction", (function != null ? function.toString() : "null"));
            aggregateCallMap.put("argIndexes", aggInfo.argIndexes());
            aggregateCallMap.put("consumeRetraction", Boolean.toString(aggInfo.consumeRetraction()));
            aggregateCallMap.put("filterArg", (aggCall != null ? aggCall.filterArg : "null"));
            aggregateCalls.add(aggregateCallMap);
        }
        return aggregateCalls;
    }
}
