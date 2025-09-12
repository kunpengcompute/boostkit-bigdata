package com.huawei.omniruntime.flink.table.types.logical;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.json.JSONObject;

/**
 * LogicalTypeDescriptor
 *
 * @since 2025-04-27
 */
public class LogicalTypeDescriptor {
    /**
     * varchar
     */
    public static final String VARCHAR = "VARCHAR";

    /**
     * bigInt
     */
    public static final String BIGINT = "BIGINT";

    // public static String TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP_WITH_LOCAL_TIME_ZONE";
    /**
     * timeStamp
     */
    public static final String TIMESTAMP = "TIMESTAMP";

    /**
     * unsolved
     */
    public static final String UNRESOLVED = "UNRESOLVED";


    // the type format should be
    //  { type : "typename",
    //    kind : "logic"   // logic are specific for sql type or subtype
    //    isNull: yes or no
    //    other attribute of type
    // eg.  length: number   // varchar
    // e.g. precision: number //  timestamp precision
    /**
     * createBasicTypeObject
     *
     * @param typeName typeName
     * @param isNull isNull
     * @return JSONObject
     */
    public static JSONObject createBasicTypeObject(String typeName, boolean isNull) {
        JSONObject data = new JSONObject();
        String kindStr = "logical";
        data.put("kind", kindStr);
        data.put("type", typeName);
        data.put("isNull", isNull);
        return data;
    }

    /**
     * createJSONDescriptorBigIntType
     *
     * @param bigIntType bigIntType
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorBigIntType(BigIntType bigIntType) {
        return createBasicTypeObject(BIGINT, bigIntType.isNullable());
    }

    /**
     * createJSONDescriptorVarCharType
     *
     * @param varCharType varCharType
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorVarCharType(VarCharType varCharType) {
        JSONObject typeDescriptor = createBasicTypeObject(VARCHAR, varCharType.isNullable());
        typeDescriptor.put("length", varCharType.getLength());
        return typeDescriptor;
    }

    /**
     * createJSONDescriptorTimestampType
     *
     * @param timestampType timestampType
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorTimestampType(TimestampType timestampType) {
        JSONObject typeDescriptor = createBasicTypeObject(TIMESTAMP, timestampType.isNullable());
        TimestampKind timestampKind = timestampType.getKind();
        typeDescriptor.put("timestampKind", timestampType.getKind().ordinal());
        typeDescriptor.put("precision", timestampType.getPrecision());
        return typeDescriptor;
    }

    /**
     * createJSONDescriptorUNRESOLVED
     *
     * @param type type
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorUNRESOLVED(LogicalType type) {
        JSONObject typeDescriptor = createBasicTypeObject(UNRESOLVED, type.isNullable());
        typeDescriptor.put("meta", type);
        return typeDescriptor;
    }
}


/**
 * TBD all logical type should have a name for TDD JSON
 * <p>
 * CHAR(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.CHARACTER_STRING),
 * <p>
 * VARCHAR(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.CHARACTER_STRING),
 * <p>
 * BOOLEAN(LogicalTypeFamily.PREDEFINED),
 * <p>
 * BINARY(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.BINARY_STRING),
 * <p>
 * VARBINARY(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.BINARY_STRING),
 * <p>
 * DECIMAL(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * TINYINT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * SMALLINT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * INTEGER(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * BIGINT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * FLOAT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.APPROXIMATE_NUMERIC),
 * <p>
 * DOUBLE(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.APPROXIMATE_NUMERIC),
 * <p>
 * DATE(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME),
 * <p>
 * TIME_WITHOUT_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIME),
 * <p>
 * TIMESTAMP_WITHOUT_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIMESTAMP),
 * <p>
 * TIMESTAMP_WITH_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIMESTAMP),
 * <p>
 * TIMESTAMP_WITH_LOCAL_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.DATETIME,
 * LogicalTypeFamily.TIMESTAMP,
 * LogicalTypeFamily.EXTENSION),
 * <p>
 * INTERVAL_YEAR_MONTH(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.INTERVAL),
 * <p>
 * INTERVAL_DAY_TIME(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.INTERVAL),
 * <p>
 * ARRAY(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.COLLECTION),
 * <p>
 * MULTISET(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.COLLECTION),
 * <p>
 * MAP(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.EXTENSION),
 * <p>
 * ROW(LogicalTypeFamily.CONSTRUCTED),
 * <p>
 * DISTINCT_TYPE(LogicalTypeFamily.USER_DEFINED),
 * <p>
 * STRUCTURED_TYPE(LogicalTypeFamily.USER_DEFINED),
 * <p>
 * NULL(LogicalTypeFamily.EXTENSION),
 * <p>
 * RAW(LogicalTypeFamily.EXTENSION),
 * <p>
 * SYMBOL(LogicalTypeFamily.EXTENSION),
 * <p>
 * UNRESOLVED(LogicalTypeFamily.EXTENSION);
 */