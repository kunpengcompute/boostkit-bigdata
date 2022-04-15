package org.apache.spark.sql;

import com.huawei.boostkit.omnidata.decode.type.*;

import io.airlift.slice.Slice;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction;
import org.apache.spark.sql.execution.ndp.AggExeInfo;
import org.apache.spark.sql.execution.ndp.LimitExeInfo;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.parseFloat;
import static org.apache.hadoop.hdfs.DFSUtil.getRandom;

/**
 * NdpUtils
 *
 * @since 2021-03-30
 */
public class NdpUtils {

    public static int getColumnOffset(StructType dataSchema, Seq<Attribute> outPut) {
        List<Attribute> attributeList = JavaConverters.seqAsJavaList(outPut);
        String columnName = "";
        int columnTempId = 0;
        if (attributeList.size() > 0) {
            columnName = attributeList.get(0).name();
            columnTempId = NdpUtils.getColumnId(attributeList.get(0).toString());
        }
        Map<String, Integer> columnMap = new HashMap<>();
        scala.collection.Iterator<StructField> allTableSchemas = dataSchema.iterator();
        int dataSchemaColumnNum = 0;
        while (allTableSchemas.hasNext()) {
            StructField structField = allTableSchemas.next();
            columnMap.put(structField.name(), dataSchemaColumnNum++);
        }
        int columnOffset = columnTempId - columnMap.getOrDefault(columnName, columnMap.size());
        return Math.abs(columnOffset);
    }

    public static int getColumnOffsetByAggExeInfo(StructType dataSchema,
        Seq<AggExeInfo> aggExeInfo) {
        String columnName = "";
        int columnTempId = 0;
        if (aggExeInfo != null && aggExeInfo.size() > 0) {
            List<AggExeInfo> aggExecutionList = JavaConverters.seqAsJavaList(aggExeInfo);
            for (AggExeInfo aggExeInfoTemp : aggExecutionList) {
                List<AggregateFunction> aggregateExpressions = JavaConverters.seqAsJavaList(
                    aggExeInfoTemp.aggregateExpressions());
                for (AggregateFunction aggregateFunction : aggregateExpressions) {
                    List<Expression> expressions = JavaConverters
                        .seqAsJavaList(aggregateFunction.children());
                    for (Expression expression : expressions) {
                        columnName = expression.toString().split("#")[0].replaceAll("\\(", "");
                        Pattern pattern = Pattern.compile(columnName + "#(\\d+)");
                        Matcher matcher = pattern.matcher(expression.toString());
                        if(matcher.find()) {
                            columnTempId = Integer.parseInt(matcher.group(1));
                            break;
                        }
                    }
                    break;
                }
                List<NamedExpression> namedExpressions = JavaConverters.seqAsJavaList(
                    aggExeInfoTemp.groupingExpressions());
                for (NamedExpression namedExpression : namedExpressions) {
                    columnName = namedExpression.toString().split("#")[0];
                    columnTempId = NdpUtils.getColumnId(namedExpression.toString());
                    break;
                }
            }
        }
        Map<String, Integer> columnMap = new HashMap<>();
        scala.collection.Iterator<StructField> allTableSchemas = dataSchema.iterator();
        int dataSchemaColumnNum = 0;
        while (allTableSchemas.hasNext()) {
            StructField structField = allTableSchemas.next();
            columnMap.put(structField.name(), dataSchemaColumnNum++);
        }
        int columnOffset = columnTempId - columnMap.getOrDefault(columnName, columnMap.size());
        return Math.abs(columnOffset);
    }

    public static int getColumnId(String attribute) {
        if (null == attribute) {
            return -1;
        }
        int columnTempId = 0;
        String[] columnArray = attribute.split("#");
        if (columnArray.length < 2) {
            return -1;
        }
        String columnArrayId = columnArray[1];
        if ('L' == columnArrayId.charAt(columnArrayId.length() - 1)) {
            String adf = columnArrayId.substring(0, columnArrayId.length() - 1);
            columnTempId = Integer.parseInt(adf);
        } else {
            columnTempId = Integer.parseInt(columnArrayId);
        }
        return columnTempId;
    }

    public static Type transOlkDataType(DataType dataType, boolean isUdfOperator) {
        String strType = dataType.toString().toLowerCase(Locale.ENGLISH);
        if (isUdfOperator && "integertype".equalsIgnoreCase(strType)) {
            strType = "longtype";
        }
        switch (strType) {
            case "longtype":
                return BIGINT;
            case "integertype":
                return INTEGER;
            case "bytetype":
                return TINYINT;
            case "shorttype":
                return SMALLINT;
            case "floattype":
                return REAL;
            case "doubletype":
                return DOUBLE;
            case "booleantype":
                return BOOLEAN;
            case "stringtype":
                return VARCHAR;
            case "datetype":
                return DATE;
            case "arraytype(stringtype,true)":
            case "arraytype(stringtype,false)":
                return new ArrayType<>(VARCHAR);
            case "arraytype(integertype,true)":
            case "arraytype(integertype,false)":
            case "arraytype(longtype,true)":
            case "arraytype(longtype,false)":
                return new ArrayType<>(BIGINT);
            case "arraytype(floattype,true)":
            case "arraytype(floattype,false)":
                return new ArrayType<>(REAL);
            case "arraytype(doubletype,true)":
            case "arraytype(doubletype,false)":
                return new ArrayType<>(DOUBLE);
            default:
                throw new UnsupportedOperationException("unsupported this type:" + strType);
        }
    }

    public static Type transAggRetType(Type prestoType) {
        if (BIGINT.equals(prestoType) || INTEGER.equals(prestoType) ||
            SMALLINT.equals(prestoType) || TINYINT.equals(prestoType) || REAL.equals(prestoType)) {
            return BIGINT;
        } else {
            return prestoType;
        }
    }

    public static DecodeType transAggDecodeType(Type prestoType) {
        if (BIGINT.equals(prestoType)) {
            return new LongDecodeType();
        }
        if (INTEGER.equals(prestoType)) {
            return new LongToIntDecodeType();
        }
        if (SMALLINT.equals(prestoType)) {
            return new LongToShortDecodeType();
        }
        if (TINYINT.equals(prestoType)) {
            return new LongToByteDecodeType();
        }
        if (DOUBLE.equals(prestoType)) {
            return new DoubleDecodeType();
        }
        if (REAL.equals(prestoType)) {
            return new LongToFloatDecodeType();
        }
        if (BOOLEAN.equals(prestoType)) {
            return new BooleanDecodeType();
        }
        if (VARCHAR.equals(prestoType)) {
            return new VarcharDecodeType();
        }
        if (DATE.equals(prestoType)) {
            return new DateDecodeType();
        }
        throw new RuntimeException("unsupported this prestoType:" + prestoType);
    }

    public static DecodeType transDataIoDataType(DataType dataType) {
        String strType = dataType.toString().toLowerCase(Locale.ENGLISH);
        switch (strType) {
            case "integertype":
                return new IntDecodeType();
            case "shorttype":
                return new ShortDecodeType();
            case "longtype":
                return new LongDecodeType();
            case "floattype":
                return new FloatDecodeType();
            case "doubletype":
                return new DoubleDecodeType();
            case "booleantype":
                return new BooleanDecodeType();
            case "bytetype":
                return new ByteDecodeType();
            case "stringtype":
                return new VarcharDecodeType();
            case "datetype":
                return new DateDecodeType();
            default:
                throw new RuntimeException("unsupported this type:" + strType);
        }
    }

    public static ConstantExpression transArgumentData(String argumentValue, Type argumentType) {
        String strType = argumentType.toString().toLowerCase(Locale.ENGLISH);
        switch (strType) {
            case "bigint":
            case "integer":
            case "date":
            case "tinyint":
            case "smallint":
                long longValue = Long.parseLong(argumentValue);
                return new ConstantExpression(longValue, argumentType);
            case "real":
                return new ConstantExpression(
                    (long)floatToIntBits(parseFloat(argumentValue)), argumentType);
            case "double":
                return new ConstantExpression(Double.valueOf(argumentValue), argumentType);
            case "boolean":
                return new ConstantExpression(Boolean.valueOf(argumentValue), argumentType);
            case "varchar":
                Slice charValue = utf8Slice(argumentValue);
                return new ConstantExpression(charValue, argumentType);
            default:
                throw new UnsupportedOperationException("unsupported data type " + strType);
        }
    }

    public static Attribute getColumnAttribute(Attribute inputAttribute, List<Attribute> listAtt) {
        String columnName = inputAttribute.name();
        Attribute resAttribute = inputAttribute;
        if (columnName.contains("(")) {
            for (Attribute att : listAtt) {
                if (columnName.contains(att.name())) {
                    resAttribute = att;
                    break;
                }
            }
        }
        return resAttribute;
    }

    public static Object transData(String sparkType, String columnValue) {
        String strType = sparkType.toLowerCase(Locale.ENGLISH);
        switch (strType) {
            case "integertype":
                return Integer.valueOf(columnValue);
            case "bytetype":
                return Byte.valueOf(columnValue);
            case "shorttype":
                return Short.valueOf(columnValue);
            case "longtype":
                return Long.valueOf(columnValue);
            case "floattype":
                return (long)floatToIntBits(parseFloat(columnValue));
            case "doubletype":
                return Double.valueOf(columnValue);
            case "booleantype":
                return Boolean.valueOf(columnValue);
            case "stringtype":
            case "datetype":
                return columnValue;
            default:
                return "";
        }
    }

    public static OptionalLong convertLimitExeInfo(Option<LimitExeInfo> limitExeInfo) {
        return limitExeInfo.isEmpty() ? OptionalLong.empty()
            : OptionalLong.of(limitExeInfo.get().limit());
    }

    public static String getPartitionValue(String filePath, String columnName) {
        String[] filePathStrArray = filePath.split("\\/");
        String partitionValue = "";
        Pattern pn = Pattern.compile(columnName + "\\=");
        for (String strColumn : filePathStrArray) {
            Matcher matcher = pn.matcher(strColumn);
            if (matcher.find()) {
                partitionValue = strColumn.split("\\=")[1];
                if (partitionValue.contains("__HIVE_DEFAULT_PARTITION__")) {
                    partitionValue = null;
                }
                break;
            }
        }
        return partitionValue;
    }

    public static int getFpuHosts(int hostSize) {
        return (int) (Math.random() * hostSize);
    }

    public static boolean isValidDateFormat(String dateString) {
        boolean isValid = true;
        String pattern = "yyyy-MM-dd";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.STRICT);
        try {
            formatter.parse(dateString);
        } catch (DateTimeParseException e) {
            isValid = false;
        }
        return isValid;
    }

    public static boolean isInDateExpression(Expression expression, String Operator) {
        boolean isInDate = false;
        if (expression instanceof Cast && Operator.equals("in")) {
            isInDate = ((Cast) expression).child().dataType() instanceof DateType;
        }
        return isInDate;
    }
}
