package org.apache.spark.sql;

import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

import com.huawei.boostkit.omnidata.exception.OmniDataException;
import com.huawei.boostkit.omnidata.exception.OmniErrorCode;
import com.huawei.boostkit.omnidata.model.AggregationInfo;
import com.huawei.boostkit.omnidata.model.Column;
import com.huawei.boostkit.omnidata.model.Predicate;
import com.huawei.boostkit.omnidata.model.TaskSource;
import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsOrcDataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsParquetDataSource;
import com.huawei.boostkit.omnidata.reader.impl.DataReaderImpl;
import com.huawei.boostkit.omnidata.spark.SparkDeserializer;
import com.huawei.boostkit.omnidata.type.DecodeType;
import com.huawei.boostkit.omnidata.type.LongDecodeType;
import com.huawei.boostkit.omnidata.type.RowDecodeType;

import com.google.common.collect.ImmutableMap;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryArithmetic;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.expressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.LessThan;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.expressions.Or;
import org.apache.spark.sql.catalyst.expressions.Remainder;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.ndp.AggExeInfo;
import org.apache.spark.sql.execution.ndp.FilterExeInfo;
import org.apache.spark.sql.execution.ndp.PushDownInfo;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.hive.HiveSimpleUDF;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;

/**
 * DataIoAdapter
 *
 * @since 2021-03-31
 */
public class DataIoAdapter {
    private int TASK_FAILED_TIMES = 3;

    private List<Type> omnidataTypes = new ArrayList<>();

    private List<Column> omnidataColumns = new ArrayList<>();

    private Optional<RowExpression> prestoFilter = Optional.empty();

    private Set<String> columnNameSet = new HashSet<>();

    private List<RowExpression> omnidataProjections = new ArrayList<>();

    private boolean hasNextPage = false;

    private DataReaderImpl orcDataReader = null;

    private List<DecodeType> columnTypesList = new ArrayList<>();

    private List<Integer> columnOrdersList = new ArrayList<>();

    private List<DecodeType> filterTypesList = new ArrayList<>();

    private List<Integer> filterOrdersList = new ArrayList<>();

    private Map<String, Integer> fieldMap = new HashMap<>();

    private Map<String, Integer> columnNameMap = new HashMap<>();

    private Set<String> partitionColumnName = new HashSet<>();

    private List<Attribute> listAtt = new ArrayList<>();

    private int columnOffset = 0;

    private String filePath = "";

    private int columnOrder = 0;

    private NdpUdfExpressions ndpUdfExpressions = new NdpUdfExpressions();

    private static final Logger LOG = LoggerFactory.getLogger(DataIoAdapter.class);

    /**
     * Contact with Omni-Data-Server
     * @param pageCandidate file split info
     * @param sparkOutPut data schema
     * @param partitionColumn partition column
     * @param filterOutPut filter schema
     * @param pushDownOperators push down expressions
     * @param omniDataProperties auth properties
     * @return WritableColumnVector data result info
     * @throws TaskExecutionException connect to omni-data-server failed exception
     * @notice 3rd parties api throws Exception, function has to catch basic Exception
     */
    public Iterator<WritableColumnVector[]> getPageIterator(
        PageCandidate pageCandidate,
        Seq<Attribute> sparkOutPut,
        Seq<Attribute> partitionColumn,
        Seq<Attribute> filterOutPut,
        PushDownInfo pushDownOperators,
        OmniDataProperties omniDataProperties) throws TaskExecutionException, UnknownHostException {
        // initCandidates
        initCandidates(pageCandidate, filterOutPut);

        // create AggregationInfo
        // init agg candidates
        List<Attribute> partitionColumnBatch = JavaConverters.seqAsJavaList(partitionColumn);
        for (Attribute attribute : partitionColumnBatch) {
            partitionColumnName.add(attribute.name());
        }
        List<AggExeInfo> aggExecutionList =
            JavaConverters.seqAsJavaList(pushDownOperators.aggExecutions());
        if (aggExecutionList.size() == 0) {
            initColumnInfo(sparkOutPut);
        }
        DataSource dataSource = initDataSource(pageCandidate);
        RowExpression rowExpression = initFilter(pushDownOperators.filterExecutions());
        prestoFilter = rowExpression == null ?
            Optional.empty() : Optional.of(rowExpression);
        Optional<AggregationInfo> aggregations =
            initAggAndGroupInfo(aggExecutionList);
        // create limitLong
        OptionalLong limitLong = NdpUtils.convertLimitExeInfo(pushDownOperators.limitExecution());

        Predicate predicate = new Predicate(
            omnidataTypes, omnidataColumns, prestoFilter, omnidataProjections,
            ImmutableMap.of(), ImmutableMap.of(), aggregations, limitLong);
        TaskSource taskSource = new TaskSource(dataSource, predicate, 1048576);
        SparkDeserializer deserializer = initSparkDesializer();
        Properties properties = NdpUtils.getProperties(omniDataProperties);
        WritableColumnVector[] page = null;
        int failedTimes = 0;
        String[] sdiHostArray = pageCandidate.getSdiHosts().split(",");
        int random_index = (int) (Math.random() * sdiHostArray.length);
        Iterator<String> sdiHosts = Arrays.stream(sdiHostArray).iterator();
        Set<String> sdiHostSet = new HashSet<>();
        sdiHostSet.add(sdiHostArray[random_index]);
        while (sdiHosts.hasNext()) {
            String sdiHost;
            if (failedTimes == 0) {
                sdiHost = sdiHostArray[random_index];
            } else {
                sdiHost = sdiHosts.next();
                if (sdiHostSet.contains(sdiHost)) {
                    continue;
                }
            }
            String ipAddress = InetAddress.getByName(sdiHost).getHostAddress();
            LOG.info("Push down node info: [hostname :{} ,ip :{}]", sdiHost, ipAddress);
            properties.put("grpc.client.target", ipAddress + ":" + pageCandidate.getSdiPort());
            try {
                orcDataReader = new DataReaderImpl<SparkDeserializer>(
                        properties, taskSource, deserializer);
                hasNextPage = true;
                page = (WritableColumnVector[]) orcDataReader.getNextPageBlocking();
                if (orcDataReader.isFinished()) {
                    orcDataReader.close();
                    hasNextPage = false;
                }
                break;
            } catch (OmniDataException omniDataException) {
                OmniErrorCode errorCode = omniDataException.getErrorCode();
                switch (errorCode) {
                    case OMNIDATA_INSUFFICIENT_RESOURCES:
                        LOG.warn("OMNIDATA_INSUFFICIENT_RESOURCES: "
                            + "OmniData-server's push down queue is full, "
                            + "begin to find next OmniData-server");
                        break;
                    case OMNIDATA_UNSUPPORTED_OPERATOR:
                        LOG.warn("OMNIDATA_UNSUPPORTED_OPERATOR: "
                            + "OmniDataException: exist unsupported operator");
                        break;
                    case OMNIDATA_GENERIC_ERROR:
                        LOG.warn("OMNIDATA_GENERIC_ERROR: Current OmniData-server unavailable, "
                            + "begin to find next OmniData-server");
                        break;
                    case OMNIDATA_NOT_FOUND:
                        LOG.warn("OMNIDATA_NOT_FOUND: Current OmniData-Server not found, "
                            + "begin to find next OmniData-server");
                        break;
                    case OMNIDATA_INVALID_ARGUMENT:
                        LOG.warn("OMNIDATA_INVALID_ARGUMENT: INVALID_ARGUMENT, "
                            + "exist unsupported operator or dataType");
                        break;
                    case OMNIDATA_IO_ERROR:
                        LOG.warn("OMNIDATA_IO_ERROR: Current OmniData-Server io exception, "
                            + "begin to find next OmniData-server");
                        break;
                    default:
                        LOG.warn("OmniDataException: OMNIDATA_ERROR.");
                }
                LOG.warn("Push down failed node info [hostname :{} ,ip :{}]", sdiHost, ipAddress);
                ++failedTimes;
            } catch (Exception e) {
                LOG.warn("Push down failed node info [hostname :{} ,ip :{}]", sdiHost, ipAddress);
                ++failedTimes;
            }
        }
        int retryTime = Math.min(TASK_FAILED_TIMES, sdiHostArray.length);
        if (failedTimes >= retryTime) {
            LOG.warn("No Omni-data-server to Connect, Task has tried {} times.", retryTime);
            throw new TaskExecutionException("No Omni-data-server to Connect");
        }
        List<WritableColumnVector[]> l = new ArrayList<>();
        l.add(page);
        return l.iterator();
    }

    public boolean hasnextIterator(List<Object> pageList, PageToColumnar pageToColumnarClass,
        PartitionedFile partitionFile, boolean isVectorizedReader)
        throws Exception {
        if (!hasNextPage) {
            return hasNextPage;
        }
        WritableColumnVector[] page = (WritableColumnVector[]) orcDataReader.getNextPageBlocking();
        if (orcDataReader.isFinished()) {
            orcDataReader.close();
            return false;
        }
        List<WritableColumnVector[]> l = new ArrayList<>();
        l.add(page);
        pageList.addAll(pageToColumnarClass
            .transPageToColumnar(l.iterator(), isVectorizedReader));
        return true;
    }

    private void initCandidates(PageCandidate pageCandidate, Seq<Attribute> filterOutPut) {
        omnidataTypes.clear();
        omnidataColumns.clear();
        omnidataProjections.clear();
        fieldMap.clear();
        columnNameSet.clear();
        columnTypesList.clear();
        columnOrdersList.clear();
        filterTypesList.clear();
        filterOrdersList.clear();
        partitionColumnName.clear();
        columnNameMap.clear();
        columnOrder = 0;
        filePath = pageCandidate.getFilePath();
        columnOffset = pageCandidate.getColumnOffset();
        listAtt = JavaConverters.seqAsJavaList(filterOutPut);
    }

    private RowExpression extractNamedExpression(Expression namedExpression) {
        Type prestoType = NdpUtils.transOlkDataType(namedExpression.dataType(), false);
        int aggProjectionId;
        String aggColumnName = namedExpression.toString().split("#")[0].toLowerCase(Locale.ENGLISH);
        columnOrdersList.add(columnOrder++);
        columnTypesList.add(NdpUtils.transDataIoDataType(namedExpression.dataType()));

        if (null != fieldMap.get(aggColumnName)) {
            aggProjectionId = fieldMap.get(aggColumnName);
        } else {
            int columnId = NdpUtils
                .getColumnId(namedExpression.toString()) - columnOffset;
            aggProjectionId = fieldMap.size();
            fieldMap.put(aggColumnName, aggProjectionId);
            omnidataTypes.add(prestoType);
            boolean isPartitionKey = partitionColumnName.contains(aggColumnName);
            String partitionValue = NdpUtils.getPartitionValue(filePath, aggColumnName);
            omnidataColumns.add(new Column(columnId, aggColumnName,
                prestoType, isPartitionKey, partitionValue));
            columnNameSet.add(aggColumnName);
            if (null == columnNameMap.get(aggColumnName)) {
                columnNameMap.put(aggColumnName, columnNameMap.size());
            }
            omnidataProjections.add(new InputReferenceExpression(aggProjectionId, prestoType));
        }

        return new InputReferenceExpression(aggProjectionId, prestoType);
    }

    private void extractSumMaxMinAggregation(Type prestoType, String expressionName,
        Map<String, AggregationInfo.AggregateFunction> aggregationMap) {
        String operatorName = expressionName.split("\\(")[0];
        List<RowExpression> arguments = new ArrayList<>();
        Type returnType = NdpUtils.transAggRetType(prestoType);
        if(operatorName.equals("sum")) {
            columnTypesList.add(NdpUtils.transAggDecodeType(returnType));
        } else {
            columnTypesList.add(NdpUtils.transAggDecodeType(prestoType));
        }
        FunctionHandle functionHandle = new BuiltInFunctionHandle(
            new Signature(QualifiedObjectName.valueOfDefaultFunction(operatorName), AGGREGATE,
                prestoType.getTypeSignature(), prestoType.getTypeSignature()));
        int aggProjectionId = fieldMap.get(expressionName);
        RowExpression rowExpression = new InputReferenceExpression(aggProjectionId, prestoType);
        arguments.add(rowExpression);
        CallExpression callExpression = new CallExpression(operatorName,
            functionHandle, returnType, arguments, Optional.empty());
        aggregationMap.put(String.format("%s_%s", operatorName, columnOrder),
            new AggregationInfo.AggregateFunction(callExpression, false));
    }

    private void extractAvgAggregation(Type prestoType, String expressionName,
        Map<String, AggregationInfo.AggregateFunction> aggregationMap) {
        List<RowExpression> arguments = new ArrayList<>();
        int aggProjectionId = fieldMap.get(expressionName);
        RowExpression rowExpression = new InputReferenceExpression(aggProjectionId, prestoType);
        arguments.add(rowExpression);
        FunctionHandle functionHandle = new BuiltInFunctionHandle(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(), prestoType.getTypeSignature()));
        List<Type> rowType = Arrays.asList(DoubleType.DOUBLE, BigintType.BIGINT);
        RowType returnType = RowType.anonymous(rowType);
        CallExpression callExpression = new CallExpression("avg",
            functionHandle, returnType, arguments, Optional.empty());
        aggregationMap.put(String.format("%s_%s", "avg", columnOrder),
            new AggregationInfo.AggregateFunction(callExpression, false));
    }

    private void extractCountAggregation(Type prestoType, String expressionName,
        Map<String, AggregationInfo.AggregateFunction> aggregationMap) {
        List<RowExpression> arguments = new ArrayList<>();
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("count"),
            AGGREGATE, BIGINT.getTypeSignature());
        if (!expressionName.equals("count(1)")) {
            int aggProjectionId = fieldMap.get(expressionName);
            RowExpression rowExpression = new InputReferenceExpression(aggProjectionId, prestoType);
            arguments.add(rowExpression);
            signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("count"),
                AGGREGATE, BIGINT.getTypeSignature(), prestoType.getTypeSignature());
        }
        FunctionHandle functionHandle = new BuiltInFunctionHandle(signature);
        CallExpression callExpression = new CallExpression("count",
            functionHandle, BIGINT, arguments, Optional.empty());
        aggregationMap.put(String.format("%s_%s", "count", columnOrder),
            new AggregationInfo.AggregateFunction(callExpression, false));
    }

    private CallExpression createAggBinCall(BinaryArithmetic expression,
        String operatorName, Type prestoType) {
        List<RowExpression> arguments = new ArrayList<>();
        Type leftPrestoType = NdpUtils.transOlkDataType(
            expression.left().dataType(), false);
        Type rightPrestoType = NdpUtils.transOlkDataType(
            expression.right().dataType(), false);
        FunctionHandle functionHandle = new BuiltInFunctionHandle(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("$operator$" + operatorName),
                SCALAR, prestoType.getTypeSignature(),
                leftPrestoType.getTypeSignature(),
                rightPrestoType.getTypeSignature()));
        arguments.add(createAggProjection(expression.left()));
        arguments.add(createAggProjection(expression.right()));
        return new CallExpression(operatorName, functionHandle,
            prestoType, arguments, Optional.empty());
    }

    private RowExpression createAggProjection(Expression expression) {
        Type prestoType = NdpUtils.transOlkDataType(expression.dataType(), false);
        AggExpressionType aggExpressionType = AggExpressionType
            .valueOf(expression.getClass().getSimpleName());
        switch (aggExpressionType) {
            case Add:
                return createAggBinCall((Add) expression, "Add", prestoType);
            case Subtract:
                return createAggBinCall((Subtract) expression, "Subtract", prestoType);
            case Multiply:
                return createAggBinCall((Multiply) expression, "Multiply", prestoType);
            case Divide:
                return createAggBinCall((Divide) expression, "Divide", prestoType);
            case Remainder:
                return createAggBinCall((Remainder) expression, "Remainder", prestoType);
            case Literal:
                Object value = NdpUtils.transData(
                    expression.dataType().toString(), expression.toString());
                return new ConstantExpression(value, prestoType);
            case AttributeReference:
                String aggColumnName = expression.toString().split("#")[0].toLowerCase(Locale.ENGLISH);
                int field;
                if (null == columnNameMap.get(aggColumnName)) {
                    field = columnNameMap.size();
                    columnNameMap.put(aggColumnName, field);
                    int columnId = NdpUtils
                        .getColumnId(expression.toString()) - columnOffset;
                    boolean isPartitionKey = partitionColumnName.contains(aggColumnName);
                    String partitionValue = NdpUtils
                        .getPartitionValue(filePath, aggColumnName);
                    omnidataColumns.add(
                        new Column(columnId, aggColumnName,
                            prestoType, isPartitionKey, partitionValue));
                } else {
                    field = columnNameMap.get(aggColumnName);
                }
                return new InputReferenceExpression(field, prestoType);

            default:
                throw new UnsupportedOperationException("unsupported agg operation type");
        }
    }

    enum AggExpressionType {
        Multiply,
        Add,
        Subtract,
        Divide,
        Remainder,
        Literal,
        AttributeReference
    }

    private void extractAggregateFunction(AggregateFunction aggregateFunction,
        Map<String, AggregationInfo.AggregateFunction> aggregationMap) {
        List<Expression> expressions = JavaConverters.seqAsJavaList(aggregateFunction.children());
        String aggregateFunctionName = aggregateFunction.toString();
        Type prestoType = NdpUtils.transOlkDataType(aggregateFunction.dataType(), false);
        AggregateFunctionType aggregateFunctionType = AggregateFunctionType.valueOf(
            aggregateFunction.getClass().getSimpleName());
        for (Expression expression : expressions) {
            if(!(expression instanceof Literal)){
                omnidataProjections.add(createAggProjection(expression));
                int projectionId = fieldMap.size();
                fieldMap.put(aggregateFunctionName, projectionId);
                if (aggregateFunctionType.equals(AggregateFunctionType.Count)) {
                    prestoType = NdpUtils.transOlkDataType(expression.dataType(), false);
                }
                omnidataTypes.add(prestoType);
                break;
            }
        }
        columnOrdersList.add(columnOrder++);
        switch (aggregateFunctionType) {
            case Sum:
            case Max:
            case Min:
                extractSumMaxMinAggregation(prestoType, aggregateFunctionName, aggregationMap);
                break;
            case Average:
                columnTypesList.add(new RowDecodeType());
                columnOrdersList.add(columnOrder++);
                extractAvgAggregation(prestoType, aggregateFunctionName, aggregationMap);
                break;
            case Count:
                columnTypesList.add(new LongDecodeType());
                extractCountAggregation(prestoType, aggregateFunctionName, aggregationMap);
                break;
        }
    }

    enum AggregateFunctionType {
        Sum,
        Average,
        Max,
        Min,
        Count
    }

    enum ExpressionOperator {
        And,
        Or,
        Not,
        EqualTo,
        IsNotNull,
        LessThan,
        GreaterThan,
        GreaterThanOrEqual,
        LessThanOrEqual,
        In,
        HiveSimpleUDF,
        IsNull
    }

    private Optional<AggregationInfo> createAggregationInfo(
        List<AggregateFunction> aggregateFunctions,
        List<NamedExpression> namedExpressions) {
        List<RowExpression> groupingKeys = new ArrayList<>();
        Map<String, AggregationInfo.AggregateFunction> aggregationMap = new LinkedHashMap<>();
        boolean isEmpty = true;
        for (NamedExpression namedExpression : namedExpressions) {
            RowExpression groupingKey = extractNamedExpression((Expression) namedExpression);
            groupingKeys.add(groupingKey);
            isEmpty = false;
        }
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            extractAggregateFunction(aggregateFunction, aggregationMap);
            isEmpty = false;
        }
        return isEmpty ? Optional.empty() : Optional.of(
            new AggregationInfo(aggregationMap, groupingKeys));
    }

    private Optional<AggregationInfo> extractAggAndGroupExpression(
        List<AggExeInfo> aggExecutionList) {
        Optional<AggregationInfo> resAggregationInfo = Optional.empty();
        for (AggExeInfo aggExeInfo : aggExecutionList) {
            List<AggregateFunction> aggregateExpressions = JavaConverters.seqAsJavaList(
                aggExeInfo.aggregateExpressions());
            List<NamedExpression> namedExpressions = JavaConverters.seqAsJavaList(
                aggExeInfo.groupingExpressions());
            resAggregationInfo = createAggregationInfo(aggregateExpressions, namedExpressions);
        }
        return resAggregationInfo;
    }

    private RowExpression extractFilterExpression(Seq<FilterExeInfo> filterExecution) {
        List<FilterExeInfo> filterExecutionList = JavaConverters.seqAsJavaList(filterExecution);
        RowExpression resRowExpression = null;
        for (FilterExeInfo filterExeInfo : filterExecutionList) {
            resRowExpression = reverseExpressionTree(filterExeInfo.filter());
        }
        return resRowExpression;
    }

    private RowExpression reverseExpressionTree(Expression filterExpression) {
        RowExpression resRowExpression = null;
        if (filterExpression == null) {
            return resRowExpression;
        }
        List<RowExpression> tempRowExpression = new ArrayList<>();
        if (filterExpression instanceof Or) {
            RowExpression a1 = getExpression(((Or) filterExpression).left());
            RowExpression a2 = getExpression(((Or) filterExpression).right());
            tempRowExpression.add(a1);
            tempRowExpression.add(a2);
            resRowExpression = new SpecialForm(SpecialForm.Form.valueOf("OR"),
                BOOLEAN, tempRowExpression);
        } else if (filterExpression instanceof And) {
            RowExpression a1 = getExpression(((And) filterExpression).left());
            RowExpression a2 = getExpression(((And) filterExpression).right());
            tempRowExpression.add(a2);
            tempRowExpression.add(a1);
            resRowExpression = new SpecialForm(SpecialForm.Form.valueOf("AND"),
                BOOLEAN, tempRowExpression);
        } else {
            resRowExpression = getExpression(filterExpression);
        }
        return resRowExpression;
    }

    private RowExpression getExpression(Expression filterExpression) {
        RowExpression resRowExpression = null;
        List<Expression> rightExpressions = new ArrayList<>();
        ExpressionOperator expressionOperType =
            ExpressionOperator.valueOf(filterExpression.getClass().getSimpleName());
        Expression left;
        Expression right;
        String operatorName;
        switch (expressionOperType) {
            case Or:
            case And:
                return reverseExpressionTree(filterExpression);
            case Not:
                Signature notSignature = new Signature(
                    QualifiedObjectName.valueOfDefaultFunction("not"),
                    FunctionKind.SCALAR, new TypeSignature("boolean"),
                    new TypeSignature("boolean"));
                RowExpression tempRowExpression = getExpression(((Not) filterExpression).child());
                List<RowExpression> notArguments = new ArrayList<>();
                notArguments.add(tempRowExpression);
                return new CallExpression("not", new BuiltInFunctionHandle(notSignature),
                    BOOLEAN, notArguments, Optional.empty());
            case EqualTo:
                if (((EqualTo) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((EqualTo) filterExpression).left());
                    left = ((EqualTo) filterExpression).right();
                } else {
                    rightExpressions.add(((EqualTo) filterExpression).right());
                    left = ((EqualTo) filterExpression).left();
                }
                return getRowExpression(left,
                    "equal", rightExpressions);
            case IsNotNull:
                Signature isnullSignature = new Signature(
                    QualifiedObjectName.valueOfDefaultFunction("not"),
                    FunctionKind.SCALAR, new TypeSignature("boolean"),
                    new TypeSignature("boolean"));
                RowExpression isnullRowExpression =
                    getRowExpression(((IsNotNull) filterExpression).child(),
                    "is_null", null);
                List<RowExpression> isnullArguments = new ArrayList<>();
                isnullArguments.add(isnullRowExpression);
                return new CallExpression("not", new BuiltInFunctionHandle(isnullSignature),
                    BOOLEAN, isnullArguments, Optional.empty());
            case IsNull:
                return getRowExpression(((IsNull) filterExpression).child(),
                    "is_null", null);
            case LessThan:
                if (((LessThan) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((LessThan) filterExpression).left());
                    left = ((LessThan) filterExpression).right();
                    operatorName = "greater_than";
                } else {
                    rightExpressions.add(((LessThan) filterExpression).right());
                    left = ((LessThan) filterExpression).left();
                    operatorName = "less_than";
                }
                return getRowExpression(left,
                    operatorName, rightExpressions);
            case GreaterThan:
                if (((GreaterThan) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((GreaterThan) filterExpression).left());
                    left = ((GreaterThan) filterExpression).right();
                    operatorName = "less_than";
                } else {
                    rightExpressions.add(((GreaterThan) filterExpression).right());
                    left = ((GreaterThan) filterExpression).left();
                    operatorName = "greater_than";
                }
                return getRowExpression(left,
                    operatorName, rightExpressions);
            case GreaterThanOrEqual:
                if (((GreaterThanOrEqual) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((GreaterThanOrEqual) filterExpression).left());
                    left = ((GreaterThanOrEqual) filterExpression).right();
                    operatorName = "less_than_or_equal";
                } else {
                    rightExpressions.add(((GreaterThanOrEqual) filterExpression).right());
                    left = ((GreaterThanOrEqual) filterExpression).left();
                    operatorName = "greater_than_or_equal";
                }
                return getRowExpression(left,
                    operatorName, rightExpressions);
            case LessThanOrEqual:
                if (((LessThanOrEqual) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((LessThanOrEqual) filterExpression).left());
                    left = ((LessThanOrEqual) filterExpression).right();
                    operatorName = "greater_than_or_equal";
                } else {
                    rightExpressions.add(((LessThanOrEqual) filterExpression).right());
                    left = ((LessThanOrEqual) filterExpression).left();
                    operatorName = "less_than_or_equal";
                }
                return getRowExpression(left,
                    operatorName, rightExpressions);
            case In:
                List<Expression> rightExpression =
                    JavaConverters.seqAsJavaList(((In) filterExpression).list());
                return getRowExpression(((In) filterExpression).value(), "in", rightExpression);
            case HiveSimpleUDF:
                return getRowExpression(filterExpression,
                    ((HiveSimpleUDF) filterExpression).name(), rightExpressions);
            default:
                return resRowExpression;
        }
    }

    private RowExpression getRowExpression(Expression leftExpression, String operatorName,
        List<Expression> rightExpression) {
        String signatureName = operatorName;
        PrestoExpressionInfo expressionInfo = new PrestoExpressionInfo();
        Type prestoType;
        int filterProjectionId;
        // deal with left expression only UDF and Attribute
        if (leftExpression instanceof AttributeReference) {
            prestoType = NdpUtils.transOlkDataType(leftExpression.dataType(), false);
            filterProjectionId = putFilterValue(leftExpression, prestoType);
        } else if (leftExpression instanceof Cast && operatorName.equals("in")) {
            prestoType = NdpUtils.transOlkDataType(((Cast) leftExpression).child().dataType(), false);
            filterProjectionId = putFilterValue(((Cast) leftExpression).child(), prestoType);
        } else {
            ndpUdfExpressions.createNdpUdf(leftExpression, expressionInfo, fieldMap);
            putFilterValue(expressionInfo.getChildExpression(), expressionInfo.getFieldDataType());
            prestoType = expressionInfo.getReturnType();
            filterProjectionId = expressionInfo.getProjectionId();
        }
        // deal with right expression
        List<Object> argumentValues = new ArrayList<>();
        List<RowExpression> multiArguments = new ArrayList<>();
        int rightProjectionId = -1;
        RowExpression rowExpression;
        if (rightExpression != null && rightExpression.size() > 0 &&
            rightExpression.get(0) instanceof AttributeReference) {
            rightProjectionId = putFilterValue(rightExpression.get(0), prestoType);
            multiArguments.add(new InputReferenceExpression(filterProjectionId, prestoType));
            multiArguments.add(new InputReferenceExpression(rightProjectionId, prestoType));
            rowExpression = NdpFilterUtils.generateRowExpression(
                signatureName, expressionInfo, prestoType, filterProjectionId,
                null, multiArguments, "multy_columns");
        } else {
            // get right value
            if (NdpUtils.isInDateExpression(leftExpression, operatorName)) {
                argumentValues = getDateValue(rightExpression);
            } else {
                argumentValues = getValue(rightExpression, signatureName,
                        leftExpression.dataType().toString());
            }
            rowExpression = NdpFilterUtils.generateRowExpression(
                signatureName, expressionInfo, prestoType, filterProjectionId,
                argumentValues, null, signatureName);
        }
        return rowExpression;
    }

    // column projection赋值
    private int putFilterValue(Expression valueExpression, Type prestoType) {
        // Filter赋值
        int columnId = NdpUtils.getColumnId(valueExpression.toString()) - columnOffset;
        String filterColumnName = valueExpression.toString().split("#")[0].toLowerCase(Locale.ENGLISH);
        if (null != fieldMap.get(filterColumnName)) {
            return fieldMap.get(filterColumnName);
        }
        boolean isPartitionKey = partitionColumnName.contains(filterColumnName);
        int filterProjectionId = fieldMap.size();
        fieldMap.put(filterColumnName, filterProjectionId);
        filterTypesList.add(NdpUtils.transDataIoDataType(valueExpression.dataType()));
        filterOrdersList.add(filterProjectionId);
        String partitionValue = NdpUtils.getPartitionValue(filePath, filterColumnName);
        columnNameSet.add(filterColumnName);
        omnidataProjections.add(new InputReferenceExpression(filterProjectionId, prestoType));
        omnidataColumns.add(new Column(columnId, filterColumnName,
            prestoType, isPartitionKey, partitionValue));
        omnidataTypes.add(prestoType);
        if (null == columnNameMap.get(filterColumnName)) {
            columnNameMap.put(filterColumnName, columnNameMap.size());
        }
        return filterProjectionId;
    }

    // for date parse
    private List<Object> getDateValue(List<Expression> rightExpression) {
        long DAY_TO_MILL_SECS = 24L * 3600L * 1000L;
        List<Object> dateTimes = new ArrayList<>();
        for (Expression rExpression: rightExpression) {
            String dateStr = rExpression.toString();
            if (NdpUtils.isValidDateFormat(dateStr)) {
                String[] dateStrArray = dateStr.split("-");
                int year = Integer.parseInt(dateStrArray[0]) - 1900;
                int month = Integer.parseInt(dateStrArray[1]) - 1;
                int day = Integer.parseInt(dateStrArray[2]);
                Date date = new Date(year, month, day);
                dateTimes.add(String.valueOf((date.getTime() - date.getTimezoneOffset() * 60000L) / DAY_TO_MILL_SECS));
            } else {
                throw new UnsupportedOperationException("decode date failed: " + dateStr);
            }
        }
        return dateTimes;
    }

    private List<Object> getValue(List<Expression> rightExpression,
        String operatorName,
        String sparkType) {
        Object objectValue;
        List<Object> argumentValues = new ArrayList<>();
        if (null == rightExpression || rightExpression.size() == 0) {
            return argumentValues;
        }
        switch (operatorName.toLowerCase(Locale.ENGLISH)) {
            case "in":
                List<Object> inValue = new ArrayList<>();
                for (Expression rExpression : rightExpression) {
                    inValue.add(rExpression.toString());
                }
                argumentValues = inValue;
                break;
            default:
                argumentValues.add(rightExpression.get(0).toString());
                break;
        }
        return argumentValues;
    }

    private SparkDeserializer initSparkDesializer() {
        DecodeType[] columnTypes = columnTypesList.toArray(new DecodeType[0]);
        int[] columnOrders = columnOrdersList.stream().mapToInt(Integer::intValue).toArray();
        DecodeType[] filterTypes = filterTypesList.toArray(new DecodeType[0]);
        int[] filterOrders = filterOrdersList.stream().mapToInt(Integer::intValue).toArray();
        SparkDeserializer deserializer;
        if (columnTypes.length == 0) {
            deserializer = new SparkDeserializer(filterTypes, filterOrders);
        } else {
            deserializer = new SparkDeserializer(columnTypes, columnOrders);
        }
        return deserializer;
    }

    private DataSource initDataSource(PageCandidate pageCandidate)
        throws UnsupportedOperationException {
        DataSource dataSource;
        String fileFormat = pageCandidate.getFileFormat();
        Long fileStartPos = pageCandidate.getStartPos();
        Long fileLen = pageCandidate.getSplitLen();
        if ("ORC".equalsIgnoreCase(fileFormat)) {
            dataSource = new HdfsOrcDataSource(filePath, fileStartPos, fileLen, false);
        } else if ("PARQUET".equalsIgnoreCase(fileFormat)) {
            dataSource = new HdfsParquetDataSource(filePath, fileStartPos, fileLen, false);
        } else {
            throw new UnsupportedOperationException("unsupported data format : " + fileFormat);
        }
        return dataSource;
    }

    private RowExpression initFilter(Seq<FilterExeInfo> filterExecutions) {
        RowExpression rowExpression = extractFilterExpression(filterExecutions);
        return rowExpression;
    }

    private Optional<AggregationInfo> initAggAndGroupInfo(
        List<AggExeInfo> aggExecutionList) {
        // create AggregationInfo
        Optional<AggregationInfo> aggregationInfo = extractAggAndGroupExpression(aggExecutionList);
        return aggregationInfo;
    }

    private void initColumnInfo(Seq<Attribute> sparkOutPut) {
        if (listAtt == null || listAtt.size() == 0) {
            return;
        }

        List<Attribute> outputColumnList = JavaConverters.seqAsJavaList(sparkOutPut);
        boolean isPartitionKey;
        int filterColumnId = 0;
        for (int p = 0; p < outputColumnList.size(); p++) {
            Attribute resAttribute = NdpUtils.getColumnAttribute(outputColumnList.get(p), listAtt);
            String columnName = resAttribute.name().toLowerCase(Locale.ENGLISH);
            Type type = NdpUtils.transOlkDataType(resAttribute.dataType(), false);
            int columnId = NdpUtils.getColumnId(resAttribute.toString()) - columnOffset;
            isPartitionKey = partitionColumnName.contains(columnName);
            String partitionValue = NdpUtils.getPartitionValue(filePath, columnName);
            omnidataColumns.add(new Column(columnId,
                columnName, type, isPartitionKey, partitionValue));
            omnidataTypes.add(type);
            filterTypesList.add(NdpUtils.transDataIoDataType(resAttribute.dataType()));
            filterOrdersList.add(filterColumnId);
            omnidataProjections.add(new InputReferenceExpression(filterColumnId, type));
            fieldMap.put(columnName, filterColumnId);
            ++filterColumnId;
        }
    }
}



