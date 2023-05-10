/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql;

import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongDecodeType;
import com.huawei.boostkit.omnidata.decode.type.RowDecodeType;
import com.huawei.boostkit.omnidata.model.AggregationInfo;
import com.huawei.boostkit.omnidata.model.Column;
import com.huawei.boostkit.omnidata.model.Predicate;
import com.huawei.boostkit.omnidata.model.TaskSource;
import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsOrcDataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsParquetDataSource;
import com.huawei.boostkit.omnidata.reader.impl.DataReaderImpl;
import com.huawei.boostkit.omnidata.spark.PageDeserializer;

import com.google.common.collect.ImmutableMap;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.DomainTranslator;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.*;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.execution.ndp.NdpConf;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryArithmetic;
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
import org.apache.spark.sql.execution.ndp.AggExeInfo;
import org.apache.spark.sql.execution.ndp.FilterExeInfo;
import org.apache.spark.sql.execution.ndp.PushDownInfo;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.hive.HiveSimpleUDF;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
 */
public class DataIoAdapter {
    private int TASK_FAILED_TIMES = 4;

    private int MAX_PAGE_SIZE_IN_BYTES = 1048576;

    private List<Type> omnidataTypes = new ArrayList<>();

    private List<Column> omnidataColumns = new ArrayList<>();

    private Set<String> columnNameSet = new HashSet<>();

    private List<RowExpression> omnidataProjections = new ArrayList<>();

    private boolean hasNextPage = false;

    private DataReaderImpl<WritableColumnVector[]> orcDataReader = null;

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

    private int taskTimeout = 300;

    private NdpUdfExpressions ndpUdfExpressions = new NdpUdfExpressions();

    private static final Logger LOG = LoggerFactory.getLogger(DataIoAdapter.class);

    private boolean isPushDownAgg = true;

    private boolean isOperatorCombineEnabled;

    /**
     * Contact with Omni-Data-Server
     *
     * @param pageCandidate     file split info
     * @param sparkOutPut       data schema
     * @param partitionColumn   partition column
     * @param filterOutPut      filter schema
     * @param pushDownOperators push down expressions
     * @return WritableColumnVector data result info
     * @throws TaskExecutionException connect to omni-data-server failed exception
     * @notice 3rd parties api throws Exception, function has to catch basic Exception
     */
    public Iterator<WritableColumnVector[]> getPageIterator(
            PageCandidate pageCandidate,
            Seq<Attribute> sparkOutPut,
            Seq<Attribute> partitionColumn,
            Seq<Attribute> filterOutPut,
            PushDownInfo pushDownOperators) throws TaskExecutionException, UnknownHostException {
        // initCandidates
        initCandidates(pageCandidate, filterOutPut);

        // add partition column
        JavaConverters.seqAsJavaList(partitionColumn).forEach(a -> partitionColumnName.add(a.name()));

        // init column info
        if (pushDownOperators.aggExecutions().size() == 0) {
            isPushDownAgg = false;
            initColumnInfo(sparkOutPut);
        }

        // create filter
        Optional<RowExpression> filterRowExpression = initFilter(pushDownOperators.filterExecutions());

        // create agg
        Optional<AggregationInfo> aggregations = initAggAndGroupInfo(pushDownOperators.aggExecutions());

        // create limit
        OptionalLong limitLong = NdpUtils.convertLimitExeInfo(pushDownOperators.limitExecution());

        // create TaskSource
        DataSource dataSource = initDataSource(pageCandidate);

        Predicate predicate = new Predicate(
                omnidataTypes, omnidataColumns, filterRowExpression, omnidataProjections,
                buildDomains(filterRowExpression), ImmutableMap.of(), aggregations, limitLong);
        TaskSource taskSource = new TaskSource(dataSource, predicate, MAX_PAGE_SIZE_IN_BYTES);

        // create deserializer
        this.isOperatorCombineEnabled =
                pageCandidate.isOperatorCombineEnabled() && NdpUtils.checkOmniOpColumns(omnidataColumns);
        PageDeserializer deserializer = initPageDeserializer();

        // get available host
        String[] pushDownHostArray = pageCandidate.getpushDownHosts().split(",");
        List<String> pushDownHostList = new ArrayList<>(Arrays.asList(pushDownHostArray));
        Optional<String> availablePushDownHost = getRandomAvailablePushDownHost(pushDownHostArray,
                JavaConverters.mapAsJavaMap(pushDownOperators.fpuHosts()));
        availablePushDownHost.ifPresent(pushDownHostList::add);
        return getIterator(pushDownHostList.iterator(), taskSource, pushDownHostArray, deserializer,
                pushDownHostList.size());
    }

    private Iterator<WritableColumnVector[]> getIterator(Iterator<String> pushDownHosts, TaskSource taskSource,
                                                         String[] pushDownHostArray, PageDeserializer deserializer,
                                                         int pushDownHostsSize) throws UnknownHostException {
        int randomIndex = (int) (Math.random() * pushDownHostArray.length);
        int failedTimes = 0;
        WritableColumnVector[] page = null;
        Set<String> pushDownHostSet = new HashSet<>();
        pushDownHostSet.add(pushDownHostArray[randomIndex]);
        while (pushDownHosts.hasNext()) {
            String pushDownHost;
            if (failedTimes == 0) {
                pushDownHost = pushDownHostArray[randomIndex];
            } else {
                pushDownHost = pushDownHosts.next();
                if (pushDownHostSet.contains(pushDownHost)) {
                    continue;
                }
            }
            String ipAddress = InetAddress.getByName(pushDownHost).getHostAddress();
            Properties properties = new Properties();
            properties.put("omnidata.client.target.list", ipAddress);
            properties.put("omnidata.client.task.timeout", taskTimeout);
            LOG.info("Push down node info: [hostname :{} ,ip :{}]", pushDownHost, ipAddress);
            try {
                orcDataReader = new DataReaderImpl<>(
                        properties, taskSource, deserializer);
                hasNextPage = true;
                page = orcDataReader.getNextPageBlocking();
                if (orcDataReader.isFinished()) {
                    orcDataReader.close();
                    hasNextPage = false;
                }
                break;
            } catch (Exception e) {
                LOG.warn("Push down failed node info [hostname :{} ,ip :{}]", pushDownHost, ipAddress, e);
                ++failedTimes;
                if (orcDataReader != null) {
                    orcDataReader.close();
                    hasNextPage = false;
                }
            }
        }
        int retryTime = Math.min(TASK_FAILED_TIMES, pushDownHostsSize);
        if (failedTimes >= retryTime) {
            LOG.warn("No Omni-data-server to Connect, Task has tried {} times.", retryTime);
            throw new TaskExecutionException("No Omni-data-server to Connect");
        }
        List<WritableColumnVector[]> l = new ArrayList<>();
        l.add(page);
        return l.iterator();
    }

    private Optional<String> getRandomAvailablePushDownHost(String[] pushDownHostArray,
                                                            Map<String, String> fpuHosts) {
        List<String> existingHosts = Arrays.asList(pushDownHostArray);
        List<String> allHosts = new ArrayList<>(fpuHosts.values());
        allHosts.removeAll(existingHosts);
        if (allHosts.size() > 0) {
            LOG.info("Add another available host: " + allHosts.get(0));
            return Optional.of(allHosts.get(0));
        } else {
            return Optional.empty();
        }
    }

    public boolean hasNextIterator(List<Object> pageList, PageToColumnar pageToColumnarClass,
                                   boolean isVectorizedReader, Seq<Attribute> sparkOutPut, String orcImpl) {
        if (!hasNextPage) {
            return false;
        }
        WritableColumnVector[] page = orcDataReader.getNextPageBlocking();
        if (orcDataReader.isFinished()) {
            orcDataReader.close();
            hasNextPage = false;
            return false;
        }
        List<WritableColumnVector[]> l = new ArrayList<>();
        l.add(page);
        pageList.addAll(pageToColumnarClass
                .transPageToColumnar(l.iterator(), isVectorizedReader, isOperatorCombineEnabled, sparkOutPut, orcImpl));
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
        TASK_FAILED_TIMES = pageCandidate.getMaxFailedTimes();
        taskTimeout = pageCandidate.getTaskTimeout();
        isPushDownAgg = true;
    }

    private RowExpression extractNamedExpression(NamedExpression namedExpression) {
        Type prestoType = NdpUtils.transOlkDataType(((Expression) namedExpression).dataType(), namedExpression,
                false);
        int aggProjectionId;
        String aggColumnName = namedExpression.name();
        columnOrdersList.add(columnOrder++);
        columnTypesList.add(NdpUtils.transDecodeType(((Expression) namedExpression).dataType()));

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
            columnNameMap.computeIfAbsent(aggColumnName, k -> columnNameMap.size());
            omnidataProjections.add(new InputReferenceExpression(aggProjectionId, prestoType));
        }

        return new InputReferenceExpression(aggProjectionId, prestoType);
    }

    private void extractSumMaxMinAggregation(Type prestoType, String expressionName,
                                             Map<String, AggregationInfo.AggregateFunction> aggregationMap) {
        String operatorName = expressionName.split("\\(")[0];
        List<RowExpression> arguments = new ArrayList<>();
        Type returnType = NdpUtils.transAggRetType(prestoType);
        if (operatorName.equals("sum")) {
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
                expression.left().dataType(), expression.left(), false);
        Type rightPrestoType = NdpUtils.transOlkDataType(
                expression.right().dataType(), expression.right(), false);
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
        Type prestoType = NdpUtils.transOlkDataType(expression.dataType(), expression, false);
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
                return createAggBinCall((Remainder) expression, "Modulus", prestoType);
            case Literal:
                return NdpUtils.transConstantExpression(expression.toString(), prestoType);
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
            if (!(expression instanceof Literal)) {
                omnidataProjections.add(createAggProjection(expression));
                int projectionId = fieldMap.size();
                fieldMap.put(aggregateFunctionName, projectionId);
                if (aggregateFunctionType.equals(AggregateFunctionType.Count)
                        || aggregateFunctionType.equals(AggregateFunctionType.Average)) {
                    prestoType = NdpUtils.transOlkDataType(expression.dataType(), expression, false);
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
        IsNull,
        AttributeReference
    }

    private Optional<AggregationInfo> createAggregationInfo(
            List<AggregateFunction> aggregateFunctions,
            List<NamedExpression> namedExpressions) {
        List<RowExpression> groupingKeys = new ArrayList<>();
        Map<String, AggregationInfo.AggregateFunction> aggregationMap = new LinkedHashMap<>();
        boolean isEmpty = true;
        for (NamedExpression namedExpression : namedExpressions) {
            RowExpression groupingKey = extractNamedExpression(namedExpression);
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
        String operatorName;
        switch (expressionOperType) {
            case Or:
            case And:
                return reverseExpressionTree(filterExpression);
            case Not:
                if (!(filterExpression instanceof Not)) {
                    return resRowExpression;
                }
                if (((Not) filterExpression).child() instanceof EqualTo) {
                    EqualTo equalToExpression = (EqualTo) ((Not) filterExpression).child();
                    if (equalToExpression.left() instanceof Literal) {
                        rightExpressions.add(equalToExpression.left());
                        left = equalToExpression.right();
                    } else {
                        rightExpressions.add(equalToExpression.right());
                        left = equalToExpression.left();
                    }
                    return getRowExpression(left,
                            "NOT_EQUAL", rightExpressions);
                }
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
                        "EQUAL", rightExpressions);
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
                    operatorName = "GREATER_THAN";
                } else {
                    rightExpressions.add(((LessThan) filterExpression).right());
                    left = ((LessThan) filterExpression).left();
                    operatorName = "LESS_THAN";
                }
                return getRowExpression(left,
                        operatorName, rightExpressions);
            case GreaterThan:
                if (((GreaterThan) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((GreaterThan) filterExpression).left());
                    left = ((GreaterThan) filterExpression).right();
                    operatorName = "LESS_THAN";
                } else {
                    rightExpressions.add(((GreaterThan) filterExpression).right());
                    left = ((GreaterThan) filterExpression).left();
                    operatorName = "GREATER_THAN";
                }
                return getRowExpression(left,
                        operatorName, rightExpressions);
            case GreaterThanOrEqual:
                if (((GreaterThanOrEqual) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((GreaterThanOrEqual) filterExpression).left());
                    left = ((GreaterThanOrEqual) filterExpression).right();
                    operatorName = "LESS_THAN_OR_EQUAL";
                } else {
                    rightExpressions.add(((GreaterThanOrEqual) filterExpression).right());
                    left = ((GreaterThanOrEqual) filterExpression).left();
                    operatorName = "GREATER_THAN_OR_EQUAL";
                }
                return getRowExpression(left,
                        operatorName, rightExpressions);
            case LessThanOrEqual:
                if (((LessThanOrEqual) filterExpression).left() instanceof Literal) {
                    rightExpressions.add(((LessThanOrEqual) filterExpression).left());
                    left = ((LessThanOrEqual) filterExpression).right();
                    operatorName = "GREATER_THAN_OR_EQUAL";
                } else {
                    rightExpressions.add(((LessThanOrEqual) filterExpression).right());
                    left = ((LessThanOrEqual) filterExpression).left();
                    operatorName = "LESS_THAN_OR_EQUAL";
                }
                return getRowExpression(left,
                        operatorName, rightExpressions);
            case In:
                if (!(filterExpression instanceof In)) {
                    return resRowExpression;
                }
                In in = (In) filterExpression;
                List<Expression> rightExpression =
                        JavaConverters.seqAsJavaList(in.list());
                // check if filed on right
                if (rightExpression.size() == 1 && rightExpression.get(0) instanceof AttributeReference
                        && in.value() instanceof Literal) {
                    List<Expression> newRightExpression = new ArrayList<>();
                    newRightExpression.add(in.value());
                    return getRowExpression(rightExpression.get(0), "in", newRightExpression);
                }
                return getRowExpression(in.value(), "in", rightExpression);
            case HiveSimpleUDF:
                return getRowExpression(filterExpression,
                        ((HiveSimpleUDF) filterExpression).name(), rightExpressions);
            case AttributeReference:
                Type type = NdpUtils.transOlkDataType(filterExpression.dataType(), filterExpression,
                        false);
                return new InputReferenceExpression(putFilterValue(filterExpression, type), type);
            default:
                return resRowExpression;
        }
    }

    private RowExpression getRowExpression(Expression leftExpression, String operatorName,
                                           List<Expression> rightExpression) {
        PrestoExpressionInfo expressionInfo = new PrestoExpressionInfo();
        Type prestoType;
        int filterProjectionId;
        // deal with left expression only UDF and Attribute
        if (leftExpression instanceof AttributeReference) {
            prestoType = NdpUtils.transOlkDataType(leftExpression.dataType(), leftExpression, false);
            filterProjectionId = putFilterValue(leftExpression, prestoType);
        } else if (leftExpression instanceof HiveSimpleUDF) {
            for (int i = 0; i < leftExpression.children().length(); i++) {
                Expression childExpr = leftExpression.children().apply(i);
                if (childExpr instanceof Attribute) {
                    putFilterValue(childExpr, NdpUtils.transOlkDataType(childExpr.dataType(),
                            childExpr, false));
                } else if (!(childExpr instanceof Literal)) {
                    putFilterValue(childExpr, NdpUtils.transOlkDataType(childExpr.dataType(), false));
                }
            }
            ndpUdfExpressions.createNdpUdf(leftExpression, expressionInfo, fieldMap);
            prestoType = expressionInfo.getReturnType();
            filterProjectionId = expressionInfo.getProjectionId();
        } else {
            ndpUdfExpressions.createNdpUdf(leftExpression, expressionInfo, fieldMap);
            putFilterValue(expressionInfo.getChildExpression(), expressionInfo.getFieldDataType());
            prestoType = expressionInfo.getReturnType();
            filterProjectionId = expressionInfo.getProjectionId();
        }
        // deal with right expression
        List<Object> argumentValues;
        List<RowExpression> multiArguments = new ArrayList<>();
        int rightProjectionId;
        RowExpression rowExpression;
        if (rightExpression != null && rightExpression.size() > 0 &&
                rightExpression.get(0) instanceof AttributeReference) {
            rightProjectionId = putFilterValue(rightExpression.get(0), prestoType);
            multiArguments.add(new InputReferenceExpression(filterProjectionId, prestoType));
            multiArguments.add(new InputReferenceExpression(rightProjectionId, prestoType));
            rowExpression = NdpFilterUtils.generateRowExpression(
                    operatorName, expressionInfo, prestoType, filterProjectionId,
                    null, multiArguments, "multy_columns");
        } else {
            // get right value
            argumentValues = getValue(rightExpression, operatorName,
                    leftExpression.dataType().toString());
            rowExpression = NdpFilterUtils.generateRowExpression(
                    operatorName, expressionInfo, prestoType, filterProjectionId,
                    argumentValues, null, operatorName);
        }
        return rowExpression;
    }

    // column projection
    private int putFilterValue(Expression valueExpression, Type prestoType) {
        // set filter
        int columnId = NdpUtils.getColumnId(valueExpression.toString()) - columnOffset;
        String filterColumnName = valueExpression.toString().split("#")[0].toLowerCase(Locale.ENGLISH);
        if (null != fieldMap.get(filterColumnName)) {
            return fieldMap.get(filterColumnName);
        }
        boolean isPartitionKey = partitionColumnName.contains(filterColumnName);
        int filterProjectionId = fieldMap.size();
        fieldMap.put(filterColumnName, filterProjectionId);

        String partitionValue = NdpUtils.getPartitionValue(filePath, filterColumnName);
        columnNameSet.add(filterColumnName);
        omnidataColumns.add(new Column(columnId, filterColumnName,
                prestoType, isPartitionKey, partitionValue));
        if (isPushDownAgg) {
            filterTypesList.add(NdpUtils.transDecodeType(valueExpression.dataType()));
            filterOrdersList.add(filterProjectionId);
            omnidataProjections.add(new InputReferenceExpression(filterProjectionId, prestoType));
            omnidataTypes.add(prestoType);
        }
        if (null == columnNameMap.get(filterColumnName)) {
            columnNameMap.put(filterColumnName, columnNameMap.size());
        }
        return filterProjectionId;
    }

    private List<Object> getValue(List<Expression> rightExpression,
                                  String operatorName,
                                  String sparkType) {
        List<Object> argumentValues = new ArrayList<>();
        if (null == rightExpression || rightExpression.size() == 0) {
            return argumentValues;
        }
        if ("in".equals(operatorName.toLowerCase(Locale.ENGLISH))) {
            List<Object> inValue = new ArrayList<>();
            for (Expression rExpression : rightExpression) {
                inValue.add(rExpression.toString());
            }
            argumentValues = inValue;
        } else {
            argumentValues.add(rightExpression.get(0).toString());
        }
        return argumentValues;
    }

    private PageDeserializer initPageDeserializer() {
        DecodeType[] columnTypes = columnTypesList.toArray(new DecodeType[0]);
        int[] columnOrders = columnOrdersList.stream().mapToInt(Integer::intValue).toArray();
        DecodeType[] filterTypes = filterTypesList.toArray(new DecodeType[0]);
        int[] filterOrders = filterOrdersList.stream().mapToInt(Integer::intValue).toArray();
        if (columnTypes.length == 0) {
            return new PageDeserializer(filterTypes, filterOrders, isOperatorCombineEnabled);
        } else {
            return new PageDeserializer(columnTypes, columnOrders, isOperatorCombineEnabled);
        }
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

    public Optional<RowExpression> initFilter(Seq<FilterExeInfo> filterExecutions) {
        List<FilterExeInfo> filterExecutionList = JavaConverters.seqAsJavaList(filterExecutions);
        Optional<RowExpression> resRowExpression = Optional.empty();
        for (FilterExeInfo filterExeInfo : filterExecutionList) {
            resRowExpression = Optional.ofNullable(reverseExpressionTree(filterExeInfo.filter()));
        }
        return resRowExpression;
    }

    private Optional<AggregationInfo> initAggAndGroupInfo(
            Seq<AggExeInfo> aggExeInfoSeq) {
        List<AggExeInfo> aggExecutionList =
                JavaConverters.seqAsJavaList(aggExeInfoSeq);
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

    private void initColumnInfo(Seq<Attribute> sparkOutPut) {
        if (listAtt == null || listAtt.size() == 0) {
            return;
        }

        List<Attribute> outputColumnList = JavaConverters.seqAsJavaList(sparkOutPut);
        boolean isPartitionKey;
        int filterColumnId = 0;
        for (Attribute attribute : outputColumnList) {
            Attribute resAttribute = NdpUtils.getColumnAttribute(attribute, listAtt);
            String columnName = resAttribute.name().toLowerCase(Locale.ENGLISH);
            Type type = NdpUtils.transOlkDataType(resAttribute.dataType(), resAttribute, false);
            int columnId = NdpUtils.getColumnId(resAttribute.toString()) - columnOffset;
            isPartitionKey = partitionColumnName.contains(columnName);
            String partitionValue = NdpUtils.getPartitionValue(filePath, columnName);
            omnidataColumns.add(new Column(columnId,
                    columnName, type, isPartitionKey, partitionValue));
            omnidataTypes.add(type);
            filterTypesList.add(NdpUtils.transDecodeType(resAttribute.dataType()));
            filterOrdersList.add(filterColumnId);
            omnidataProjections.add(new InputReferenceExpression(filterColumnId, type));
            fieldMap.put(columnName, filterColumnId);
            ++filterColumnId;
        }
    }

    public boolean isOperatorCombineEnabled() {
        return isOperatorCombineEnabled;
    }

    public ImmutableMap buildDomains(Optional<RowExpression> filterRowExpression) {
        long startTime = System.currentTimeMillis();
        ImmutableMap.Builder<String, Domain> domains = ImmutableMap.builder();
        if (filterRowExpression.isPresent() && NdpConf.getNdpDomainGenerateEnable(TaskContext.get())) {
            ConnectorSession session = MetaStore.getConnectorSession();
            RowExpressionDomainTranslator domainTranslator = new RowExpressionDomainTranslator(MetaStore.getMetadata());
            DomainTranslator.ColumnExtractor<InputReferenceExpression> columnExtractor = (expression, domain) -> {
                if (expression instanceof InputReferenceExpression) {
                    return Optional.of((InputReferenceExpression) expression);
                }
                return Optional.empty();
            };
            DomainTranslator.ExtractionResult<InputReferenceExpression> extractionResult = domainTranslator
                    .fromPredicate(session, filterRowExpression.get(), columnExtractor);
            if (!extractionResult.getTupleDomain().isNone()) {
                extractionResult.getTupleDomain().getDomains().get().forEach((columnHandle, domain) -> {
                    Type type = domain.getType();
                    //  unSupport dataType skip
                    if (type instanceof MapType ||
                            type instanceof ArrayType ||
                            type instanceof RowType ||
                            type instanceof DecimalType ||
                            type instanceof TimestampType) {
                        return;
                    }

                    domains.put(omnidataColumns.get(columnHandle.getField()).getName(), domain);
                });
            }
        }

        ImmutableMap<String, Domain> domainImmutableMap = domains.build();
        long costTime = System.currentTimeMillis() - startTime;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Push down generate domain cost time:" + costTime + ";generate domain:" + domainImmutableMap.size());
        }
        return domainImmutableMap;
    }
}
