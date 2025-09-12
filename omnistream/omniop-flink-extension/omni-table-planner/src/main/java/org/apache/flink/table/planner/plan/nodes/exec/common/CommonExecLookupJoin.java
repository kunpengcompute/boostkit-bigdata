/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.common;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink.PARTITIONER_TRANSFORMATION;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.plan.utils.ReflectionUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.KeyedLookupJoinWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * CommonExecLookupJoin
 *
 * @since 2025-04-27
 */
public abstract class CommonExecLookupJoin extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {
    /**
     * LOOKUP_JOIN_TRANSFORMATION
     */
    public static final String LOOKUP_JOIN_TRANSFORMATION = "lookup-join";

    /**
     * LOOKUP_JOIN_MATERIALIZE_TRANSFORMATION
     */
    public static final String LOOKUP_JOIN_MATERIALIZE_TRANSFORMATION = "lookup-join-materialize";

    /**
     * FIELD_NAME_JOIN_TYPE
     */
    public static final String FIELD_NAME_JOIN_TYPE = "joinType";

    /**
     * FIELD_NAME_JOIN_CONDITION
     */
    public static final String FIELD_NAME_JOIN_CONDITION = "joinCondition";

    /**
     * FIELD_NAME_TEMPORAL_TABLE
     */
    public static final String FIELD_NAME_TEMPORAL_TABLE = "temporalTable";

    /**
     * FIELD_NAME_LOOKUP_KEYS
     */
    public static final String FIELD_NAME_LOOKUP_KEYS = "lookupKeys";

    /**
     * FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE
     */
    public static final String FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE =
            "projectionOnTemporalTable";

    /**
     * FIELD_NAME_FILTER_ON_TEMPORAL_TABLE
     */
    public static final String FIELD_NAME_FILTER_ON_TEMPORAL_TABLE = "filterOnTemporalTable";

    /**
     * FIELD_NAME_INPUT_CHANGELOG_MODE
     */
    public static final String FIELD_NAME_INPUT_CHANGELOG_MODE = "inputChangelogMode";

    /**
     * FIELD_NAME_ASYNC_OPTIONS
     */
    public static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";

    /**
     * FIELD_NAME_RETRY_OPTIONS
     */
    public static final String FIELD_NAME_RETRY_OPTIONS = "retryOptions";

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecLookupJoin.class);

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    /**
     * lookup keys: the key is index in dim table. the value is source of lookup key either constant
     * or field from right table.
     */
    @JsonProperty(FIELD_NAME_LOOKUP_KEYS)
    private final Map<Integer, LookupJoinUtil.LookupKey> lookupKeys;

    @JsonProperty(FIELD_NAME_TEMPORAL_TABLE)
    private final TemporalTableSourceSpec temporalTableSourceSpec;

    @JsonProperty(FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE)
    @Nullable
    private final List<RexNode> projectionOnTemporalTable;

    @JsonProperty(FIELD_NAME_FILTER_ON_TEMPORAL_TABLE)
    @Nullable
    private final RexNode filterOnTemporalTable;

    /**
     * join condition except equi-conditions extracted as lookup keys.
     */
    @JsonProperty(FIELD_NAME_JOIN_CONDITION)
    @Nullable
    private final RexNode joinCondition;

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE)
    private final ChangelogMode inputChangelogMode;

    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final LookupJoinUtil.AsyncLookupOptions asyncLookupOptions;

    @JsonProperty(FIELD_NAME_RETRY_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final LookupJoinUtil.RetryLookupOptions retryOptions;

    protected CommonExecLookupJoin(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            @Nullable
            RexNode joinCondition,
            TemporalTableSourceSpec temporalTableSourceSpec,
            Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            @Nullable
            List<RexNode> projectionOnTemporalTable,
            @Nullable
            RexNode filterOnTemporalTable,
            @Nullable
            LookupJoinUtil.AsyncLookupOptions asyncLookupOptions,
            @Nullable
            LookupJoinUtil.RetryLookupOptions retryOptions,
            ChangelogMode inputChangelogMode,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.joinType = checkNotNull(joinType);
        this.joinCondition = joinCondition;
        this.lookupKeys = Collections.unmodifiableMap(checkNotNull(lookupKeys));
        this.temporalTableSourceSpec = checkNotNull(temporalTableSourceSpec);
        this.projectionOnTemporalTable = projectionOnTemporalTable;
        this.filterOnTemporalTable = filterOnTemporalTable;
        this.inputChangelogMode = inputChangelogMode;
        this.asyncLookupOptions = asyncLookupOptions;
        this.retryOptions = retryOptions;
    }

    private static Map<String, String> getOptions(RelOptTable temporalTable) {
        Map<String, String> options = new HashMap<>();
        if (temporalTable instanceof TableSourceTable) {
            options = ((TableSourceTable) temporalTable).contextResolvedTable().getResolvedTable().getOptions();
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            options = ((LegacyTableSourceTable<?>) temporalTable).catalogTable().getOptions();
        }
        return options;
    }

    public TemporalTableSourceSpec getTemporalTableSourceSpec() {
        return temporalTableSourceSpec;
    }

    /**
     * createJoinTransformation
     *
     * @param planner planner
     * @param config config
     * @param upsertMaterialize upsertMaterialize
     * @param lookupKeyContainsPrimaryKey lookupKeyContainsPrimaryKey
     * @return Transformation<RowData>
     */
    protected Transformation<RowData> createJoinTransformation(
            PlannerBase planner,
            ExecNodeConfig config,
            boolean upsertMaterialize,
            boolean lookupKeyContainsPrimaryKey) {
        RelOptTable temporalTable =
                temporalTableSourceSpec.getTemporalTable(
                        planner.getFlinkContext(), unwrapTypeFactory(planner));
        // validate whether the node is valid and supported.
        validate(temporalTable);
        final ExecEdge inputEdge = getInputEdges().get(0);
        RowType inputRowType = (RowType) inputEdge.getOutputType();
        RowType tableSourceRowType = FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType());
        RowType resultRowType = (RowType) getOutputType();
        validateLookupKeyType(lookupKeys, inputRowType, tableSourceRowType);
        boolean isAsyncEnabled = null != asyncLookupOptions;
        ResultRetryStrategy retryStrategy =
                retryOptions != null ? retryOptions.toRetryStrategy() : null;

        UserDefinedFunction lookupFunction =
                LookupJoinUtil.getLookupFunction(
                        temporalTable,
                        lookupKeys.keySet(),
                        planner.getFlinkContext().getClassLoader(),
                        isAsyncEnabled,
                        retryStrategy);
        UserDefinedFunctionHelper.prepareInstance(config, lookupFunction);

        boolean isLeftOuterJoin = joinType == FlinkJoinType.LEFT;
        if (isAsyncEnabled) {
            assert lookupFunction instanceof AsyncTableFunction;
        }

        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        Transformation<RowData> transformation;

        if (upsertMaterialize) {
            // upsertMaterialize only works on sync lookup mode, async lookup is unsupported.
            assert !isAsyncEnabled && !inputChangelogMode.containsOnly(RowKind.INSERT);
            transformation = createSyncLookupJoinWithState(
                    inputTransformation,
                    temporalTable,
                    config,
                    planner.getFlinkContext().getClassLoader(),
                    lookupKeys,
                    (TableFunction<Object>) lookupFunction,
                    planner.createRelBuilder(),
                    inputRowType,
                    tableSourceRowType,
                    resultRowType,
                    isLeftOuterJoin,
                    planner.getExecEnv().getConfig().isObjectReuseEnabled(),
                    lookupKeyContainsPrimaryKey);
        } else {
            StreamOperatorFactory<RowData> operatorFactory;
            if (isAsyncEnabled) {
                operatorFactory =
                        createAsyncLookupJoin(
                                temporalTable,
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                lookupKeys,
                                (AsyncTableFunction<Object>) lookupFunction,
                                planner.createRelBuilder(),
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                isLeftOuterJoin,
                                asyncLookupOptions);
            } else {
                operatorFactory =
                        createSyncLookupJoin(
                                temporalTable,
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                lookupKeys,
                                (TableFunction<Object>) lookupFunction,
                                planner.createRelBuilder(),
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                isLeftOuterJoin,
                                planner.getExecEnv().getConfig().isObjectReuseEnabled());
            }

            transformation = ExecNodeUtil.createOneInputTransformation(
                    inputTransformation,
                    createTransformationMeta(LOOKUP_JOIN_TRANSFORMATION, config),
                    operatorFactory,
                    InternalTypeInfo.of(resultRowType),
                    inputTransformation.getParallelism());
        }
        transformation.setDescription(getExtraDescription(transformation.getDescription(), inputRowType, temporalTable));
        return transformation;
    }


    private Transformation<RowData> createSyncLookupJoinWithState(
            Transformation<RowData> inputTransformation, RelOptTable temporalTable,
            ExecNodeConfig config, ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder, RowType inputRowType,
            RowType tableSourceRowType, RowType resultRowType,
            boolean isLeftOuterJoin, boolean isObjectReuseEnabled, boolean lookupKeyContainsPrimaryKey) {
        // create lookup function first
        ProcessFunction<RowData, RowData> processFunction =
                createSyncLookupJoinFunction(
                        temporalTable, config, classLoader, allLookupKeys,
                        syncLookupFunction, relBuilder, inputRowType, tableSourceRowType,
                        resultRowType, isLeftOuterJoin, isObjectReuseEnabled);

        RowType rightRowType =
                getRightOutputRowType(
                        getProjectionOutputRelDataType(relBuilder), tableSourceRowType);
        checkState(processFunction instanceof LookupJoinRunner);
        KeyedLookupJoinWrapper keyedLookupJoinWrapper =
                new KeyedLookupJoinWrapper(
                        (LookupJoinRunner) processFunction,
                        StateConfigUtil.createTtlConfig(
                                config.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis()),
                        InternalSerializers.create(rightRowType),
                        lookupKeyContainsPrimaryKey);

        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(keyedLookupJoinWrapper);

        List<Integer> refKeys =
                allLookupKeys.values().stream()
                        .filter(key -> key instanceof LookupJoinUtil.FieldRefLookupKey)
                        .map(key -> ((LookupJoinUtil.FieldRefLookupKey) key).index)
                        .collect(Collectors.toList());
        RowDataKeySelector keySelector;

        // use single parallelism for empty key shuffle
        boolean singleParallelism = refKeys.isEmpty();
        if (singleParallelism) {
            // all lookup keys are constants, then use an empty key selector
            keySelector = EmptyRowDataKeySelector.INSTANCE;
        } else {
            // make it a deterministic asc order
            Collections.sort(refKeys);
            keySelector = KeySelectorUtil.getRowDataSelector(
                            classLoader,
                            refKeys.stream().mapToInt(Integer::intValue).toArray(),
                            InternalTypeInfo.of(inputRowType));
        }
        final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                new KeyGroupStreamPartitioner<>(
                        keySelector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        Transformation<RowData> partitionedTransform =
                new PartitionTransformation<>(inputTransformation, partitioner);
        createTransformationMeta(PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                .fill(partitionedTransform);
        if (singleParallelism) {
            setSingletonParallelism(partitionedTransform);
        } else {
            partitionedTransform.setParallelism(inputTransformation.getParallelism());
        }

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(partitionedTransform,
                        createTransformationMeta(LOOKUP_JOIN_MATERIALIZE_TRANSFORMATION, config),
                        operator, InternalTypeInfo.of(resultRowType),
                        partitionedTransform.getParallelism());
        transform.setStateKeySelector(keySelector);
        transform.setStateKeyType(keySelector.getProducedType());
        if (singleParallelism) {
            setSingletonParallelism(transform);
        }
        return transform;
    }

    private void setSingletonParallelism(Transformation transformation) {
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
    }

    /**
     * validateLookupKeyType
     *
     * @param lookupKeys lookupKeys
     * @param inputRowType inputRowType
     * @param tableSourceRowType tableSourceRowType
     */
    protected void validateLookupKeyType(
            final Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            final RowType inputRowType,
            final RowType tableSourceRowType) {
        final List<String> imCompatibleConditions = new LinkedList<>();
        lookupKeys.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof LookupJoinUtil.FieldRefLookupKey)
                .forEach(
                        entry -> {
                            int rightKey = entry.getKey();
                            int leftKey =
                                    ((LookupJoinUtil.FieldRefLookupKey) entry.getValue()).index;
                            LogicalType leftType = inputRowType.getTypeAt(leftKey);
                            LogicalType rightType = tableSourceRowType.getTypeAt(rightKey);
                            boolean isCompatible =
                                    PlannerTypeUtils.isInteroperable(leftType, rightType);
                            if (!isCompatible) {
                                String leftName = inputRowType.getFieldNames().get(leftKey);
                                String rightName = tableSourceRowType.getFieldNames().get(rightKey);
                                imCompatibleConditions.add(
                                        String.format(
                                                "%s[%s]=%s[%s]",
                                                leftName, leftType, rightName, rightType));
                            }
                        });

        if (!imCompatibleConditions.isEmpty()) {
            throw new TableException(
                    "Temporal table join requires equivalent condition "
                            + "of the same type, but the condition is "
                            + StringUtils.join(imCompatibleConditions, ","));
        }
    }

    @SuppressWarnings("unchecked")
    private StreamOperatorFactory<RowData> createAsyncLookupJoin(
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            AsyncTableFunction<Object> asyncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            LookupJoinUtil.AsyncLookupOptions asyncLookupOptions) {
        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();

        LookupJoinCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                generatedFuncWithType =
                LookupJoinCodeGenerator.generateAsyncLookupFunction(
                        config,
                        classLoader,
                        dataTypeFactory,
                        inputRowType,
                        tableSourceRowType,
                        resultRowType,
                        allLookupKeys,
                        LookupJoinUtil.getOrderedLookupKeys(allLookupKeys.keySet()),
                        asyncLookupFunction,
                        StringUtils.join(temporalTable.getQualifiedName(), "."));

        RelDataType projectionOutputRelDataType = getProjectionOutputRelDataType(relBuilder);
        RowType rightRowType =
                getRightOutputRowType(projectionOutputRelDataType, tableSourceRowType);
        // a projection or filter after table source scan
        GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture =
                LookupJoinCodeGenerator.generateTableAsyncCollector(
                        config,
                        classLoader,
                        "TableFunctionResultFuture",
                        inputRowType,
                        rightRowType,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(joinCondition)));

        DataStructureConverter<?, ?> fetcherConverter =
                DataStructureConverters.getConverter(generatedFuncWithType.dataType());
        AsyncFunction<RowData, RowData> asyncFunc;
        if (projectionOnTemporalTable != null) {
            // a projection or filter after table source scan
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            classLoader,
                            JavaScalaConversionUtil.toScala(projectionOnTemporalTable),
                            filterOnTemporalTable,
                            projectionOutputRelDataType,
                            tableSourceRowType);
            asyncFunc =
                    new AsyncLookupJoinWithCalcRunner(
                            generatedFuncWithType.tableFunc(),
                            (DataStructureConverter<RowData, Object>) fetcherConverter,
                            generatedCalc,
                            generatedResultFuture,
                            InternalSerializers.create(rightRowType),
                            isLeftOuterJoin,
                            asyncLookupOptions.asyncBufferCapacity);
        } else {
            // right type is the same as table source row type, because no calc after temporal table
            asyncFunc =
                    new AsyncLookupJoinRunner(
                            generatedFuncWithType.tableFunc(),
                            (DataStructureConverter<RowData, Object>) fetcherConverter,
                            generatedResultFuture,
                            InternalSerializers.create(rightRowType),
                            isLeftOuterJoin,
                            asyncLookupOptions.asyncBufferCapacity);
        }

        // Why not directly enable retry on 'AsyncWaitOperator'? because of two reasons:
        // 1. AsyncLookupJoinRunner has a 'stateful' resultFutureBuffer bind to each input record
        // (it's non-reenter-able) 2. can not lookup new value if cache empty values enabled when
        // chained with the new AsyncCachingLookupFunction. So similar to sync lookup join with
        // retry, use a 'RetryableAsyncLookupFunctionDelegator' to support retry.
        return new AsyncWaitOperatorFactory<>(
                asyncFunc,
                asyncLookupOptions.asyncTimeout,
                asyncLookupOptions.asyncBufferCapacity,
                asyncLookupOptions.asyncOutputMode);
    }

    private StreamOperatorFactory<RowData> createSyncLookupJoin(
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled) {
        return SimpleOperatorFactory.of(
                new ProcessOperator<>(
                        createSyncLookupJoinFunction(
                                temporalTable,
                                config,
                                classLoader,
                                allLookupKeys,
                                syncLookupFunction,
                                relBuilder,
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                isLeftOuterJoin,
                                isObjectReuseEnabled)));
    }

    private RelDataType getProjectionOutputRelDataType(RelBuilder relBuilder) {
        return projectionOnTemporalTable != null
                ? RexUtil.createStructType(unwrapTypeFactory(relBuilder), projectionOnTemporalTable)
                : null;
    }

    private RowType getRightOutputRowType(
            RelDataType projectionOutputRelDataType, RowType tableSourceRowType) {
        return (projectionOutputRelDataType != null
                && toLogicalType(projectionOutputRelDataType) instanceof RowType)
                ? (RowType) toLogicalType(projectionOutputRelDataType)
                : tableSourceRowType;
    }

    private ProcessFunction<RowData, RowData> createSyncLookupJoinFunction(
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled) {
        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();

        int[] orderedLookupKeys = LookupJoinUtil.getOrderedLookupKeys(allLookupKeys.keySet());

        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
                LookupJoinCodeGenerator.generateSyncLookupFunction(
                        config,
                        classLoader,
                        dataTypeFactory,
                        inputRowType,
                        tableSourceRowType,
                        resultRowType,
                        allLookupKeys,
                        orderedLookupKeys,
                        syncLookupFunction,
                        StringUtils.join(temporalTable.getQualifiedName(), "."),
                        isObjectReuseEnabled);

        RelDataType projectionOutputRelDataType = getProjectionOutputRelDataType(relBuilder);
        RowType rightRowType =
                getRightOutputRowType(projectionOutputRelDataType, tableSourceRowType);
        GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
                LookupJoinCodeGenerator.generateCollector(
                        new CodeGeneratorContext(config, classLoader),
                        inputRowType,
                        rightRowType,
                        resultRowType,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(joinCondition)),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        true);
        ProcessFunction<RowData, RowData> processFunc;
        if (projectionOnTemporalTable != null) {
            // a projection or filter after table source scan
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            classLoader,
                            JavaScalaConversionUtil.toScala(projectionOnTemporalTable),
                            filterOnTemporalTable,
                            projectionOutputRelDataType,
                            tableSourceRowType);

            processFunc =
                    new LookupJoinWithCalcRunner(
                            generatedFetcher,
                            generatedCalc,
                            generatedCollector,
                            isLeftOuterJoin,
                            rightRowType.getFieldCount());
        } else {
            // right type is the same as table source row type, because no calc after temporal table
            processFunc =
                    new LookupJoinRunner(
                            generatedFetcher,
                            generatedCollector,
                            isLeftOuterJoin,
                            rightRowType.getFieldCount());
        }
        return processFunc;
    }

    private void validate(RelOptTable temporalTable) {
        // validate table source and function implementation first
        validateTableSource(temporalTable);

        // check join on all fields of PRIMARY KEY or (UNIQUE) INDEX
        if (lookupKeys.isEmpty()) {
            throw new TableException(
                    String.format(
                            "Temporal table join requires an equality condition on fields of %s.",
                            getTableSourceDescription(temporalTable)));
        }

        // check type
        if (joinType != FlinkJoinType.LEFT && joinType != FlinkJoinType.INNER) {
            throw new TableException(
                    String.format(
                            "Temporal table join currently only support INNER JOIN and LEFT JOIN, but was %s JOIN.",
                            joinType.toString()));
        }
        // success
    }

    private String getTableSourceDescription(RelOptTable temporalTable) {
        if (temporalTable instanceof TableSourceTable) {
            return String.format(
                    "table [%s]",
                    ((TableSourceTable) temporalTable)
                            .contextResolvedTable()
                            .getIdentifier()
                            .asSummaryString());
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            return String.format(
                    "table [%s]",
                    ((LegacyTableSourceTable<?>) temporalTable)
                            .tableIdentifier()
                            .asSummaryString());
        }
        // should never reach here.
        return "";
    }

    private int[] getTableSourceSelectedFields(RelOptTable temporalTable) {
        if (temporalTable instanceof LegacyTableSourceTable) {
            TableSource<?> tableSource = ((LegacyTableSourceTable<?>) temporalTable).tableSource();
            if (tableSource instanceof CsvTableSource) {
                CsvTableSource tableSource1 = (CsvTableSource) tableSource;
                Object config = ReflectionUtils.retrievePrivateField(tableSource1, "config");
                return ReflectionUtils.retrievePrivateField(config, "selectedFields");
            }
        }
        return new int[0];
    }

    private void validateTableSource(RelOptTable temporalTable) {
        if (temporalTable instanceof TableSourceTable) {
            if (!(((TableSourceTable) temporalTable).tableSource() instanceof LookupTableSource)) {
                throw new TableException(
                        String.format(
                                "%s must implement LookupTableSource interface if it is used in temporal table join.",
                                getTableSourceDescription(temporalTable)));
            }
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            TableSource<?> tableSource = ((LegacyTableSourceTable<?>) temporalTable).tableSource();
            if (!(tableSource instanceof LookupableTableSource)) {
                throw new TableException(
                        String.format(
                                "%s must implement LookupableTableSource interface "
                                        + "if it is used in temporal table join.",
                                getTableSourceDescription(temporalTable)));
            }
            TypeInformation<?> tableSourceProducedType =
                    TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(
                            tableSource.getProducedDataType());
            if (!(tableSourceProducedType instanceof InternalTypeInfo
                    && tableSourceProducedType
                    .getTypeClass()
                    .isAssignableFrom(RowData.class))
                    && !(tableSourceProducedType instanceof RowTypeInfo)) {
                throw new TableException(
                        String.format(
                                "Temporal table join only support Row "
                                        + "or RowData type as return type of temporal table. But was %s.",
                                tableSourceProducedType));
            }
        } else {
            throw new TableException(
                    String.format(
                            "table [%s] is neither TableSourceTable not LegacyTableSourceTable.",
                            StringUtils.join(temporalTable.getQualifiedName(), ".")));
        }
    }

    private String getExtraDescription(String oldDescription, RowType inputRowType, RelOptTable temporalTable) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        //get stream input types
        List<String> inputTypeList = new ArrayList<>();
        List<RowType.RowField> inputFields = inputRowType.getFields();

        for (RowType.RowField field : inputFields) {
            LogicalType fieldType = field.getType();
            inputTypeList.add(DescriptionUtil.getFieldType(fieldType));
        }

        //get outputTypes info
        List<String> outputTypeList = getOutputTypes();
        //get condition info
        Map<String, Object> conditionMap = null;
        if (joinCondition != null) {
            conditionMap = RexNodeUtil.buildJsonMap(joinCondition);
        }
        //get projectionOnTemporalTable
        List<Map<String, Object>> projectionList = new ArrayList<>();
        if (projectionOnTemporalTable != null) {
            for (RexNode rexNode : projectionOnTemporalTable) {
                projectionList.add(RexNodeUtil.buildJsonMap(rexNode));
            }
        }
        //get filterOnTemporalTable info
        Map<String, Object> filterMap = null;
        if (filterOnTemporalTable != null) {
            filterMap = RexNodeUtil.buildJsonMap(filterOnTemporalTable);
        }
        List<String> lookupList = getLookupList(temporalTable);
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("lookupInputTypes", lookupList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("joinType", joinType.toString());
        jsonMap.put("condition", conditionMap);
        jsonMap.put("projectionOnTemporalTable", projectionList);
        jsonMap.put("filterOnTemporalTable", filterMap);
        jsonMap.put("lookupKeys", lookupKeys);
        jsonMap.put("temporalTableSourceSpec", getTableSourceDescription(temporalTable));
        jsonMap.put("temporalTableSourceTypeName", getTableSourceTypeName(temporalTable));
        jsonMap.put("selectedFields", getTableSourceSelectedFields(temporalTable));

        Map<String, String> options = getOptions(temporalTable);
        jsonMap.put("connectorType", options.getOrDefault("connector.type", "filesystem"));
        jsonMap.put("connectorPath", options.getOrDefault("connector.path", "file://${FLINK_HOME}/data/side_input.txt"));
        jsonMap.put("formatType", options.getOrDefault("format.type", "csv"));

        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);  // Handle the exception or log it
        }
        return jsonString;
    }

    private List<String> getLookupList(RelOptTable temporalTable) {
        RelDataType rowType = temporalTable.getRowType();
        List<String> lookupList = new ArrayList<>();

        List<RelDataTypeField> rightFields = rowType.getFieldList();
        for (RelDataTypeField field : rightFields) {
            String typeName = field.getType().toString();
            lookupList.add(typeName);
        }
        return lookupList;
    }

    private List<String> getOutputTypes() {
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (RowType.RowField field : fields) {
            LogicalType fieldType = field.getType();
            outputTypeList.add(DescriptionUtil.getFieldType(fieldType));
        }
        return outputTypeList;
    }

    private String getTableSourceTypeName(RelOptTable temporalTable) {
        if (temporalTable instanceof TableSourceTable) {
            return ((TableSourceTable) temporalTable).tableSource().getClass().getSimpleName();
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            return ((LegacyTableSourceTable<?>) temporalTable).tableSource().getClass().getSimpleName();
        }
        // should never reach here.
        return "";
    }
}