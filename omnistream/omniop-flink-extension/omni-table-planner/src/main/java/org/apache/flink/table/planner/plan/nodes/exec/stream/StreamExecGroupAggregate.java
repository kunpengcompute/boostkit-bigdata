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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.DistinctInfo;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stream {@link ExecNode} for unbounded group aggregate.
 *
 * <p>This node does support un-splittable aggregate function (e.g. STDDEV_POP).
 */
@ExecNodeMetadata(
        name = "stream-exec-group-aggregate",
        version = 1,
        consumedOptions = {"table.exec.mini-batch.enabled", "table.exec.mini-batch.size"},
        producedTransformations = StreamExecGroupAggregate.GROUP_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecGroupAggregate extends StreamExecAggregateBase {

    public static final String GROUP_AGGREGATE_TRANSFORMATION = "group-aggregate";
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecGroupAggregate.class);
    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    /**
     * Each element indicates whether the corresponding agg call needs `retract` method.
     */
    @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS)
    private final boolean[] aggCallNeedRetractions;

    /**
     * Whether this node will generate UPDATE_BEFORE messages.
     */
    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    /**
     * Whether this node consumes retraction messages.
     */
    @JsonProperty(FIELD_NAME_NEED_RETRACTION)
    private final boolean needRetraction;

    public StreamExecGroupAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            boolean generateUpdateBefore,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGroupAggregate.class),
                ExecNodeContext.newPersistedConfig(StreamExecGroupAggregate.class, tableConfig),
                grouping,
                aggCalls,
                aggCallNeedRetractions,
                generateUpdateBefore,
                needRetraction,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecGroupAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS) boolean[] aggCallNeedRetractions,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.aggCallNeedRetractions = checkNotNull(aggCallNeedRetractions);
        checkArgument(aggCalls.length == aggCallNeedRetractions.length);
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
    }

    private String getExtraDescription(String oldDescription, AggregateInfoList aggInfoList, LogicalType[] accTypes,
        LogicalType[] aggValueTypes, Transformation<RowData> inputTransform, String name) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        List<String> inputTypeList = getInputTypes(inputTransform);

        List<String> outputTypeList = getOutputTypes();

        //get aggInfoList info map
        Map<String, Object> aggInfoListMap = new LinkedHashMap<>();
        List<Map<String, Object>> aggregateCalls = getAggregateCalls(aggInfoList);
        aggInfoListMap.put("aggregateCalls", aggregateCalls);
        List<String> accTypesList = new ArrayList<>();
        for (LogicalType accType : accTypes) {
            accTypesList.add(accType.toString());
        }
        aggInfoListMap.put("accTypes", accTypesList); // Empty list

        List<String> aggValueTypesList = new ArrayList<>();
        for (LogicalType accType : aggValueTypes) {
            aggValueTypesList.add(accType.toString());
        }
        aggInfoListMap.put("aggValueTypes", aggValueTypesList);
        aggInfoListMap.put("indexOfCountStar", aggInfoList.getIndexOfCountStar());

        // distinctInfo List Map
        List<Map<String, Object>> distinctInfos = getDistinctInfos(aggInfoList);

        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("grouping", grouping);
        jsonMap.put("aggInfoList", aggInfoListMap);
        jsonMap.put("generateUpdateBefore", generateUpdateBefore);
        jsonMap.put("distinctInfos", distinctInfos);
        if (name.contains("$e")) {
            jsonMap.put("groupingSets", "INVALID");
        }
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);
        }
        return jsonString;
    }

    private static List<Map<String, Object>> getAggregateCalls(AggregateInfoList aggInfoList) {
        AggregateInfo[] aggInfos = aggInfoList.getActualAggregateInfos();
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

    private static List<Map<String, Object>> getDistinctInfos(AggregateInfoList aggInfoList) {
        List<Map<String, Object>> distinctInfos = new ArrayList<>();
        for (DistinctInfo distinctInfo : aggInfoList.distinctInfos()) {
            Map<String, Object> distinctInfoMap = new LinkedHashMap<>();
            distinctInfoMap.put("argIndexes", distinctInfo.argIndexes());
            distinctInfoMap.put("filterArgs", distinctInfo.filterArgs().toArray(ClassTag$.MODULE$.apply(Integer.TYPE)));
            distinctInfoMap.put("aggIndexes", distinctInfo.aggIndexes().toArray(ClassTag$.MODULE$.apply(Integer.TYPE)));
            distinctInfos.add(distinctInfoMap);
        }
        return distinctInfos;
    }

    private List<String> getOutputTypes() {
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (RowType.RowField field : fields) {
            LogicalType fieldType = field.getType();
            LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
            String typeName = typeRoot.toString();
            if (typeRoot == LogicalTypeRoot.VARCHAR) {
                VarCharType varcharType = (VarCharType) fieldType;
                typeName += "(" + varcharType.getLength() + ")";
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
            outputTypeList.add(typeName);
        }
        return outputTypeList;
    }

    private static List<String> getInputTypes(Transformation<RowData> inputTransform) {
        List<String> inputTypeList = new ArrayList<>();
        List<RowType.RowField> inputFields =
            ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields();
        for (RowType.RowField field : inputFields) {
            LogicalType fieldType = field.getType();
            LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
            String typeName = typeRoot.toString();
            if (typeRoot == LogicalTypeRoot.VARCHAR) {
                VarCharType varcharType = (VarCharType) fieldType;
                typeName += "(" + varcharType.getLength() + ")";
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
            inputTypeList.add(typeName);
        }
        return inputTypeList;
    }


    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (grouping.length > 0 && config.getStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent excessive "
                            + "state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        planner.createRelBuilder(),
                        JavaScalaConversionUtil.toScala(inputRowType.getChildren()),

                        //  we have to copy input field

                        //  improve this in future

                        true)
                        .needAccumulate();

        if (needRetraction) {
            generator.needRetract();
        }

        final AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        planner.getTypeFactory(),
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        true,
                        true);
        final GeneratedAggsHandleFunction aggsHandler =
                generator.generateAggsHandler("GroupAggsHandler", aggInfoList);

        final LogicalType[] accTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        final LogicalType[] aggValueTypes =
                Arrays.stream(aggInfoList.getActualValueTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        final GeneratedRecordEqualiser recordEqualiser =
                new EqualiserCodeGenerator(
                        aggValueTypes, planner.getFlinkContext().getClassLoader())
                        .generateRecordEqualiser("GroupAggValueEqualiser");
        final int inputCountIndex = aggInfoList.getIndexOfCountStar();
        final boolean isMiniBatchEnabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);

        final OneInputStreamOperator<RowData, RowData> operator;
        if (isMiniBatchEnabled) {
            MiniBatchGroupAggFunction aggFunction =
                    new MiniBatchGroupAggFunction(
                            aggsHandler,
                            recordEqualiser,
                            accTypes,
                            inputRowType,
                            inputCountIndex,
                            generateUpdateBefore,
                            config.getStateRetentionTime());
            operator =
                    new KeyedMapBundleOperator<>(
                            aggFunction, AggregateUtil.createMiniBatchTrigger(config));
        } else {
            GroupAggFunction aggFunction =
                    new GroupAggFunction(
                            aggsHandler,
                            recordEqualiser,
                            accTypes,
                            inputCountIndex,
                            generateUpdateBefore,
                            config.getStateRetentionTime());
            operator = new KeyedProcessOperator<>(aggFunction);
        }

        // partitioned aggregation
        final OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDescription(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        String oldDescription = transform.getDescription();
        transform.setDescription(getExtraDescription(oldDescription, aggInfoList, accTypes, aggValueTypes,
            inputTransform, transform.getName()));

        return transform;
    }
}
