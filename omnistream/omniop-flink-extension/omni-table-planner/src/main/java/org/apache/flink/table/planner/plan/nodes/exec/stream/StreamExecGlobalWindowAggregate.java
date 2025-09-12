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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.*;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.SlicingWindowAggOperatorBuilder;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based global aggregate. */
@ExecNodeMetadata(
        name = "stream-exec-global-window-aggregate",
        version = 1,
        consumedOptions = "table.local-time-zone",
        producedTransformations =
                StreamExecGlobalWindowAggregate.GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecGlobalWindowAggregate extends StreamExecWindowAggregateBase {
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecGlobalWindowAggregate.class);


    public static final String GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION = "global-window-aggregate";

    public static final String FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE = "localAggInputRowType";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_WINDOWING)
    private final WindowingStrategy windowing;

    @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
    private final NamedWindowProperty[] namedWindowProperties;

    /** The input row type of this node's local agg. */
    @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE)
    private final RowType localAggInputRowType;

    public StreamExecGlobalWindowAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            WindowingStrategy windowing,
            NamedWindowProperty[] namedWindowProperties,
            InputProperty inputProperty,
            RowType localAggInputRowType,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGlobalWindowAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecGlobalWindowAggregate.class, tableConfig),
                grouping,
                aggCalls,
                windowing,
                namedWindowProperties,
                Collections.singletonList(inputProperty),
                localAggInputRowType,
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecGlobalWindowAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
            @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
            NamedWindowProperty[] namedWindowProperties,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE) RowType localAggInputRowType,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.windowing = checkNotNull(windowing);
        this.namedWindowProperties = checkNotNull(namedWindowProperties);
        this.localAggInputRowType = localAggInputRowType;
    }

    private String getExtraDescription(String oldDescription, AggregateInfoList localAggInfoList,
                                       AggregateInfoList globalAggInfoList, LogicalType[] globalAccTypes,
                                       LogicalType[] globalAggValueTypes, Transformation<RowData> inputTransform,
                                       ZoneId shiftTimeZone) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        StreamExecGlobalGroupAggregate.setGlobalGroupAggDescription(jsonMap, this, oldDescription,
                localAggInfoList, globalAggInfoList, globalAccTypes, globalAggValueTypes, inputTransform,
                grouping, false);

        WindowSpec windowSpec= windowing.getWindow();
        getSliceAssignerInfo(windowing,shiftTimeZone,jsonMap);
        //window info
        String typeName = DescriptionUtil.getFieldType(windowing.getTimeAttributeType());
        jsonMap.put("window", windowSpec.toString());
        jsonMap.put("timeAttributeType",typeName);
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);  // Handle the exception or log it
        }
        return jsonString;
    }

    private void getSliceAssignerInfo(
            WindowingStrategy windowingStrategy, ZoneId shiftTimeZone,Map<String, Object> jsonMap) {
        WindowSpec windowSpec = windowingStrategy.getWindow();
        if (windowingStrategy instanceof WindowAttachedWindowingStrategy) {
            int windowEndIndex =
                    ((WindowAttachedWindowingStrategy) windowingStrategy).getWindowEnd();
            buildJsonMap(windowSpec, Integer.MAX_VALUE, shiftTimeZone,jsonMap);
            jsonMap.put("windowEndIndex",windowEndIndex);

        } else if (windowingStrategy instanceof SliceAttachedWindowingStrategy) {
            int sliceEndIndex = ((SliceAttachedWindowingStrategy) windowingStrategy).getSliceEnd();
            buildJsonMap(windowSpec, Integer.MAX_VALUE, shiftTimeZone,jsonMap);
            jsonMap.put("sliceEndIndex",sliceEndIndex);

        } else if (windowingStrategy instanceof TimeAttributeWindowingStrategy) {
            final int timeAttributeIndex;
            if (windowingStrategy.isRowtime()) {
                timeAttributeIndex =
                        ((TimeAttributeWindowingStrategy) windowingStrategy)
                                .getTimeAttributeIndex();
            } else {
                timeAttributeIndex = -1;
            }
            buildJsonMap(windowSpec, timeAttributeIndex, shiftTimeZone,jsonMap);

        } else {
            throw new UnsupportedOperationException(windowingStrategy + " is not supported yet.");
        }
    }

    private void buildJsonMap(
            WindowSpec windowSpec, int timeAttributeIndex, ZoneId shiftTimeZone,Map<String, Object> jsonMap) {
        if (windowSpec instanceof TumblingWindowSpec) {
            Duration size = ((TumblingWindowSpec) windowSpec).getSize();
            Duration offset = ((TumblingWindowSpec) windowSpec).getOffset();
            if (offset != null) {
                jsonMap.put("offset",offset.toMillis());
            }
            jsonMap.put("timeAttributeIndex",timeAttributeIndex);
//            jsonMap.put("shiftTimeZone",shiftTimeZone);
            jsonMap.put("size",size.toMillis());
        } else if (windowSpec instanceof HoppingWindowSpec) {
            Duration size = ((HoppingWindowSpec) windowSpec).getSize();
            Duration slide = ((HoppingWindowSpec) windowSpec).getSlide();
            if (size.toMillis() % slide.toMillis() != 0) {
                throw new TableException(
                        String.format(
                                "HOP table function based aggregate requires size must be an "
                                        + "integral multiple of slide, but got size %s ms and slide %s ms",
                                size.toMillis(), slide.toMillis()));
            }
            Duration offset = ((HoppingWindowSpec) windowSpec).getOffset();
            if (offset != null) {
                jsonMap.put("offset",offset.toMillis());
            }
            jsonMap.put("timeAttributeIndex",timeAttributeIndex);
//            jsonMap.put("shiftTimeZone",shiftTimeZone);
            jsonMap.put("size",size.toMillis());
            jsonMap.put("slide",slide.toMillis());
        } else if (windowSpec instanceof CumulativeWindowSpec) {
            Duration maxSize = ((CumulativeWindowSpec) windowSpec).getMaxSize();
            Duration step = ((CumulativeWindowSpec) windowSpec).getStep();
            if (maxSize.toMillis() % step.toMillis() != 0) {
                throw new TableException(
                        String.format(
                                "CUMULATE table function based aggregate requires maxSize must be an "
                                        + "integral multiple of step, but got maxSize %s ms and step %s ms",
                                maxSize.toMillis(), step.toMillis()));
            }
            Duration offset = ((CumulativeWindowSpec) windowSpec).getOffset();
            if (offset != null) {
                jsonMap.put("offset",offset.toMillis());
            }
            jsonMap.put("timeAttributeIndex",timeAttributeIndex);
//            jsonMap.put("shiftTimeZone",shiftTimeZone);
            jsonMap.put("maxSize",maxSize.toMillis());
            jsonMap.put("step",step.toMillis());
        } else {
            throw new UnsupportedOperationException(windowSpec + " is not supported yet.");
        }
    }

    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowing.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config));
        final SliceAssigner sliceAssigner = createSliceAssigner(windowing, shiftTimeZone);

        final AggregateInfoList localAggInfoList =
                AggregateUtil.deriveStreamWindowAggregateInfoList(
                        planner.getTypeFactory(),
                        localAggInputRowType, // should use original input here
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        windowing.getWindow(),
                        false); // isStateBackendDataViews

        final AggregateInfoList globalAggInfoList =
                AggregateUtil.deriveStreamWindowAggregateInfoList(
                        planner.getTypeFactory(),
                        localAggInputRowType, // should use original input here
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        windowing.getWindow(),
                        true); // isStateBackendDataViews

        // handler used to merge multiple local accumulators into one accumulator,
        // where the accumulators are all on memory
        final GeneratedNamespaceAggsHandleFunction<Long> localAggsHandler =
                createAggsHandler(
                        "LocalWindowAggsHandler",
                        sliceAssigner,
                        localAggInfoList,
                        grouping.length,
                        true,
                        localAggInfoList.getAccTypes(),
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder(),
                        shiftTimeZone);

        // handler used to merge the single local accumulator (on memory) into state accumulator
        final GeneratedNamespaceAggsHandleFunction<Long> globalAggsHandler =
                createAggsHandler(
                        "GlobalWindowAggsHandler",
                        sliceAssigner,
                        globalAggInfoList,
                        0,
                        true,
                        localAggInfoList.getAccTypes(),
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder(),
                        shiftTimeZone);

        // handler used to merge state accumulators for merging slices into window,
        // e.g. Hop and Cumulate
        final GeneratedNamespaceAggsHandleFunction<Long> stateAggsHandler =
                createAggsHandler(
                        "StateWindowAggsHandler",
                        sliceAssigner,
                        globalAggInfoList,
                        0,
                        false,
                        globalAggInfoList.getAccTypes(),
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder(),
                        shiftTimeZone);

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        grouping,
                        InternalTypeInfo.of(inputRowType));
        final LogicalType[] accTypes = convertToLogicalTypes(globalAggInfoList.getAccTypes());

        final LogicalType[] globalAccTypes =
                Arrays.stream(globalAggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        final LogicalType[] globalAggValueTypes =
                Arrays.stream(globalAggInfoList.getActualValueTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        final OneInputStreamOperator<RowData, RowData> windowOperator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(new RowDataSerializer(inputRowType))
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(
                                (PagedTypeSerializer<RowData>)
                                        selector.getProducedType().toSerializer())
                        .assigner(sliceAssigner)
                        .countStarIndex(globalAggInfoList.getIndexOfCountStar())
                        .globalAggregate(
                                localAggsHandler,
                                globalAggsHandler,
                                stateAggsHandler,
                                new RowDataSerializer(accTypes))
                        .build();

        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION, config),
                        SimpleOperatorFactory.of(windowOperator),
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        WINDOW_AGG_MEMORY_RATIO);

        // set KeyType and Selector for state
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        String oldDescription= transform.getDescription();
        transform.setDescription(getExtraDescription(oldDescription, localAggInfoList, globalAggInfoList,
                globalAccTypes, globalAggValueTypes, inputTransform,shiftTimeZone));

        return transform;
    }

    private GeneratedNamespaceAggsHandleFunction<Long> createAggsHandler(
            String name,
            SliceAssigner sliceAssigner,
            AggregateInfoList aggInfoList,
            int mergedAccOffset,
            boolean mergedAccIsOnHeap,
            DataType[] mergedAccExternalTypes,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RelBuilder relBuilder,
            ZoneId shifTimeZone) {
        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        new CodeGeneratorContext(config, classLoader),
                        relBuilder,
                        JavaScalaConversionUtil.toScala(localAggInputRowType.getChildren()),
                        true) // copyInputField
                        .needAccumulate()
                        .needMerge(mergedAccOffset, mergedAccIsOnHeap, mergedAccExternalTypes);

        final List<WindowProperty> windowProperties =
                Arrays.asList(
                        Arrays.stream(namedWindowProperties)
                                .map(NamedWindowProperty::getProperty)
                                .toArray(WindowProperty[]::new));

        return generator.generateNamespaceAggsHandler(
                name,
                aggInfoList,
                JavaScalaConversionUtil.toScala(windowProperties),
                sliceAssigner,
                shifTimeZone);
    }
}
