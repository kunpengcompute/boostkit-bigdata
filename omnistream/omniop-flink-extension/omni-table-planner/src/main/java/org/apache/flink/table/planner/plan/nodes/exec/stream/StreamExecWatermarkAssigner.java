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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.util.*;

import static org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil.getFieldType;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} which generates watermark based on the input elements. */
@ExecNodeMetadata(
        name = "stream-exec-watermark-assigner",
        version = 1,
        producedTransformations = StreamExecWatermarkAssigner.WATERMARK_ASSIGNER_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecWatermarkAssigner extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String WATERMARK_ASSIGNER_TRANSFORMATION = "watermark-assigner";

    public static final String FIELD_NAME_WATERMARK_EXPR = "watermarkExpr";
    public static final String FIELD_NAME_ROWTIME_FIELD_INDEX = "rowtimeFieldIndex";

    @JsonProperty(FIELD_NAME_WATERMARK_EXPR)
    private final RexNode watermarkExpr;

    @JsonProperty(FIELD_NAME_ROWTIME_FIELD_INDEX)
    private final int rowtimeFieldIndex;

    public StreamExecWatermarkAssigner(
            ReadableConfig tableConfig,
            RexNode watermarkExpr,
            int rowtimeFieldIndex,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecWatermarkAssigner.class),
                ExecNodeContext.newPersistedConfig(StreamExecWatermarkAssigner.class, tableConfig),
                watermarkExpr,
                rowtimeFieldIndex,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    private String getExtraDescription(String oldDescription, long idleTimeout, Transformation<RowData> inputTransform) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        List<String> inputTypeList = new ArrayList<>();
        List<RowType.RowField> inputFields = ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields();

        HashMap<Integer, Integer> accessIndexMap = new HashMap<>();
        HashMap<Integer, Integer> fieldCountMap = new HashMap<>();

        int currentIndex = 0;
        for (int oldIndex = 0; oldIndex < inputFields.size(); oldIndex++) {
            RowType.RowField field = inputFields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if(fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // save the offset and subfield cnt and expand row
                accessIndexMap.put(oldIndex, currentIndex);
                fieldCountMap.put(oldIndex, subFields.size());
                for (RowType.RowField rowField : subFields) {
                    inputTypeList.add(getFieldType(rowField.getType()));
                }
                currentIndex += subFields.size();
            } else {
                accessIndexMap.put(oldIndex, currentIndex);
                inputTypeList.add(getFieldType(fieldType));
                currentIndex++;
            }
        }

        //get outputTypes info
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (int oldIndex = 0; oldIndex < fields.size(); oldIndex++) {
            RowType.RowField field = fields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if(fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // expand row
                for (RowType.RowField rowField : subFields) {
                    outputTypeList.add(getFieldType(rowField.getType()));
                }
            } else {
                outputTypeList.add(getFieldType(fieldType));
            }
        }
        // Set the accessIndexMap
        RexNodeUtil.accessIndexMap = accessIndexMap;
        Map<String, Object> exprMap =null;
        if (watermarkExpr != null) {
            exprMap = RexNodeUtil.buildJsonMap(watermarkExpr);
        }

        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("idleTimeout", idleTimeout);
        jsonMap.put("config", exprMap);
        jsonMap.put("rowtimeFieldIndex", accessIndexMap.getOrDefault(rowtimeFieldIndex, rowtimeFieldIndex));

        ImmutableList<RexNode> operands = ((RexCall) watermarkExpr).operands;
        for (RexNode operand : operands) {
            String name = operand.getType().getSqlTypeName().getName();
            if (name.equals("INTERVAL_SECOND")){
                jsonMap.put("intervalSecond",((RexLiteral) operand).getValue());
                break;
            }
        }

        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();  // Handle the exception or log it
        }
        RexNodeUtil.accessIndexMap.clear();
        return jsonString;
    }
    @JsonCreator
    public StreamExecWatermarkAssigner(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_WATERMARK_EXPR) RexNode watermarkExpr,
            @JsonProperty(FIELD_NAME_ROWTIME_FIELD_INDEX) int rowtimeFieldIndex,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.watermarkExpr = checkNotNull(watermarkExpr);
        this.rowtimeFieldIndex = rowtimeFieldIndex;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final GeneratedWatermarkGenerator watermarkGenerator =
                WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        (RowType) inputEdge.getOutputType(),
                        watermarkExpr,
                        JavaScalaConversionUtil.toScala(Optional.empty()));

        final long idleTimeout =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT).toMillis();

        final WatermarkAssignerOperatorFactory operatorFactory =
                new WatermarkAssignerOperatorFactory(
                        rowtimeFieldIndex, idleTimeout, watermarkGenerator);

        final Transformation<RowData> ret = new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism());
        ret.setDescription(getExtraDescription(ret.getDescription(), idleTimeout, inputTransform));
        return ret;
    }
}
