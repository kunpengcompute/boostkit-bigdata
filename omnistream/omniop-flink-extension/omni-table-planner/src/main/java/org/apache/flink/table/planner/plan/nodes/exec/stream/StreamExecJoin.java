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
 *
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.calcite.rex.RexNode;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link StreamExecNode} for regular Joins.
 *
 * <p>Regular joins are the most generic type of join in which any new records or changes to either
 * side of the join input are visible and are affecting the whole join result.
 */
@ExecNodeMetadata(
        name = "stream-exec-join",
        version = 1,
        producedTransformations = StreamExecJoin.JOIN_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String JOIN_TRANSFORMATION = "join";

    public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";
    public static final String FIELD_NAME_LEFT_UPSERT_KEYS = "leftUpsertKeys";
    public static final String FIELD_NAME_RIGHT_UPSERT_KEYS = "rightUpsertKeys";

    @JsonProperty(FIELD_NAME_JOIN_SPEC)
    private final JoinSpec joinSpec;

    @JsonProperty(FIELD_NAME_LEFT_UPSERT_KEYS)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final List<int[]> leftUpsertKeys;

    @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEYS)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final List<int[]> rightUpsertKeys;

    public StreamExecJoin(
            ReadableConfig tableConfig,
            JoinSpec joinSpec,
            List<int[]> leftUpsertKeys,
            List<int[]> rightUpsertKeys,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecJoin.class, tableConfig),
                joinSpec,
                leftUpsertKeys,
                rightUpsertKeys,
                Lists.newArrayList(leftInputProperty, rightInputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
            @JsonProperty(FIELD_NAME_LEFT_UPSERT_KEYS) List<int[]> leftUpsertKeys,
            @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEYS) List<int[]> rightUpsertKeys,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 2);
        this.joinSpec = checkNotNull(joinSpec);
        this.leftUpsertKeys = leftUpsertKeys;
        this.rightUpsertKeys = rightUpsertKeys;
    }

    private String getExtraDescription(String oldDescription, RowType leftType, RowType rightType, String leftInputSpec, String rightInputSpec) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        //get inputType info
        List<String> leftInputTypeList = new ArrayList<>();
        List<RowType.RowField> leftInputFields = leftType.getFields();

        for(RowType.RowField field : leftInputFields) {
            LogicalType fieldType = field.getType();
            leftInputTypeList.add(DescriptionUtil.getFieldType(fieldType));
        }

        List<String> rightInputTypeList = new ArrayList<>();
        List<RowType.RowField> rightInputFields = rightType.getFields();
        for (RowType.RowField field : rightInputFields) {
            LogicalType fieldType = field.getType();
            rightInputTypeList.add(DescriptionUtil.getFieldType(fieldType));
        }
        //get outputTypes.

        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (RowType.RowField field : fields) {
            LogicalType fieldType = field.getType();
            outputTypeList.add(DescriptionUtil.getFieldType(fieldType));
        }

        //get join keys from both sides.

        //example: [0,1] vs ["0","1"]
        final int[] leftJoinKey = joinSpec.getLeftKeys();
        final int[] rightJoinKey = joinSpec.getRightKeys();
        FlinkJoinType joinType = joinSpec.getJoinType();
        boolean[] filterNulls = joinSpec.getFilterNulls();
        RexNode nonEquiCondition = joinSpec.getNonEquiCondition().orElse(null);
        Map<String, Object> nonEquiConditionMap = null;
        if (nonEquiCondition != null) {
            nonEquiConditionMap = RexNodeUtil.buildJsonMap(nonEquiCondition);
        }
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("leftInputTypes", leftInputTypeList);
        jsonMap.put("rightInputTypes", rightInputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("leftJoinKey", leftJoinKey);
        jsonMap.put("rightJoinKey", rightJoinKey);
        jsonMap.put("nonEquiCondition", nonEquiConditionMap);
        jsonMap.put("joinType", joinType.toString());
        jsonMap.put("filterNulls", filterNulls);
        jsonMap.put("leftInputSpec", leftInputSpec);
        jsonMap.put("rightInputSpec", rightInputSpec);
        jsonMap.put("leftUniqueKeys", leftUpsertKeys);
        jsonMap.put("rightUniqueKeys", rightUpsertKeys);
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();  // Handle the exception or log it
        }

        return jsonString;

    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge leftInputEdge = getInputEdges().get(0);
        final ExecEdge rightInputEdge = getInputEdges().get(1);

        final Transformation<RowData> leftTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        final Transformation<RowData> rightTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        final RowType leftType = (RowType) leftInputEdge.getOutputType();
        final RowType rightType = (RowType) rightInputEdge.getOutputType();
        JoinUtil.validateJoinSpec(joinSpec, leftType, rightType, true);

        final int[] leftJoinKey = joinSpec.getLeftKeys();
        final int[] rightJoinKey = joinSpec.getRightKeys();

        final InternalTypeInfo<RowData> leftTypeInfo = InternalTypeInfo.of(leftType);
        final JoinInputSideSpec leftInputSpec =
                JoinUtil.analyzeJoinInput(
                        planner.getFlinkContext().getClassLoader(),
                        leftTypeInfo,
                        leftJoinKey,
                        leftUpsertKeys);

        final InternalTypeInfo<RowData> rightTypeInfo = InternalTypeInfo.of(rightType);
        final JoinInputSideSpec rightInputSpec =
                JoinUtil.analyzeJoinInput(
                        planner.getFlinkContext().getClassLoader(),
                        rightTypeInfo,
                        rightJoinKey,
                        rightUpsertKeys);

        GeneratedJoinCondition generatedCondition =
                JoinUtil.generateConditionFunction(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        joinSpec,
                        leftType,
                        rightType);

        long minRetentionTime = config.getStateRetentionTime();

        AbstractStreamingJoinOperator operator;
        FlinkJoinType joinType = joinSpec.getJoinType();
        if (joinType == FlinkJoinType.ANTI || joinType == FlinkJoinType.SEMI) {
            operator =
                    new StreamingSemiAntiJoinOperator(
                            joinType == FlinkJoinType.ANTI,
                            leftTypeInfo,
                            rightTypeInfo,
                            generatedCondition,
                            leftInputSpec,
                            rightInputSpec,
                            joinSpec.getFilterNulls(),
                            minRetentionTime);
        } else {
            boolean leftIsOuter = joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL;
            boolean rightIsOuter =
                    joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL;
            operator =
                    new StreamingJoinOperator(
                            leftTypeInfo,
                            rightTypeInfo,
                            generatedCondition,
                            leftInputSpec,
                            rightInputSpec,
                            leftIsOuter,
                            rightIsOuter,
                            joinSpec.getFilterNulls(),
                            minRetentionTime);
        }

        final RowType returnType = (RowType) getOutputType();
        final TwoInputTransformation<RowData, RowData, RowData> transform =
                ExecNodeUtil.createTwoInputTransformation(
                        leftTransform,
                        rightTransform,
                        createTransformationMeta(JOIN_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(returnType),
                        leftTransform.getParallelism());

        // set KeyType and Selector for state
        RowDataKeySelector leftSelect =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(), leftJoinKey, leftTypeInfo);
        RowDataKeySelector rightSelect =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(), rightJoinKey, rightTypeInfo);
        transform.setStateKeySelectors(leftSelect, rightSelect);
        transform.setStateKeyType(leftSelect.getProducedType());
        transform.setDescription(getExtraDescription(transform.getDescription(), leftType, rightType, leftInputSpec.toString(), rightInputSpec.toString()));
        return transform;
    }
}
