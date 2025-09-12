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

import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.CollectDynamicSink;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stream {@link ExecNode} to to write data into an external sink defined by a {@link
 * DynamicTableSink}.
 */
@ExecNodeMetadata(
        name = "stream-exec-sink",
        version = 1,
        consumedOptions = {
            "table.exec.sink.not-null-enforcer",
            "table.exec.sink.type-length-enforcer",
            "table.exec.sink.upsert-materialize",
            "table.exec.sink.keyed-shuffle"
        },
        producedTransformations = {
            CommonExecSink.CONSTRAINT_VALIDATOR_TRANSFORMATION,
            CommonExecSink.PARTITIONER_TRANSFORMATION,
            CommonExecSink.UPSERT_MATERIALIZE_TRANSFORMATION,
            CommonExecSink.TIMESTAMP_INSERTER_TRANSFORMATION,
            CommonExecSink.SINK_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecSink extends CommonExecSink implements StreamExecNode<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCalc.class);

    public static final String FIELD_NAME_INPUT_CHANGELOG_MODE = "inputChangelogMode";
    public static final String FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE = "requireUpsertMaterialize";
    public static final String FIELD_NAME_INPUT_UPSERT_KEY = "inputUpsertKey";

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE)
    private final ChangelogMode inputChangelogMode;

    @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean upsertMaterialize;

    @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final int[] inputUpsertKey;

    public StreamExecSink(
            ReadableConfig tableConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            InputProperty inputProperty,
            LogicalType outputType,
            boolean upsertMaterialize,
            int[] inputUpsertKey,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecSink.class),
                ExecNodeContext.newPersistedConfig(StreamExecSink.class, tableConfig),
                tableSinkSpec,
                inputChangelogMode,
                Collections.singletonList(inputProperty),
                outputType,
                upsertMaterialize,
                inputUpsertKey,
                description);
    }

    @JsonCreator
    public StreamExecSink(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK) DynamicTableSinkSpec tableSinkSpec,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) ChangelogMode inputChangelogMode,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
            @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE) boolean upsertMaterialize,
            @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY) int[] inputUpsertKey,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                tableSinkSpec,
                inputChangelogMode,
                false, // isBounded
                inputProperties,
                outputType,
                description);
        this.inputChangelogMode = inputChangelogMode;
        this.upsertMaterialize = upsertMaterialize;
        this.inputUpsertKey = inputUpsertKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        checkState(inputEdge.getOutputType() instanceof RowType);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final DynamicTableSink tableSink = tableSinkSpec.getTableSink(planner.getFlinkContext());
        final boolean isCollectSink = tableSink instanceof CollectDynamicSink;

        final List<Integer> rowtimeFieldIndices = new ArrayList<>();
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                rowtimeFieldIndices.add(i);
            }
        }
        final int rowtimeFieldIndex;
        if (rowtimeFieldIndices.size() > 1 && !isCollectSink) {
            throw new TableException(
                    String.format(
                            "The query contains more than one rowtime attribute column [%s] for writing into table '%s'.\n"
                                    + "Please select the column that should be used as the event-time timestamp "
                                    + "for the table sink by casting all other columns to regular TIMESTAMP or TIMESTAMP_LTZ.",
                            rowtimeFieldIndices.stream()
                                    .map(i -> inputRowType.getFieldNames().get(i))
                                    .collect(Collectors.joining(", ")),
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString()));
        } else if (rowtimeFieldIndices.size() == 1) {
            rowtimeFieldIndex = rowtimeFieldIndices.get(0);
        } else {
            rowtimeFieldIndex = -1;
        }

        Transformation<Object> transformation = createSinkTransformation(
                planner.getExecEnv(),
                config,
                planner.getFlinkContext().getClassLoader(),
                inputTransform,
                tableSink,
                rowtimeFieldIndex,
                upsertMaterialize,
                inputUpsertKey);

        String oldDescription = transformation.getDescription();
        transformation.setDescription(getExtraDescription(oldDescription, inputTransform));

        return transformation;
    }

    private String getExtraDescription(String oldDescription, Transformation<RowData> inputTransform) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        HashMap<Integer, Integer> accessIndexMap = new HashMap<>();
        HashMap<Integer, Integer> fieldCountMap = new HashMap<>();

        //get inputType info
        List<String> inputTypeList = new ArrayList<>();
        List<RowType.RowField> inputFields = ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields();
        int currentIndex = 0;
        for (int oldIndex = 0; oldIndex < inputFields.size(); oldIndex++) {
            RowType.RowField field = inputFields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if (fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // save the offset and subfield cnt and expand row
                accessIndexMap.put(oldIndex, currentIndex);
                fieldCountMap.put(oldIndex, subFields.size());
                for (RowType.RowField rowField : subFields) {
                    inputTypeList.add(DescriptionUtil.getFieldType(rowField.getType()));
                }
                currentIndex += subFields.size();
            } else {
                inputTypeList.add(DescriptionUtil.getFieldType(fieldType));
                currentIndex++;
            }
        }

        //get outputTypes info
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (int oldIndex = 0; oldIndex < fields.size(); oldIndex++) {
            RowType.RowField field = fields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if (fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // expand row
                for (RowType.RowField rowField : subFields) {
                    outputTypeList.add(DescriptionUtil.getFieldType(rowField.getType()));
                }
            } else {
                outputTypeList.add(DescriptionUtil.getFieldType(fieldType));
            }
        }
        // Set the accessIndexMap
        RexNodeUtil.accessIndexMap = accessIndexMap;
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e); // Handle the exception or log it
        }
        RexNodeUtil.accessIndexMap.clear();
        return jsonString;
    }
}
