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

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.deduplicate.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} which deduplicate on keys and keeps only first row or last row. This node
 * is an optimization of {@link StreamExecRank} for some special cases. Compared to {@link
 * StreamExecRank}, this node could use mini-batch and access less state.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@ExecNodeMetadata(
        name = "stream-exec-deduplicate",
        version = 1,
        consumedOptions = {
                "table.exec.mini-batch.enabled",
                "table.exec.mini-batch.size",
                "table.exec.deduplicate.insert-update-after-sensitive-enabled",
                "table.exec.deduplicate.mini-batch.compact-changes-enabled"
        },
        producedTransformations = StreamExecDeduplicate.DEDUPLICATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecDeduplicate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecDeduplicate.class);
    public static final String DEDUPLICATE_TRANSFORMATION = "deduplicate";

    public static final String FIELD_NAME_UNIQUE_KEYS = "uniqueKeys";
    public static final String FIELD_NAME_IS_ROWTIME = "isRowtime";
    public static final String FIELD_NAME_KEEP_LAST_ROW = "keepLastRow";
    public static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";

    @Experimental
    public static final ConfigOption<Boolean> TABLE_EXEC_INSERT_AND_UPDATE_AFTER_SENSITIVE =
            ConfigOptions.key("table.exec.insert-and-updateafter-sensitive")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Set whether the job (especially the sinks) is sensitive to "
                                    + "INSERT messages and UPDATE_AFTER messages. "
                                    + "If false, Flink may send UPDATE_AFTER instead of INSERT for the first row "
                                    + "at some times (e.g. deduplication for last row). "
                                    + "If true, Flink will guarantee to send INSERT for the first row, "
                                    + "but there will be additional overhead."
                                    + "Default is true.");

    @Experimental
    public static final ConfigOption<Boolean> TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES =
            ConfigOptions.key("table.exec.deduplicate.mini-batch.compact-changes")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Set whether to compact the changes sent downstream in row-time mini-batch. "
                                    + "If true, Flink will compact changes, only send the latest change to downstream. "
                                    + "Notes: If the downstream needs the details of versioned data, this optimization cannot be opened. "
                                    + "If false, Flink will send all changes to downstream just like when the mini-batch is not on.");

    @JsonProperty(FIELD_NAME_UNIQUE_KEYS)
    private final int[] uniqueKeys;

    @JsonProperty(FIELD_NAME_IS_ROWTIME)
    private final boolean isRowtime;

    @JsonProperty(FIELD_NAME_KEEP_LAST_ROW)
    private final boolean keepLastRow;

    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    public StreamExecDeduplicate(
            ReadableConfig tableConfig,
            int[] uniqueKeys,
            boolean isRowtime,
            boolean keepLastRow,
            boolean generateUpdateBefore,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecDeduplicate.class),
                ExecNodeContext.newPersistedConfig(StreamExecDeduplicate.class, tableConfig),
                uniqueKeys,
                isRowtime,
                keepLastRow,
                generateUpdateBefore,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecDeduplicate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_UNIQUE_KEYS) int[] uniqueKeys,
            @JsonProperty(FIELD_NAME_IS_ROWTIME) boolean isRowtime,
            @JsonProperty(FIELD_NAME_KEEP_LAST_ROW) boolean keepLastRow,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.uniqueKeys = checkNotNull(uniqueKeys);
        this.isRowtime = isRowtime;
        this.keepLastRow = keepLastRow;
        this.generateUpdateBefore = generateUpdateBefore;
    }


    private String getExtraDescription(TableConfig tableConfig, String oldDescription,
                                       Transformation<RowData> inputTransform, RowType inputRowType) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        //get inputType info
        List<String> inputTypeList = DescriptionUtil.getFieldTypeList(
                ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields()
        );

        //get outputTypes info
        List<String> outputTypeList = DescriptionUtil.getFieldTypeList(((RowType) getOutputType()).getFields());


        // get isCompactChanges
        boolean isCompactChanges= tableConfig
                .getConfiguration()
                .getBoolean(TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES);

        // get minRetentionTime
        long minRetentionTime= tableConfig.getMinIdleStateRetentionTime();

        // get rowtimeIndex
        int rowtimeIndex = -1;
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                rowtimeIndex = i;
                break;
            }
        }

        // get generateUpdateBefore directly

        // get generateInsert
        boolean generateInsert = tableConfig
                .getConfiguration()
                .getBoolean(TABLE_EXEC_INSERT_AND_UPDATE_AFTER_SENSITIVE);

        // get keepLastRow directly


        // get miniBatchSize
        long miniBatchSize = -1;
        if (tableConfig
                .getConfiguration()
                .getBoolean(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)){
            miniBatchSize = tableConfig
                    .getConfiguration()
                    .getLong(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
        }


        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("isCompactChanges", isCompactChanges);
        jsonMap.put("minRetentionTime", minRetentionTime);
        jsonMap.put("rowtimeIndex", rowtimeIndex);
        jsonMap.put("generateUpdateBefore", generateUpdateBefore);
        jsonMap.put("generateInsert", generateInsert);
        jsonMap.put("keepLastRow", keepLastRow);
        jsonMap.put("miniBatchSize", miniBatchSize);
        jsonMap.put("isRowtime", isRowtime);
        jsonMap.put("grouping", uniqueKeys);
        String jsonString="";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);  // Handle the exception or log it
        }
        return jsonString;
    }


    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final InternalTypeInfo<RowData> rowTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        final TypeSerializer<RowData> rowSerializer =
                rowTypeInfo.createSerializer(planner.getExecEnv().getConfig());
        final OneInputStreamOperator<RowData, RowData> operator;

        if (isRowtime) {
            operator =
                    new RowtimeDeduplicateOperatorTranslator(
                            config,
                            rowTypeInfo,
                            rowSerializer,
                            inputRowType,
                            keepLastRow,
                            generateUpdateBefore)
                            .createDeduplicateOperator();
        } else {
            operator =
                    new ProcTimeDeduplicateOperatorTranslator(
                            config,
                            planner.getFlinkContext().getClassLoader(),
                            rowTypeInfo,
                            rowSerializer,
                            inputRowType,
                            keepLastRow,
                            generateUpdateBefore)
                            .createDeduplicateOperator();
        }

        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(DEDUPLICATE_TRANSFORMATION, config),
                        operator,
                        rowTypeInfo,
                        inputTransform.getParallelism());

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(), uniqueKeys, rowTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        String oldDescription= transform.getDescription();
        transform.setDescription(getExtraDescription(planner.getTableConfig()
                ,oldDescription,inputTransform,inputRowType));

        return transform;
    }

    /** Base translator to create deduplicate operator. */
    private abstract static class DeduplicateOperatorTranslator {
        private final ReadableConfig config;
        protected final InternalTypeInfo<RowData> rowTypeInfo;
        protected final TypeSerializer<RowData> typeSerializer;
        protected final boolean keepLastRow;
        protected final boolean generateUpdateBefore;

        protected DeduplicateOperatorTranslator(
                ReadableConfig config,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                boolean keepLastRow,
                boolean generateUpdateBefore) {
            this.config = config;
            this.rowTypeInfo = rowTypeInfo;
            this.typeSerializer = typeSerializer;
            this.keepLastRow = keepLastRow;
            this.generateUpdateBefore = generateUpdateBefore;
        }

        protected boolean generateInsert() {
            return config.get(TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED);
        }

        protected boolean isMiniBatchEnabled() {
            return config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
        }

        protected boolean isCompactChanges() {
            return config.get(TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED);
        }

        protected long getMinRetentionTime() {
            return config.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
        }

        protected long getMiniBatchSize() {
            if (isMiniBatchEnabled()) {
                long size = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
                checkArgument(
                        size > 0,
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE.key()
                                + " should be greater than 0.");
                return size;
            } else {
                return -1;
            }
        }

        abstract OneInputStreamOperator<RowData, RowData> createDeduplicateOperator();
    }

    /** Translator to create event time deduplicate operator. */
    private static class RowtimeDeduplicateOperatorTranslator
            extends DeduplicateOperatorTranslator {

        private final RowType inputRowType;

        protected RowtimeDeduplicateOperatorTranslator(
                ReadableConfig config,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                RowType inputRowType,
                boolean keepLastRow,
                boolean generateUpdateBefore) {
            super(config, rowTypeInfo, typeSerializer, keepLastRow, generateUpdateBefore);
            this.inputRowType = inputRowType;
        }

        @Override
        OneInputStreamOperator<RowData, RowData> createDeduplicateOperator() {
            int rowtimeIndex = -1;
            for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
                if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                    rowtimeIndex = i;
                    break;
                }
            }
            checkArgument(rowtimeIndex >= 0);
            if (isMiniBatchEnabled()) {
                CountBundleTrigger<RowData> trigger = new CountBundleTrigger<>(getMiniBatchSize());
                MapBundleFunction processFunction;
                if (isCompactChanges()) {
                    processFunction =
                            new RowTimeMiniBatchLatestChangeDeduplicateFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    getMinRetentionTime(),
                                    rowtimeIndex,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    keepLastRow);
                } else {
                    processFunction =
                            new RowTimeMiniBatchDeduplicateFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    getMinRetentionTime(),
                                    rowtimeIndex,
                                    generateUpdateBefore,
                                    generateInsert(),
                                    keepLastRow);
                }
                return new KeyedMapBundleOperator<>(processFunction, trigger);
            } else {
                RowTimeDeduplicateFunction processFunction =
                        new RowTimeDeduplicateFunction(
                                rowTypeInfo,
                                getMinRetentionTime(),
                                rowtimeIndex,
                                generateUpdateBefore,
                                generateInsert(),
                                keepLastRow);
                return new KeyedProcessOperator<>(processFunction);
            }
        }
    }

    /** Translator to create process time deduplicate operator. */
    private static class ProcTimeDeduplicateOperatorTranslator
            extends DeduplicateOperatorTranslator {
        private final GeneratedRecordEqualiser generatedEqualiser;

        protected ProcTimeDeduplicateOperatorTranslator(
                ReadableConfig config,
                ClassLoader classLoader,
                InternalTypeInfo<RowData> rowTypeInfo,
                TypeSerializer<RowData> typeSerializer,
                RowType inputRowType,
                boolean keepLastRow,
                boolean generateUpdateBefore) {
            super(config, rowTypeInfo, typeSerializer, keepLastRow, generateUpdateBefore);
            generatedEqualiser =
                    new EqualiserCodeGenerator(inputRowType, classLoader)
                            .generateRecordEqualiser("DeduplicateRowEqualiser");
        }

        @Override
        OneInputStreamOperator<RowData, RowData> createDeduplicateOperator() {
            if (isMiniBatchEnabled()) {
                CountBundleTrigger<RowData> trigger = new CountBundleTrigger<>(getMiniBatchSize());
                if (keepLastRow) {
                    ProcTimeMiniBatchDeduplicateKeepLastRowFunction processFunction =
                            new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
                                    rowTypeInfo,
                                    typeSerializer,
                                    getMinRetentionTime(),
                                    generateUpdateBefore,
                                    generateInsert(),
                                    true,
                                    generatedEqualiser);
                    return new KeyedMapBundleOperator<>(processFunction, trigger);
                } else {
                    ProcTimeMiniBatchDeduplicateKeepFirstRowFunction processFunction =
                            new ProcTimeMiniBatchDeduplicateKeepFirstRowFunction(
                                    typeSerializer, getMinRetentionTime());
                    return new KeyedMapBundleOperator<>(processFunction, trigger);
                }
            } else {
                if (keepLastRow) {
                    ProcTimeDeduplicateKeepLastRowFunction processFunction =
                            new ProcTimeDeduplicateKeepLastRowFunction(
                                    rowTypeInfo,
                                    getMinRetentionTime(),
                                    generateUpdateBefore,
                                    generateInsert(),
                                    true,
                                    generatedEqualiser);
                    return new KeyedProcessOperator<>(processFunction);
                } else {
                    ProcTimeDeduplicateKeepFirstRowFunction processFunction =
                            new ProcTimeDeduplicateKeepFirstRowFunction(getMinRetentionTime());
                    return new KeyedProcessOperator<>(processFunction);
                }
            }
        }
    }
}
