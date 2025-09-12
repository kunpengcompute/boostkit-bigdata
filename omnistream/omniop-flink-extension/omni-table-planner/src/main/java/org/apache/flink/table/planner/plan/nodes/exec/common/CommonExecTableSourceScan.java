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

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.TransformationScanProvider;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.ReflectionUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Base {@link ExecNode} to read data from an external source defined by a {@link ScanTableSource}.
 *
 * @since 2025/05/17
 */
public abstract class CommonExecTableSourceScan extends ExecNodeBase<RowData>
        implements MultipleTransformationTranslator<RowData> {
    public static final String SOURCE_TRANSFORMATION = "source";

    public static final String FIELD_NAME_SCAN_TABLE_SOURCE = "scanTableSource";

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecTableSourceScan.class);

    @JsonProperty(FIELD_NAME_SCAN_TABLE_SOURCE)
    private final DynamicTableSourceSpec tableSourceSpec;

    protected CommonExecTableSourceScan(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            DynamicTableSourceSpec tableSourceSpec,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.tableSourceSpec = tableSourceSpec;
    }

    @Override
    public String getSimplifiedName() {
        return tableSourceSpec.getContextResolvedTable().getIdentifier().getObjectName();
    }

    public DynamicTableSourceSpec getTableSourceSpec() {
        return tableSourceSpec;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final StreamExecutionEnvironment env = planner.getExecEnv();
        final TransformationMetadata meta = createTransformationMeta(SOURCE_TRANSFORMATION, config);
        final InternalTypeInfo<RowData> outputTypeInfo =
                InternalTypeInfo.of((RowType) getOutputType());
        final ScanTableSource tableSource =
                tableSourceSpec.getScanTableSource(
                        planner.getFlinkContext(), ShortcutUtils.unwrapTypeFactory(planner));
        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        if (provider instanceof SourceFunctionProvider) {
            final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
            final SourceFunction<RowData> function = sourceFunctionProvider.createSourceFunction();
            final Transformation<RowData> transformation =
                    createSourceFunctionTransformation(
                            env,
                            function,
                            sourceFunctionProvider.isBounded(),
                            meta.getName(),
                            outputTypeInfo);
            return meta.fill(transformation);
        } else if (provider instanceof InputFormatProvider) {
            final InputFormat<RowData, ?> inputFormat =
                    ((InputFormatProvider) provider).createInputFormat();
            final Transformation<RowData> transformation =
                    createInputFormatTransformation(
                            env, inputFormat, outputTypeInfo, meta.getName());
            return meta.fill(transformation);
        } else if (provider instanceof SourceProvider) {
            final Source<RowData, ?, ?> source = ((SourceProvider) provider).createSource();
            // TODO: Push down watermark strategy to source scan
            final Transformation<RowData> transformation =
                    env.fromSource(
                                    source,
                                    WatermarkStrategy.noWatermarks(),
                                    meta.getName(),
                                    outputTypeInfo)
                            .getTransformation();
            return meta.fill(transformation);
        } else if (provider instanceof DataStreamScanProvider) {
            Transformation<RowData> transformation =
                    ((DataStreamScanProvider) provider)
                            .produceDataStream(createProviderContext(config), env)
                            .getTransformation();
            meta.fill(transformation);
            transformation.setOutputType(outputTypeInfo);
            transformation.setDescription(getExtraDescription(transformation));
            return transformation;
        } else if (provider instanceof TransformationScanProvider) {
            final Transformation<RowData> transformation =
                    ((TransformationScanProvider) provider)
                            .createTransformation(createProviderContext(config));
            meta.fill(transformation);
            transformation.setOutputType(outputTypeInfo);
            return transformation;
        } else {
            throw new UnsupportedOperationException(
                    provider.getClass().getSimpleName() + " is unsupported now.");
        }
    }

    private ProviderContext createProviderContext(ExecNodeConfig config) {
        return name -> {
            if (this instanceof StreamExecNode && config.shouldSetUid()) {
                return Optional.of(createTransformationUid(name, config));
            }
            return Optional.empty();
        };
    }

    /**
     * Adopted from {@link StreamExecutionEnvironment#addSource(SourceFunction, String,
     * TypeInformation)} but with custom {@link Boundedness}.
     */
    protected Transformation<RowData> createSourceFunctionTransformation(
            StreamExecutionEnvironment env,
            SourceFunction<RowData> function,
            boolean isBounded,
            String operatorName,
            TypeInformation<RowData> outputTypeInfo) {
        env.clean(function);

        final int parallelism;
        if (function instanceof ParallelSourceFunction) {
            parallelism = env.getParallelism();
        } else {
            parallelism = 1;
        }

        final Boundedness boundedness;
        if (isBounded) {
            boundedness = Boundedness.BOUNDED;
        } else {
            boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        }

        final StreamSource<RowData, ?> sourceOperator = new StreamSource<>(function, !isBounded);
        return new LegacySourceTransformation<>(
                operatorName, sourceOperator, outputTypeInfo, parallelism, boundedness);
    }

    /**
     * Creates a {@link Transformation} based on the given {@link InputFormat}. The implementation
     * is different for streaming mode and batch mode.
     */
    protected abstract Transformation<RowData> createInputFormatTransformation(
            StreamExecutionEnvironment env,
            InputFormat<RowData, ?> inputFormat,
            InternalTypeInfo<RowData> outputTypeInfo,
            String operatorName);

    private String getExtraDescription(Transformation<RowData> transformation) {
        if (!(transformation instanceof SourceTransformation)) {
            return transformation.getDescription();
        }
        Source source = ((SourceTransformation) transformation).getSource();
        if (!"KafkaSource".equals(source.getClass().getSimpleName())) {
            return transformation.getDescription();
        }

        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        jsonMap.put("originDescription", transformation.getDescription());

        setSourceInfo((SourceTransformation<?, ?, ?>) transformation, jsonMap, source);

        List<String> outputNameList = new ArrayList<>();
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
                    outputNameList.add(rowField.getName());
                }
            } else {
                outputTypeList.add(DescriptionUtil.getFieldType(fieldType));
                outputNameList.add(field.getName());
            }
        }
        jsonMap.put("outputNames", outputNameList);
        jsonMap.put("outputTypes", outputTypeList);
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);
        }
        return jsonString;
    }

    private void setSourceInfo(SourceTransformation<?, ?, ?> transformation,
        Map<String, Object> jsonMap, Source source) {
        jsonMap.put("format", "kafka");
        String deserializationSchema = getDeserializationSchema(source);
        jsonMap.put("deserializationSchema", deserializationSchema);
        boolean hasMetadata = true;
        if (!deserializationSchema.isEmpty()) {
            Object kafkaRecordDeserializationSchema = ReflectionUtils
                .retrievePrivateField(source, "deserializationSchema");
            Object kafkaDeserializationSchema = ReflectionUtils.retrievePrivateField(
                kafkaRecordDeserializationSchema, "kafkaDeserializationSchema");
            hasMetadata = ReflectionUtils.retrievePrivateField(kafkaDeserializationSchema, "hasMetadata");
        }
        jsonMap.put("hasMetadata", hasMetadata);
        Properties props = ReflectionUtils.retrievePrivateField(source, "props");
        Map<String, String> properties = new HashMap<>();
        properties.put("group.id", "omni-group");
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue().toString());
        }

        jsonMap.put("properties", properties);
        WatermarkStrategy<?> watermarkStrategy =
            transformation.getWatermarkStrategy();
        WatermarkGenerator watermarkGenerator = watermarkStrategy.createWatermarkGenerator(null);
        if (watermarkGenerator instanceof NoWatermarksGenerator) {
            jsonMap.put("watermarkStrategy", "no");
        } else if (watermarkGenerator instanceof AscendingTimestampsWatermarks) {
            jsonMap.put("watermarkStrategy", "ascending");
        } else if (watermarkGenerator instanceof BoundedOutOfOrdernessWatermarks) {
            jsonMap.put("watermarkStrategy", "bounded");
            long outOfOrdernessMillis = ReflectionUtils.retrievePrivateField(
                watermarkGenerator, "outOfOrdernessMillis");
            jsonMap.put("outOfOrdernessMillis", outOfOrdernessMillis);
        } else {
            LOG.warn("Unsupported watermark strategy: "
                + watermarkGenerator.getClass().getName());
            jsonMap.put("watermarkStrategy", "");
        }
    }

    private String getDeserializationSchema(Source source) {
        String emptyName = "";
        if (!source.getClass().getSimpleName().equals("KafkaSource")) {
            return emptyName;
        }
        Object kafkaRecordDeserializationSchema = ReflectionUtils.retrievePrivateField(source, "deserializationSchema");
        if (kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaValueOnlyDeserializerWrapper")) {
            return emptyName;
        }
        if (kafkaRecordDeserializationSchema.getClass().getSimpleName()
            .equals("KafkaValueOnlyDeserializationSchemaWrapper")) {
            return ReflectionUtils.retrievePrivateField(kafkaRecordDeserializationSchema, "deserializationSchema")
                .getClass().getSimpleName();
        }
        if (!kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaDeserializationSchemaWrapper")) {
            return emptyName;
        }

        Object kafkaDeserializationSchema = ReflectionUtils.retrievePrivateField(
            kafkaRecordDeserializationSchema, "kafkaDeserializationSchema");
        if (kafkaDeserializationSchema.getClass().getName().equals(
            "org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper")) {
            return ReflectionUtils.retrievePrivateField(kafkaDeserializationSchema, "deserializationSchema")
                .getClass().getSimpleName();
        }

        if (!kafkaDeserializationSchema.getClass().getSimpleName().equals("DynamicKafkaDeserializationSchema")) {
            return emptyName;
        }
        Object keyDeserialization = ReflectionUtils.retrievePrivateField(
            kafkaDeserializationSchema, "keyDeserialization");
        if (keyDeserialization != null) {
            return emptyName;
        }
        return ReflectionUtils.retrievePrivateField(
            kafkaDeserializationSchema, "valueDeserialization").getClass().getSimpleName();
    }
}
