/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan.CSVFormatPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan.JoinSourceFormatPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan.KafkaFormatPojo;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan.NexmarkFormatPOJO;
import com.huawei.omniruntime.flink.table.types.logical.LogicalTypeDescriptor;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.table.RowDataPartitionComputer;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.apache.flink.configuration.Configuration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * OperatorDescriptorHelper
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class OperatorDescriptorHelper {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorDescriptorHelper.class);

    /**
     * createOperatorDescriptor
     *
     * @param operatorConfig operatorConfig
     * @param userCodeClassloader userCodeClassloader
     * @return OperatorPOJO
     */
    public static OperatorPOJO createOperatorDescriptor(StreamConfig operatorConfig, ClassLoader userCodeClassloader) {
        return createNonChainedOperatorDescriptor(operatorConfig.getStreamOperatorFactory(userCodeClassloader), userCodeClassloader, operatorConfig);
    }

    private static OperatorPOJO createNonChainedOperatorDescriptor(
            StreamOperatorFactory operatorFactory,
            ClassLoader userCodeClassloader,
            StreamConfig operatorConfig) {
        OperatorPOJO opDesc = new OperatorPOJO();
        LOG.info("operatorFactory class name {} and getStreamOperatorClass {}",
                operatorFactory.getClass().getCanonicalName(),
                operatorFactory.getStreamOperatorClass(userCodeClassloader).getCanonicalName());
        //StreamOperator op =  operatorConfig.getStreamOperator(userCodeClassloader);
        fillBasicOperatorDescriptions(opDesc, operatorFactory, operatorConfig, userCodeClassloader);
        if (operatorFactory instanceof org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory) {
            LOG.info("operatorFactory is {}", operatorFactory.getClass().getCanonicalName());
        } else if (operatorFactory instanceof org.apache.flink.streaming.api.operators.SourceOperatorFactory) {
            handleSimpleSourceOperatorFactory(operatorFactory, userCodeClassloader, operatorConfig, opDesc);
        } else if (operatorFactory instanceof org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory
                && !(operatorFactory instanceof org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory)) {
            StreamOperator op = operatorConfig.getStreamOperator(userCodeClassloader);
            if (!(op instanceof StreamSource)) {
                return opDesc;
            }
            handleSimpleUdfStreamOperatorFactory((StreamSource) op, opDesc);
        } else if (operatorFactory instanceof SinkWriterOperatorFactory) {
            LOG.info("operatorFactory is {}", operatorFactory.getClass().getCanonicalName());
            handleSinkWriter(userCodeClassloader, operatorConfig, opDesc);
        } else if (operatorFactory instanceof CommitterOperatorFactory) {
            if (!opDesc.getDescription().contains("batch")) {
                Map<String, Object> jsonMap = new LinkedHashMap<>();
                String jsonString = "{\"batch\":true}";
                opDesc.setDescription(jsonString);
            }
        }
        return opDesc;
    }

    private static void handleSinkWriter(ClassLoader userCodeClassloader, StreamConfig operatorConfig,
        OperatorPOJO opDesc) {
        if (opDesc.getDescription().contains("batch")) {
            return;
        }
        SinkWriterOperatorFactory sinkOperatorFactory = operatorConfig.getStreamOperatorFactory(userCodeClassloader);
        Sink sink = ReflectionUtils.retrievePrivateField(sinkOperatorFactory, "sink");
        if (!"KafkaSink".equals(sink.getClass().getSimpleName())) {
            return;
        }
        LOG.info("parse kafka sink");
        String jsonString = "";
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("batch", true);
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();

        DeliveryGuarantee deliveryGuarantee = ReflectionUtils.retrievePrivateField(sink, "deliveryGuarantee");
        jsonMap.put("deliveryGuarantee", deliveryGuarantee.name());

        String transactionalIdPrefix = ReflectionUtils.retrievePrivateField(sink, "transactionalIdPrefix");
        jsonMap.put("transactionalIdPrefix", transactionalIdPrefix);

        Properties kafkaProducerConfig = ReflectionUtils.retrievePrivateField(sink, "kafkaProducerConfig");
        Map<String, String> kafkaProducerConfigMap = new HashMap<>();
        for (String key : kafkaProducerConfig.stringPropertyNames()) {
            String value = kafkaProducerConfig.getProperty(key);
            kafkaProducerConfigMap.put(key, value);
        }
        jsonMap.put("kafkaProducerConfig", kafkaProducerConfigMap);

        Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
        String topic = ReflectionUtils.retrievePrivateField(recordSerializer, "topic");
        jsonMap.put("topic", topic);

        Object valueSerializationSchema = ReflectionUtils.retrievePrivateField(recordSerializer, "valueSerialization");
        if (!"JsonRowDataSerializationSchema".equals(valueSerializationSchema.getClass().getSimpleName())) {
            return;
        }
        RowType rowType = ReflectionUtils.retrievePrivateField(valueSerializationSchema, "rowType");
        List<RowType.RowField> inputFields = rowType.getFields();
        List<String> inputTypeList = new ArrayList<>();
        List<String> inputFieldList = new ArrayList<>();
        for (RowType.RowField field : inputFields) {
            LogicalType fieldType = field.getType();
            if (fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // save the offset and subfield cnt and expand row
                for (RowType.RowField rowField : subFields) {
                    inputTypeList.add(getFieldType(rowField.getType()));
                    inputFieldList.add(rowField.getName());
                }
            } else {
                inputTypeList.add(getFieldType(fieldType));
                inputFieldList.add(field.getName());
            }
        }
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("inputFields", inputFieldList);
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.error("writeValueAsString error", e);
        }
        opDesc.setDescription(jsonString);
    }

    public static String getFieldType(LogicalType fieldType){

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
        if (typeRoot == LogicalTypeRoot.DECIMAL) {
            DecimalType decimalType = (DecimalType) fieldType;
            Integer precision = decimalType.getPrecision();
            Integer scale = decimalType.getScale();
            if (precision > 19) {
                typeName += "128";
            } else {
                typeName += "64";
            }
            typeName += "("+precision.toString() + "," + scale.toString() + ")";
        }
        return typeName;
    }

    private static List<Map<String, Object>> toStringTypeSerializers(TypeSerializer<?>[] TypeSerializers) {
        List<Map<String, Object>> typeList = new ArrayList<>();
        for (TypeSerializer<?> inputType : TypeSerializers) {
            typeList.add(toStringTypeSerializer(inputType));
        }
        return typeList;
    }

    private static Map<String, Object> toStringTypeSerializer(TypeSerializer<?> inputType) {
        Map<String, Object> inputTypeJson = new LinkedHashMap<>();
        if (inputType == null) {
            return inputTypeJson;
        }
        String typeName = inputType.getClass().getName();
        inputTypeJson.put("typeName", typeName);
        List<Map<String, Object>> filed = null;
        if (inputType instanceof TupleSerializer) {
            filed = toStringTypeSerializers(((TupleSerializer) inputType).getFieldSerializers());
        }
        inputTypeJson.put("filed", filed);
        return inputTypeJson;
    }

    private static void handleSimpleSourceOperatorFactory(StreamOperatorFactory operatorFactory,
                                                          ClassLoader userCodeClassloader,
                                                          StreamConfig operatorConfig,
                                                          OperatorPOJO opDesc) {
        LOG.info("operatorFactory is {}", operatorFactory.getClass().getCanonicalName());
        SourceOperatorFactory<?> sourceOperatorFactory = operatorConfig.getStreamOperatorFactory(userCodeClassloader);
        Source source = ReflectionUtils.retrievePrivateField(sourceOperatorFactory, "source");
        String sourceName = source.getClass().getSimpleName();
        if (sourceName.equals("NexmarkSource")) {
            LOG.info("parse nexmarkSourceFunction Plan for nexmark 0.3");
            NexmarkFormatPOJO nexmarkFormatPOJO = new NexmarkFormatPOJO();
            nexmarkFormatPOJO.setFormat("nexmark");
            nexmarkFormatPOJO.setBatchSize(1000);
            Map<String, Object> configMap = nexmarkFormatPOJO.getConfigMap();
            try {
                getNexmarkConfig(source, configMap);
            } catch (Exception exception) {
                LOG.warn("reflection NexmarkSourceFunction failed!", exception);
            }
            String description = JsonHelper.toJson(nexmarkFormatPOJO);
            opDesc.setDescription(description);
        } else if (sourceName.equals("KafkaSource")) {
            if (opDesc.getDescription().contains("batch")) {
                return;
            }
            LOG.info("parse kafka source");
            KafkaFormatPojo kafkaFormatPojo = JsonHelper.fromJson(opDesc.getDescription(), KafkaFormatPojo.class);
            kafkaFormatPojo.setBatch(true);
            boolean emitProgressiveWatermarks = ReflectionUtils.retrievePrivateField(
                sourceOperatorFactory, "emitProgressiveWatermarks");
            kafkaFormatPojo.setEmitProgressiveWatermarks(emitProgressiveWatermarks);
            String description = JsonHelper.toJson(kafkaFormatPojo);
            opDesc.setDescription(description);
        } else {
            handleCsvReaderFormat(sourceOperatorFactory, opDesc);
        }
    }

    private static void handleCsvReaderFormat(SourceOperatorFactory<?> sourceOperatorFactory, OperatorPOJO opDesc) {
        FileSource fileSource = ReflectionUtils.retrievePrivateField(sourceOperatorFactory, "source");
        StreamFormatAdapter streamFormatAdapter = ReflectionUtils.retrievePrivateField(fileSource, "readerFormat");
        StreamFormat streamFormat = ReflectionUtils.retrievePrivateField(streamFormatAdapter, "streamFormat");
        if (streamFormat instanceof CsvReaderFormat) {
            org.apache.flink.core.fs.Path[] inputPaths = ReflectionUtils.retrievePrivateField(fileSource, "inputPaths");
            InternalTypeInfo typeInformation = (InternalTypeInfo) fileSource.getProducedType();
            RowType rowType = typeInformation.toRowType();
            List<RowType.RowField> rowFields = rowType.getFields();
            CSVFormatPOJO csvFormatPOJO = new CSVFormatPOJO();
            SerializableFunction schemaGenerator = ReflectionUtils.retrievePrivateField(streamFormat, "schemaGenerator");
            SerializableSupplier mapperFactory = ReflectionUtils.retrievePrivateField(streamFormat, "mapperFactory");
            CsvSchema csvSchema = (CsvSchema) schemaGenerator.apply(mapperFactory.get());
            CsvSchema.Column[] columns = ReflectionUtils.retrievePrivateField(csvSchema, "_columns");
            Map<String, CsvSchema.Column> columnsByName = ReflectionUtils.retrievePrivateField(csvSchema, "_columnsByName");
            int[] csvSelectFieldToProjectFieldMapping = new int[rowFields.size()];
            int[] csvSelectFieldToCsvFieldMapping = new int[rowFields.size()];
            List<TypeDescriptionPOJO> fields = new ArrayList<>(columns.length);
            for (CsvSchema.Column column : columns) {
                fields.add(new TypeDescriptionPOJO("logical", true, 3,
                        column.getType().toString(), 0, column.getName()));
            }
            for (int i = 0; i < rowFields.size(); i++) {
                int index = columnsByName.get(rowFields.get(i).getName()).getIndex();
                csvSelectFieldToCsvFieldMapping[i] = index;
                fields.get(index).setIsNull(rowFields.get(i).getType().isNullable());
                fields.get(index).setType(rowFields.get(i).getType().toString());
                csvSelectFieldToProjectFieldMapping[i] = i;
            }
            csvFormatPOJO.setFields(fields);
            csvFormatPOJO.setFormat("csv");
            csvFormatPOJO.setFilePath(inputPaths[0].toString());
            csvFormatPOJO.setSelectFields(csvSelectFieldToProjectFieldMapping);
            csvFormatPOJO.setCsvSelectFieldToProjectFieldMapping(csvSelectFieldToProjectFieldMapping);
            csvFormatPOJO.setCsvSelectFieldToCsvFieldMapping(csvSelectFieldToCsvFieldMapping);
            String description = JsonHelper.toJson(csvFormatPOJO);
            LOG.info("csvInputFormat original json string {}", description);
            opDesc.setDescription(description);
        }
    }

    private static void handleSimpleUdfStreamOperatorFactory(StreamSource opSource,
                                                             OperatorPOJO opDesc) {
        Function userFunction = opSource.getUserFunction();
        String functionName = userFunction == null ? "NexmarkSourceFunction" : userFunction.getClass().getSimpleName();
        if (functionName.equals("NexmarkSourceFunction")) {
            LOG.info("parse nexmarkSourceFunction Plan for nexmark 0.2");
            NexmarkFormatPOJO nexmarkFormatPOJO = new NexmarkFormatPOJO();
            nexmarkFormatPOJO.setFormat("nexmark");
            nexmarkFormatPOJO.setBatchSize(1000);
            Map<String, Object> configMap = nexmarkFormatPOJO.getConfigMap();
            try {
                getNexmarkConfigForVersion2(userFunction, configMap);
            } catch (Exception exception) {
                LOG.warn("reflection NexmarkSourceFunction failed!", exception);
            }
            String description = JsonHelper.toJson(nexmarkFormatPOJO);
            opDesc.setDescription(description);
        } else if (functionName.equals("JoinSource")) {
            LOG.info("parse JoinSource Plan for MT");
            JoinSourceFormatPOJO joinSourceFormatPOJO = new JoinSourceFormatPOJO();
            joinSourceFormatPOJO.setFormat("joinSource");
            Map<String, Object> configMap = joinSourceFormatPOJO.getConfigMap();
            try {
                getJoinSourceConfig(userFunction, configMap);
            } catch (IllegalAccessException exception) {
                LOG.warn("reflection JoinSource failed!", exception);
            }
            String description = JsonHelper.toJson(joinSourceFormatPOJO);
            opDesc.setDescription(description);
        } else {
            LOG.info("handle user defined function: " + functionName);
        }
    }

    private static void getNexmarkConfig(Source source, Map<String, Object> configMap) throws IllegalAccessException {
        Class<?> nexmarkFunction = source.getClass();
        Field[] fields = nexmarkFunction.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            // first Reflection to get config
            if (!name.equals("config")) {
                continue;
            }
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            Object value = field.get(source);

            Class<?> config = value.getClass();
            Field[] configFields = config.getDeclaredFields();
            for (Field configField : configFields) {
                String configFieldName = configField.getName();
                if (configFieldName.equals("baseTime")
                        || configFieldName.equals("firstEventId")
                        || configFieldName.equals("maxEvents")
                        || configFieldName.equals("firstEventNumber")) {
                    configMap.put(configFieldName, configField.get(value));
                }
                // second Reflection to get configuration
                if (!configFieldName.equals("configuration")) {
                    continue;
                }
                if (!configField.isAccessible()) {
                    configField.setAccessible(true);
                }
                Object nexmarkConfigValue = configField.get(value);
                // third Reflection to get nexmarkConfig
                Class<?> nexmarkConfig = nexmarkConfigValue.getClass();
                Field[] nexmarkConfigFields = nexmarkConfig.getDeclaredFields();
                for (Field nexmarkConfigField : nexmarkConfigFields) {
                    String nexmarkConfigFieldName = nexmarkConfigField.getName();
                    if (nexmarkConfigFieldName.equals("rateShape") || nexmarkConfigFieldName.equals("rateUnit")) {
                        continue;
                    }
                    if (!configField.isAccessible()) {
                        configField.setAccessible(true);
                    }
                    configMap.put(nexmarkConfigFieldName, nexmarkConfigField.get(nexmarkConfigValue));
                }
            }
            break;
        }
    }

    private static void getJoinSourceConfig(Function userFunction,
                                            Map<String, Object> configMap) throws IllegalAccessException {
        Class<?> joinSourceFunction = userFunction.getClass();
        Field[] fields = joinSourceFunction.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            if (field.getType() != int.class) {
                continue;
            }
            Object value = field.get(userFunction);
            configMap.put(name, value);
        }
    }

    // parse config for nexmark 0.2, for test, will delete later
    private static void getNexmarkConfigForVersion2(Function userFunction, Map<String, Object> configMap) throws IllegalAccessException {
        Class<?> nexmarkFunction = userFunction.getClass();
        Field[] fields = nexmarkFunction.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            // first Reflection to get config
            if (!name.equals("config")) {
                continue;
            }
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            Object value = field.get(userFunction);

            Class<?> config = value.getClass();
            Field[] configFields = config.getDeclaredFields();
            for (Field configField : configFields) {
                String configFieldName = configField.getName();
                if (configFieldName.equals("baseTime")
                        || configFieldName.equals("firstEventId")
                        || configFieldName.equals("maxEvents")
                        || configFieldName.equals("firstEventNumber")) {
                    configMap.put(configFieldName, configField.get(value));
                }
                // second Reflection to get configuration
                if (!configFieldName.equals("configuration")) {
                    continue;
                }
                if (!configField.isAccessible()) {
                    configField.setAccessible(true);
                }
                Object nexmarkConfigValue = configField.get(value);
                // third Reflection to get nexmarkConfig
                Class<?> nexmarkConfig = nexmarkConfigValue.getClass();
                Field[] nexmarkConfigFields = nexmarkConfig.getDeclaredFields();
                for (Field nexmarkConfigField : nexmarkConfigFields) {
                    String nexmarkConfigFieldName = nexmarkConfigField.getName();
                    if (nexmarkConfigFieldName.equals("rateShape") || nexmarkConfigFieldName.equals("rateUnit")) {
                        continue;
                    }
                    if (!configField.isAccessible()) {
                        configField.setAccessible(true);
                    }
                    configMap.put(nexmarkConfigFieldName, nexmarkConfigField.get(nexmarkConfigValue));
                }
            }
            break;
        }
    }

    private static void fillBasicOperatorDescriptions(OperatorPOJO opDesc, StreamOperatorFactory operatorFactory, StreamConfig opConfig, ClassLoader userCodeClassloader) {
        opDesc.setName(opConfig.getOperatorName());
        String id = operatorFactory.getStreamOperatorClass(userCodeClassloader).getCanonicalName().split("\\$")[0];
        opDesc.setId(id);
        Integer vertexID = opConfig.getVertexID();
        opDesc.setVertexID(vertexID);
        opDesc.setOperatorId(opConfig.getOperatorID().toString());
        LOG.info("handling input for op {}", opConfig.getOperatorName());
        LOG.info("handling description for op {}", opConfig.getDescription());
        { // input
            StreamConfig.InputConfig[] inputs = opConfig.getInputs(userCodeClassloader);
            List<TypeDescriptionPOJO> inputsData = new ArrayList<>();

            for (StreamConfig.InputConfig i : inputs) {
                // warning: The InputConversionOperator do not extend the inputType info.
                // As a result, the corresponding serializer cannot be created in the Flink datastream scenario.
                if (id.equals("org.apache.flink.table.runtime.operators.source.InputConversionOperator")) {
                    break;
                }
                String[] typeDesc = parseTypeSerializer(((StreamConfig.NetworkInputConfig) i).getTypeSerializer());
                TypeDescriptionPOJO inputType = new TypeDescriptionPOJO();
                inputType.setKind(typeDesc[0]);
                inputType.setType(typeDesc[1]);
                inputsData.add(inputType);
                opDesc.setInputs(inputsData);
            }
        }

        LOG.info("handling description and output for op {}", opConfig.getOperatorName());
        { //  description and output
            if (Objects.equals(id, "org.apache.flink.connector.file.table.stream.StreamingFileWriter")) {
                try {
                    JSONObject description = new JSONObject();
                    StreamOperator<?> operator = opConfig.getStreamOperator(userCodeClassloader);
                    Configuration conf = (Configuration) ReflectionUtils.retrievePrivateField(operator, "conf");
                    description.put("path", conf.getString("path", ""));
                    JSONObject partitionCommit = new JSONObject();
                    partitionCommit.put("trigger", conf.getString("sink.partition-commit.trigger", ""));
                    partitionCommit.put("delay", conf.getString("sink.partition-commit.delay", ""));
                    JSONObject policy = new JSONObject();
                    policy.put("kind", conf.getString("sink.partition-commit.policy.kind", ""));
                    partitionCommit.put("policy", policy);
                    description.put("partition-commit", partitionCommit);
                    JSONObject rollingPolicy = new JSONObject();
                    rollingPolicy.put("rollover-interval", conf.getString("sink.rolling-policy.rollover-interval", ""));
                    rollingPolicy.put("check-interval", conf.getString("sink.rolling-policy.check-interval", ""));
                    rollingPolicy.put("file-size", conf.getString("sink.rolling-policy.file-size", ""));
                    description.put("rolling-policy", rollingPolicy);
                    JSONObject timeExtractor = new JSONObject();
                    timeExtractor.put("timestamp-pattern", conf.getString("partition.time-extractor.timestamp-pattern", ""));
                    JSONObject partitionMap = new JSONObject();
                    partitionMap.put("time-extractor", timeExtractor);
                    description.put("partition", partitionMap);
                    List<String> partitionKeys = (List<String>) ReflectionUtils.retrievePrivateField(operator, "partitionKeys");
                    description.put("partitionKeys", new JSONArray(partitionKeys));

                    Object bucketsBuilder = ReflectionUtils.retrievePrivateField(operator, "bucketsBuilder");
                    Object assigner = ReflectionUtils.retrievePrivateField(bucketsBuilder, "bucketAssigner");
                    RowDataPartitionComputer computer = ReflectionUtils.retrievePrivateField(assigner, "computer");
                    int[] partitionFields = (int[]) ReflectionUtils.retrievePrivateField(computer, "partitionIndexes");
                    if (partitionFields != null) {
                        JSONArray partitionIndexes = new JSONArray();
                        for (int i : partitionFields) {
                            partitionIndexes.put(i);
                        }
                        description.put("partitionIndexes", partitionIndexes);
                    }
                    int[] nonPartitionFields = (int[]) ReflectionUtils.retrievePrivateField(computer, "nonPartitionIndexes");
                    if (nonPartitionFields != null) {
                        JSONArray nonPartitionIndexes = new JSONArray();
                        for (int i : nonPartitionFields) {
                            nonPartitionIndexes.put(i);
                        }
                        description.put("nonPartitionIndexes", nonPartitionIndexes);
                    }
                    JSONArray inputTypes = new JSONArray();
                    for (StreamConfig.InputConfig in : opConfig.getInputs(userCodeClassloader)) {
                        String[] parsed = parseTypeSerializer(((StreamConfig.NetworkInputConfig) in).getTypeSerializer());
                        if ("Row".equals(parsed[0])) {
                            new JSONArray(parsed[1]).forEach(fld -> inputTypes.put(((JSONObject)fld).getString("type")));
                        } else {
                            inputTypes.put(parsed[1]);
                        }
                    }
                    description.put("inputTypes", inputTypes);
                    opDesc.setDescription(description.toString());
                } catch (Exception e) {
                    LOG.warn("Failed to create StreamingFileWriter description", e);
                }

                TypeDescriptionPOJO outputType = new TypeDescriptionPOJO();
                outputType.setKind("Row");
                outputType.setType(new JSONArray().toString());
                opDesc.setOutput(outputType);
            } else if (!Objects.equals(id, "org.apache.flink.table.runtime.operators.sink.SinkOperator")
                    && !Objects.equals(id, "org.apache.flink.streaming.api.operators.StreamSource")
                    && !Objects.equals(id, "org.apache.flink.table.runtime.operators.source.InputConversionOperator")) {
                // warning: The preceding three operators do not extend the outputType info.
                // As a result, the corresponding serializer cannot be created in the Flink datastream scenario.
                String description = rewriteToOmniDatatypes(opConfig.getDescription()).toString();
                LOG.info("description is {}", description);
                opDesc.setDescription(description);
                String[] typeDesc = parseTypeSerializer(opConfig.getTypeSerializerOut(userCodeClassloader));
                TypeDescriptionPOJO outputType = new TypeDescriptionPOJO();
                outputType.setKind(typeDesc[0]);
                outputType.setType(typeDesc[1]);
                opDesc.setOutput(outputType);
            } else {
                // Placeholder for empty descriptors and output types
                String description = rewriteToOmniDatatypes(opConfig.getDescription()).toString();
                opDesc.setDescription(description);
                TypeDescriptionPOJO outputType = new TypeDescriptionPOJO();
                outputType.setKind("Row");
                outputType.setType(new JSONArray().toString());
                opDesc.setOutput(outputType);
            }
        }
    }

    private static String[] parseTypeSerializer(TypeSerializer<?> ts) {
        String data[] = new String[2];
        String typeObj = "Void";
        String kindStr = "basic";

        // NOTE(yar): Could be enums, but will require syncing enums between Java and C++
        //   The whole thing could be a protobuf instead too
        if (ts instanceof StringSerializer) {
            typeObj = "String";
        } else if (ts instanceof BooleanSerializer) {
            typeObj = "Boolean";
        } else if (ts instanceof ByteSerializer) {
            typeObj = "Byte";
        } else if (ts instanceof CharSerializer) {
            typeObj = "Character";
        } else if (ts instanceof DoubleSerializer) {
            typeObj = "Double";
        } else if (ts instanceof FloatSerializer) {
            typeObj = "Float";
        } else if (ts instanceof LongSerializer) {
            typeObj = "Long";
        } else if (ts instanceof IntSerializer) {
            typeObj = "Short";
        } else if (ts instanceof ShortSerializer) {
            typeObj = "Integer";
        } else if (ts instanceof TupleSerializer) {
            kindStr = "Tuple";
            typeObj = parseTupleSerializer((TupleSerializer<?>) ts).toString();
        } else if (ts instanceof RowDataSerializer) {
            kindStr = "Row";
            try {
                typeObj = extractRowDataSerializer((RowDataSerializer) ts).toString();
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (ts instanceof PojoSerializer) {
            kindStr = "Pojo";
        } else if (ts instanceof ExternalSerializer) {
            kindStr = "External";
        } else if (ts != null && ts.getClass().getName().contains(
            "org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo")) {
            kindStr = "CommittableMessage";
        } else if (ts != null && !(ts instanceof VoidSerializer)) {
            throw new RuntimeException("Cannot setup OmniOperatorChain because an unsupported serializer type was requested: " + ts.toString());
        }

        data[0] = kindStr; //"kind"
        data[1] = typeObj;// "type"
        return data;
    }

    private static JSONArray parseTupleSerializer(TupleSerializer<?> ts) {
        JSONArray tupleData = new JSONArray();

        for (int i = 0; i < ts.getArity(); i++) {
            tupleData.put(parseTypeSerializer(ts.getFieldSerializers()[i]));
        }

        return tupleData;
    }


    private static JSONArray extractRowDataSerializer(RowDataSerializer rowDataSerializers) throws NoSuchFieldException, IllegalAccessException {
        JSONArray rowFields = new JSONArray();
        Field f = rowDataSerializers.getClass().getDeclaredField("types");
        f.setAccessible(true);
        LogicalType[] logicalTypes = (LogicalType[]) f.get(rowDataSerializers);
        for (LogicalType type : logicalTypes) {
            if (type instanceof BigIntType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorBigIntType((BigIntType) type));
            } else if (type instanceof VarCharType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorVarCharType((VarCharType) type));
            } else if (type instanceof TimestampType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorTimestampType((TimestampType) type));
            } else {

            }

        }
        return rowFields;
    }

    private static JSONObject rewriteToOmniDatatypes(String description) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject = new JSONObject(description);
        } catch (JSONException e) {
            LOG.error("Description is not JSON format {}", description, e);
            //throw new RuntimeException(e);
        }
        return jsonObject;
    }
}
