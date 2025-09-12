package com.huawei.omniruntime.flink.streaming.api.graph;

import com.google.gson.Gson;

import com.huawei.omniruntime.flink.utils.ReflectionUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.client.cli.UdfConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamNodeOptimized implements StreamNodeExtraDescription {

    private static final Logger LOG = LoggerFactory.getLogger(StreamNodeOptimized.class);
    private final static StreamNodeOptimized instance = new StreamNodeOptimized();

    private final static String featureSoName = "udf_so";
    private final static String featureKeyByName = "key_so";
    private final static String featureShuffleName = "hash_so";
    private final static String featureUdfObj = "udf_obj";

    private final static String lambdaRule = "lambda\\$[A-Za-z0-9_]+\\$[0-9a-f]+\\$[0-9]+";

    private static final Set<String> SUPPORT_KAFKA_DESERIALIZATION_SCHEMA_TPYE = new HashSet<>();
    private static final Set<String> SUPPORT_KAFKA_SERIALIZATION_SCHEMA_TPYE = new HashSet<>();

    static {
        SUPPORT_KAFKA_DESERIALIZATION_SCHEMA_TPYE.addAll(Arrays.asList("SimpleStringSchema"));
        SUPPORT_KAFKA_SERIALIZATION_SCHEMA_TPYE.addAll(Arrays.asList("SimpleStringSchema"));
    }

    static StreamNodeOptimized getInstance() {
        return instance;
    }

    @SuppressWarnings("unchecked")
    public boolean setExtraDescription(StreamNode streamNode, StreamConfig streamConfig, StreamGraph streamGraph, JobType jobType) throws NoSuchFieldException, IllegalAccessException, IOException, ClassNotFoundException {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        List<Map<String, Object>> inputTypes = toStringTypeSerializers(streamNode.getTypeSerializersIn());
        Map<String, Object> outputTypes = toStringTypeSerializer(streamNode.getTypeSerializerOut());

        if (streamNode.getOperatorFactory() instanceof SimpleUdfStreamOperatorFactory) {
            StreamOperator<?> operator = streamNode.getOperator();
            if (operator instanceof AbstractUdfStreamOperator) {
                Function udf = ((AbstractUdfStreamOperator<?, ?>) operator).getUserFunction();
                // udf->json
                // Java的transient关键字原本是用于阻止默认的Java序列化机制（比如ObjectOutputStream）序列化字段。Jackson默认不遵循这个关键字，但Gson默认会尊重
                Gson gson = new Gson();
                String udfObj = gson.toJson(udf);
                jsonMap.put(featureUdfObj, udfObj);

                if (!setUdfInfo((AbstractUdfStreamOperator) operator, jsonMap)) {
                    return false;
                }
            }

            // Check whether the downstream operator needs to perform hash shuffle.
            // If yes, extend the SO path of the downstream keyselector.
            Map<Integer, Object> shuffleMap = new LinkedHashMap<>();
            for (StreamEdge edge : streamNode.getOutEdges()) {
                if (edge.getPartitioner().toString().equals("HASH")) {
                    StreamNode nextNode = streamGraph.getStreamNode(edge.getTargetId());
                    setHashSelector(nextNode.getStatePartitioners(), edge.getTargetId(), shuffleMap);
                }
            }
            jsonMap.put(featureShuffleName, shuffleMap);

            if (!setKeySelector(streamNode.getStatePartitioners(), jsonMap)) {
                return false;
            }
        }

        if (streamNode.getOperatorFactory() instanceof SinkWriterOperatorFactory) {
            Sink sink = ((SinkWriterOperatorFactory<?, ?>) streamNode.getOperatorFactory()).getSink();
            if (!"KafkaSink".equals(sink.getClass().getSimpleName()) || !validateSinkAndSetDesc(sink, jsonMap)) {
                return false;
            }
        }

        if (streamNode.getOperatorFactory() instanceof SourceOperatorFactory) {
            if (validateSourceAndSetDesc(streamNode, jsonMap)) {
                return false;
            }
        }

        jsonMap.put("inputTypes", inputTypes);
        jsonMap.put("outputTypes", outputTypes);
        jsonMap.put("index", streamNode.getId());
        jsonMap.put("jobType", jobType.getValue());
        jsonMap.put("originDescription", streamConfig.getDescription());
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            System.out.println("error");  // Handle the exception or log it
            return false;
        }
        streamConfig.setDescription(jsonString);
        return true;
    }

    private boolean validateSourceAndSetDesc(StreamNode streamNode, Map<String, Object> jsonMap) {
        SourceOperatorFactory<?> sourceOperatorFactory = (SourceOperatorFactory<?>) streamNode.getOperatorFactory();
        Source source = ReflectionUtils.retrievePrivateField(sourceOperatorFactory, "source");
        if (!"KafkaSource".equals(source.getClass().getSimpleName())) {
            return true;
        }
        jsonMap.put("batch", false);
        jsonMap.put("format", "kafka");
        Optional<String> deserializationSchema = getDeserializationSchema(source);
        if (!deserializationSchema.isPresent() || !SUPPORT_KAFKA_DESERIALIZATION_SCHEMA_TPYE.contains(deserializationSchema.get())) {
            return true;
        }
        jsonMap.put("deserializationSchema", deserializationSchema.get());

        WatermarkStrategy watermarkStrategy = ReflectionUtils.retrievePrivateField(
            sourceOperatorFactory, "watermarkStrategy");
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
            return true;
        }
        Properties props = ReflectionUtils.retrievePrivateField(source, "props");
        Map<String, String> properties = new HashMap<>();
        properties.put("group.id", "omni-group");
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        jsonMap.put("properties", properties);
        boolean emitProgressiveWatermarks = ReflectionUtils.retrievePrivateField(
            sourceOperatorFactory, "emitProgressiveWatermarks");
        jsonMap.put("emitProgressiveWatermarks", emitProgressiveWatermarks);
        return false;
    }

    private Optional<String> getDeserializationSchema(Source source) {
        if (!source.getClass().getSimpleName().equals("KafkaSource")) {
            return Optional.empty();
        }
        Object kafkaRecordDeserializationSchema = ReflectionUtils.retrievePrivateField(source, "deserializationSchema");
        if (kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaValueOnlyDeserializerWrapper")) {
            return Optional.empty();
        }
        if (kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaValueOnlyDeserializationSchemaWrapper")) {
            return Optional.of(ReflectionUtils.retrievePrivateField(
                kafkaRecordDeserializationSchema, "deserializationSchema").getClass().getSimpleName());
        }
        if (!kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaDeserializationSchemaWrapper")) {
            return Optional.empty();
        }

        Object kafkaDeserializationSchema = ReflectionUtils.retrievePrivateField(kafkaRecordDeserializationSchema, "kafkaDeserializationSchema");
        if (kafkaDeserializationSchema.getClass().getName().equals("org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper")) {
            return Optional.of(ReflectionUtils.retrievePrivateField(
                kafkaDeserializationSchema, "deserializationSchema").getClass().getSimpleName());
        }

        if (!kafkaDeserializationSchema.getClass().getSimpleName().equals("DynamicKafkaDeserializationSchema")) {
            return Optional.empty();
        }
        Object keyDeserialization = ReflectionUtils.retrievePrivateField(kafkaDeserializationSchema, "keyDeserialization");
        if (keyDeserialization != null) {
            return Optional.empty();
        }
        return Optional.of(ReflectionUtils.retrievePrivateField(kafkaDeserializationSchema, "valueDeserialization").getClass().getSimpleName());
    }

    private List<Map<String, Object>> toStringTypeSerializers(TypeSerializer<?>[] TypeSerializers) {
        List<Map<String, Object>> typeList = new ArrayList<>();
        for (TypeSerializer<?> inputType : TypeSerializers) {
            typeList.add(toStringTypeSerializer(inputType));
        }
        return typeList;
    }

    private Map<String, Object> toStringTypeSerializer(TypeSerializer<?> inputType) {
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

    private boolean setUdfInfo(AbstractUdfStreamOperator operator, Map<String, Object> jsonMap) {
        Function udf = operator.getUserFunction();
        String udfName = getLambdaName(udf);
        if (udfName.isEmpty()) {
            return false;
        }
        Properties config = UdfConfig.getINSTANCE().getConfig();
        String udfSo = "";
        for (String stringPropertyName : config.stringPropertyNames()) {
            if (udfName.equals(stringPropertyName)) {
                udfSo = config.getProperty(stringPropertyName);
                break;
            }
        }
        if (udfSo.isEmpty()) {
            return false;
        }

        jsonMap.put(featureSoName, udfSo);
        return true;
    }

    private boolean setHashSelector(KeySelector<?, ?>[] statePartitioners, Integer targetId, Map<Integer, Object> shuffleMap) {
        if (statePartitioners.length > 1) {
            return false;
        }
        ArrayList<String> Keys = new ArrayList<>();
        for (KeySelector<?, ?> statePartitioner : statePartitioners) {
            String name = getLambdaName(statePartitioner);
            if (name.isEmpty()) {
                return false;
            }
            Properties config = UdfConfig.getINSTANCE().getConfig();
            boolean isMatch = false;
            for (String stringPropertyName : config.stringPropertyNames()) {
                if (name.equals(stringPropertyName)) {
                    Keys.add(config.getProperty(stringPropertyName));
                    isMatch = true;
                    break;
                }
            }
            if (!isMatch) {
                return false;
            }
        }
        shuffleMap.put(targetId, Keys.isEmpty() ? "" : Keys.get(0));
        return true;
    }

    private boolean setKeySelector(KeySelector<?, ?>[] statePartitioners, Map<String, Object> jsonMap) {
        if (statePartitioners.length > 1) {
            return false;
        }
        ArrayList<String> Keys = new ArrayList<>();
        for (KeySelector<?, ?> statePartitioner : statePartitioners) {
            String name = getLambdaName(statePartitioner);
            if (name.isEmpty()) {
                return false;
            }
            Properties config = UdfConfig.getINSTANCE().getConfig();
            boolean isMatch = false;
            for (String stringPropertyName : config.stringPropertyNames()) {
                if (name.equals(stringPropertyName)) {
                    Keys.add(config.getProperty(stringPropertyName));
                    isMatch = true;
                    break;
                }
            }
            if (!isMatch) {
                return false;
            }
        }
        jsonMap.put(featureKeyByName, Keys.isEmpty() ? "" : Keys.get(0));
        return true;
    }

    private String getLambdaName(Object object) {
        String name = object.getClass().getName();
        if (name.contains("$Lambda")) {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
                objectStream.writeObject(object);
                String serializableStr = byteStream.toString();
                Pattern pattern = Pattern.compile(lambdaRule);
                Matcher matcher = pattern.matcher(serializableStr);
                String lambdaName = "";
                if (matcher.find()) {
                    lambdaName = matcher.group();
                }
                name = name.split("\\$")[0] + "$" + lambdaName;
            } catch (IOException e) {
                System.out.println("error");
                return "";
            }
        }
        return name;
    }

    private boolean validateSinkAndSetDesc(Sink sink, Map<String, Object> jsonMap) {
        try {
            jsonMap.put("batch", false);
            DeliveryGuarantee deliveryGuarantee = ReflectionUtils.retrievePrivateField(sink, "deliveryGuarantee");
            jsonMap.put("deliveryGuarantee", deliveryGuarantee.name());

            String transactionalIdPrefix = ReflectionUtils.retrievePrivateField(sink, "transactionalIdPrefix");
            jsonMap.put("transactionalIdPrefix", transactionalIdPrefix);

            Properties kafkaProducerConfig = ReflectionUtils.retrievePrivateField(sink, "kafkaProducerConfig");
            Map<String, String> properties = new HashMap<>();
            for (Map.Entry<Object, Object> entry : kafkaProducerConfig.entrySet()) {
                properties.put(entry.getKey().toString(), entry.getValue().toString());
            }
            jsonMap.put("kafkaProducerConfig", properties);
            Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
            if (!"KafkaRecordSerializationSchemaWrapper".equals(recordSerializer.getClass().getSimpleName())) {
                return false;
            }
            Object topicSelector = ReflectionUtils.retrievePrivateField(recordSerializer, "topicSelector");
            String topic = ((java.util.function.Function<?, String>) topicSelector).apply(null);
            jsonMap.put("topic", topic);
            Object valueSerializationSchema = ReflectionUtils.retrievePrivateField(recordSerializer, "valueSerializationSchema");
            return "SimpleStringSchema".equals(valueSerializationSchema.getClass().getSimpleName());
        } catch (Exception e) {
            return false;
        }
    }

    private Optional<String> getSinkTopic(Sink sink) {
        Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
        if (!"KafkaRecordSerializationSchemaWrapper".equals(recordSerializer.getClass().getSimpleName())) {
            return Optional.empty();
        }
        Object topicSelector = ReflectionUtils.retrievePrivateField(recordSerializer, "topicSelector");
        String topic = ((java.util.function.Function<?, String>) topicSelector).apply(null);
        return Optional.of(topic);
    }
}
