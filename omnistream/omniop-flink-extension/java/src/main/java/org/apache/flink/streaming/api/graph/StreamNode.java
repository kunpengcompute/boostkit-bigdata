/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.streaming.api.graph.StreamNodeOptimized;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;
import org.codehaus.commons.nullanalysis.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class representing the operators in the streaming programs, with all their properties.
 */
@Internal
public class StreamNode {
    private static final Logger LOG = LoggerFactory.getLogger(StreamNodeOptimized.class);

    private final int id;
    private final Map<ManagedMemoryUseCase, Integer> managedMemoryOperatorScopeUseCaseWeights =
            new HashMap<>();
    private final Set<ManagedMemoryUseCase> managedMemorySlotScopeUseCases = new HashSet<>();
    private final String operatorName;
    private Class<? extends TaskInvokable> jobVertexClass;
    private final Map<Integer, StreamConfig.InputRequirement> inputRequirements = new HashMap<>();
    private int parallelism;

    /**
     * Maximum parallelism for this stream node. The maximum parallelism is the upper limit for
     * dynamic scaling and the number of key groups used for partitioned state.
     */
    private int maxParallelism;
    private ResourceSpec minResources = ResourceSpec.DEFAULT;
    private ResourceSpec preferredResources = ResourceSpec.DEFAULT;
    private long bufferTimeout;
    private String operatorDescription;
    private
    @Nullable
    String slotSharingGroup;
    private
    @Nullable
    String coLocationGroup;
    private KeySelector<?, ?>[] statePartitioners = new KeySelector[0];
    private TypeSerializer<?> stateKeySerializer;
    private StreamOperatorFactory<?> operatorFactory;
    private TypeSerializer<?>[] typeSerializersIn = new TypeSerializer[0];
    private TypeSerializer<?> typeSerializerOut;
    private List<StreamEdge> inEdges = new ArrayList<StreamEdge>();
    private List<StreamEdge> outEdges = new ArrayList<StreamEdge>();
    private InputFormat<?, ?> inputFormat;
    private OutputFormat<?> outputFormat;
    private String transformationUID;
    private String userHash;
    private
    @Nullable
    IntermediateDataSetID consumeClusterDatasetId;

    @VisibleForTesting
    public StreamNode(
            Integer id,
            @Nullable
            String slotSharingGroup,
            @Nullable
            String coLocationGroup,
            StreamOperator<?> operator,
            String operatorName,
            Class<? extends TaskInvokable> jobVertexClass) {
        this(
                id,
                slotSharingGroup,
                coLocationGroup,
                SimpleOperatorFactory.of(operator),
                operatorName,
                jobVertexClass);
    }

    public StreamNode(
            Integer id,
            @Nullable
            String slotSharingGroup,
            @Nullable
            String coLocationGroup,
            StreamOperatorFactory<?> operatorFactory,
            String operatorName,
            Class<? extends TaskInvokable> jobVertexClass) {
        this.id = id;
        this.operatorName = operatorName;
        this.operatorDescription = operatorName;
        this.operatorFactory = operatorFactory;
        this.jobVertexClass = jobVertexClass;
        this.slotSharingGroup = slotSharingGroup;
        this.coLocationGroup = coLocationGroup;

        if (operatorFactory instanceof SimpleUdfStreamOperatorFactory
            && ((SimpleUdfStreamOperatorFactory<?>) operatorFactory).getOperator() instanceof StreamSource
            && ((StreamSource) (((SimpleUdfStreamOperatorFactory<?>) operatorFactory).getOperator()))
            .getUserFunction().getClass().getSimpleName().equals("FlinkKafkaConsumer")) {
            try {
                // TODO : from source commit offset情况下需要group id
                // 2. update the operator factory
                this.operatorFactory = updateSourceOperatorFactory(operatorFactory);
                this.jobVertexClass = SourceOperatorStreamTask.class;
            } catch (IOException | ClassNotFoundException | IllegalStateException e) {
                LOG.warn("Failed to migrate legacy API to new version");
            }
        }

        if (operatorFactory instanceof SimpleUdfStreamOperatorFactory
                && ((SimpleUdfStreamOperatorFactory<?>) operatorFactory).getOperator() instanceof StreamSink
                && ((StreamSink) (((SimpleUdfStreamOperatorFactory<?>) operatorFactory).getOperator()))
                .getUserFunction().getClass().getSimpleName().equals("FlinkKafkaProducer")) {
            try {
                // update the operator factory
                this.operatorFactory = updateSinkOperatorFactory(operatorFactory);
            } catch (NoSuchFieldException | IllegalAccessException | FlinkRuntimeException e) {
                LOG.warn("Failed to migrate legacy API to new version");
            }
        }
    }

    private <T>StreamOperatorFactory<T> updateSourceOperatorFactory(StreamOperatorFactory<T> operatorFactory)
        throws IOException, ClassNotFoundException {
        SimpleUdfStreamOperatorFactory<T> oldOperatorFactory = (SimpleUdfStreamOperatorFactory<T>) operatorFactory;
        Function userFunction = oldOperatorFactory.getUserFunction();
        // 1 create kafka source
        // 1.1 create subscriber
        Object subscriber = getKafkaSubscriber(userFunction);

        // 1.2 create startingOffsetsInitializer
        Properties properties = ReflectionUtils.retrievePrivateField(userFunction, "properties");
        Object startingOffsetsInitializer = getStartingOffsetsInitializer(userFunction, properties);
        if (startingOffsetsInitializer == null) {
            throw new FlinkRuntimeException("startingOffsetsInitializer is absent..");
        }

        // 1.3 create stoppingOffsetsInitializer
        // do nothing

        // 1.4 deserializationSchema
        Object deserializationSchemaWrapper =
            ReflectionUtils.retrievePrivateField(userFunction, "deserializer");

        // 1.5 apply all the attribute into kafka source
        Object kafkaSourceBuilder = ReflectionUtils.constructInstance(
            "org.apache.flink.connector.kafka.source.KafkaSourceBuilder", Collections.emptyList());
        ReflectionUtils.invokeMethod(kafkaSourceBuilder, "setKafkaSubscriber",
            Collections.singletonList(subscriber), subscriber.getClass().getInterfaces());
        ReflectionUtils.invokeMethod(kafkaSourceBuilder, "setStartingOffsets",
            Collections.singletonList(startingOffsetsInitializer), startingOffsetsInitializer.getClass().getInterfaces()[0]);
        ReflectionUtils.invokeMethod(kafkaSourceBuilder, "setProperties",
            Collections.singletonList(properties), properties.getClass());
        Object kafkaRecordDeserializationSchema = ReflectionUtils.invokeStaticMethod(
            "org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema",
            "of", Collections.singletonList(deserializationSchemaWrapper),
            deserializationSchemaWrapper.getClass().getInterfaces());
        ReflectionUtils.invokeMethod(kafkaSourceBuilder, "setDeserializer",
            Collections.singletonList(kafkaRecordDeserializationSchema),
            kafkaRecordDeserializationSchema.getClass().getInterfaces());

        Source source = ReflectionUtils.invokeMethod(kafkaSourceBuilder, "build", Collections.emptyList());

        // 2. watermark strategy
        SerializedValue<WatermarkStrategy<T>> serializedWatermarkStrategy =
            ReflectionUtils.retrievePrivateField(userFunction, "watermarkStrategy");
        WatermarkStrategy<T> watermarkStrategy;
        if (serializedWatermarkStrategy == null) {
            watermarkStrategy = WatermarkStrategy.noWatermarks();
        } else {
            watermarkStrategy = serializedWatermarkStrategy.deserializeValue(userFunction.getClass().getClassLoader());
        }

        // 3. emitProgressiveWatermarks
        StreamSource operator =
            (StreamSource) oldOperatorFactory.getOperator();
        boolean emitsProgressiveWatermarks = operator.emitsProgressiveWatermarks();

        // 4. chaining strategy
        // there exists differences in chaining strategy, old default is "HEAD", new default is "ALWAYS"
        ChainingStrategy chainingStrategy = operator.getChainingStrategy();

        // 5. processingTimeService | mailboxExecutor are marked as transient, ignore here

        SourceOperatorFactory<T> sourceOperatorFactory = new SourceOperatorFactory<>(
            source, watermarkStrategy, emitsProgressiveWatermarks);
        sourceOperatorFactory.setChainingStrategy(chainingStrategy);
        return sourceOperatorFactory;
    }

    private <T, R> SinkWriterOperatorFactory<R, ?> updateSinkOperatorFactory(StreamOperatorFactory<T> operatorFactory) throws NoSuchFieldException, IllegalAccessException {
        SimpleUdfStreamOperatorFactory<T> oldOperatorFactory = (SimpleUdfStreamOperatorFactory<T>) operatorFactory;
        Function producer = oldOperatorFactory.getUserFunction();

        // 1 DeliveryGuarantee
        Object semantic = ReflectionUtils.retrievePrivateField(producer, "semantic");
        DeliveryGuarantee deliveryGuarantee = null;
        switch (semantic.toString()) {
            case "NONE":
                deliveryGuarantee = DeliveryGuarantee.NONE;
                break;
            case "EXACTLY_ONCE":
                deliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE;
                break;
            case "AT_LEAST_ONCE":
                deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
                break;
            default:
                LOG.warn("Cannot migrate DeliveryGuarantee: " + semantic);
        }
        if (deliveryGuarantee == null) {
            throw new FlinkRuntimeException("DeliveryGuarantee is absent..");
        }

        // 2 recordSerializer
        // 2.1 KeySerializationSchema
        Object keySchema = ReflectionUtils.retrievePrivateField(producer, "keyedSchema");

        // 2.2 topic
        String topic = ReflectionUtils.retrievePrivateField(producer, "defaultTopicId");

        // 2.3 value SerializationSchema
        Object kafkaSchema = ReflectionUtils.retrievePrivateField(producer, "kafkaSchema");
        Object serializationSchema = ReflectionUtils.retrievePrivateField(kafkaSchema, "serializationSchema");
        Object flinkKafkaPartitioner = ReflectionUtils.retrievePrivateField(kafkaSchema, "partitioner");
        Object kafkaRecordSerializationSchemaBuilder =
            ReflectionUtils.invokeStaticMethod("org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema",
                "builder", Collections.emptyList());
        ReflectionUtils.invokeMethod(kafkaRecordSerializationSchemaBuilder,
            "setTopic", Collections.singletonList(topic), String.class);
        ReflectionUtils.setPrivateField(kafkaRecordSerializationSchemaBuilder,
            "valueSerializationSchema", serializationSchema);
        ReflectionUtils.setPrivateField(kafkaRecordSerializationSchemaBuilder, "partitioner", flinkKafkaPartitioner);
        Object recordSerializationSchema = ReflectionUtils.invokeMethod(kafkaRecordSerializationSchemaBuilder,
            "build", Collections.emptyList());

        // 3. properties
        Properties producerConfig = ReflectionUtils.retrievePrivateField(producer, "producerConfig");

        // 4. transactionalIdPrefix
        String transactionalIdPrefix = ReflectionUtils.retrievePrivateField(producer, "transactionalIdPrefix");

        // 5. build kafka sink
        Object kafkaSinkBuilder = ReflectionUtils.invokeStaticMethod("org.apache.flink.connector.kafka.sink.KafkaSink",
            "builder", Collections.emptyList());
        ReflectionUtils.invokeMethod(kafkaSinkBuilder, "setDeliveryGuarantee",
            Collections.singletonList(deliveryGuarantee), deliveryGuarantee.getClass());
        ReflectionUtils.invokeMethod(kafkaSinkBuilder, "setRecordSerializer",
            Collections.singletonList(recordSerializationSchema), recordSerializationSchema.getClass().getInterfaces());
        ReflectionUtils.invokeMethod(kafkaSinkBuilder, "setKafkaProducerConfig",
            Collections.singletonList(producerConfig), producerConfig.getClass());
        if (transactionalIdPrefix != null) {
            ReflectionUtils.invokeMethod(kafkaSinkBuilder, "setTransactionalIdPrefix",
                Collections.singletonList(transactionalIdPrefix), transactionalIdPrefix.getClass());
        }
        Sink sink = ReflectionUtils.invokeMethod(kafkaSinkBuilder, "build", Collections.emptyList());
        return new SinkWriterOperatorFactory<>(sink);
    }

    private Object getKafkaSubscriber(Function function) throws ClassNotFoundException {
        Object topicsDescriptor = ReflectionUtils.retrievePrivateField(function, "topicsDescriptor");
        List<String> fixedTopics = ReflectionUtils.retrievePrivateField(topicsDescriptor, "fixedTopics");
        Object subscriber;
        if (fixedTopics != null) {
            subscriber = ReflectionUtils.invokeStaticMethod(
                "org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber",
                "getTopicListSubscriber", Collections.singletonList(fixedTopics), List.class);
        } else {
            Pattern pattern = ReflectionUtils.retrievePrivateField(topicsDescriptor, "topicPattern");
            subscriber = ReflectionUtils.invokeStaticMethod(
                "org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber",
                "getTopicListSubscriber", Collections.singletonList(pattern), Pattern.class);
        }
        return subscriber;
    }

    private Object getStartingOffsetsInitializer(Function consumer, Properties properties) {
        Object startingOffsetsInitializer = null;
        String startupMode = ReflectionUtils.retrievePrivateField(consumer, "startupMode").toString();

        switch (startupMode) {
            case "GROUP_OFFSETS":
                String offsetResetConfig = properties.getProperty("auto.offset.reset", "NONE");
                Object offsetResetStrategy = getResetStrategy(offsetResetConfig);
                startingOffsetsInitializer = ReflectionUtils.invokeStaticMethod(
                    "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer",
                    "committedOffsets", Collections.singletonList(offsetResetStrategy), offsetResetStrategy.getClass());
                break;
            case "EARLIEST":
                startingOffsetsInitializer = ReflectionUtils.invokeStaticMethod(
                    "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer",
                    "earliest", Collections.emptyList());
                break;
            case "LATEST":
                startingOffsetsInitializer = ReflectionUtils.invokeStaticMethod(
                    "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer",
                    "latest", Collections.emptyList());
                break;
            case "TIMESTAMP":
                long startupOffsetsTimestamp = ReflectionUtils.retrievePrivateField(
                    consumer, "startupOffsetsTimestamp");
                startingOffsetsInitializer = ReflectionUtils.invokeStaticMethod(
                    "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer",
                    "timestamp", Collections.singletonList(startupOffsetsTimestamp), long.class);
                break;
            case "SPECIFIC_OFFSETS":
                Map<Object, Long> subscribedPartitionsToStartOffsets =
                    ReflectionUtils.retrievePrivateField(consumer, "subscribedPartitionsToStartOffsets");
                Map<Object, Long> offsets = new HashMap<>();
                subscribedPartitionsToStartOffsets.forEach(
                    (tp, offset) ->
                        offsets.put(createTopicPartition(tp), offset));
                startingOffsetsInitializer = ReflectionUtils.invokeStaticMethod(
                    "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer",
                    "offsets", Collections.singletonList(offsets), Map.class);
                break;
            default:
                LOG.warn("Cannot migrate startingOffsetsInitializer");
        }
        return startingOffsetsInitializer;
    }

    private Object createTopicPartition(Object tp) {
        String topic = ReflectionUtils.retrievePrivateField(tp, "topic");
        int partition = ReflectionUtils.retrievePrivateField(tp, "partition");
        return ReflectionUtils.constructInstance("org.apache.kafka.common.TopicPartition",
            Arrays.asList(topic, partition), String.class, int.class);
    }

    private Object getResetStrategy(String offsetResetConfig) {
        final String offsetResetStrategyClassName = "org.apache.kafka.clients.consumer.OffsetResetStrategy";
        Object[] offsetResetStrategy = ReflectionUtils.invokeStaticMethod(offsetResetStrategyClassName,
            "values", Collections.emptyList());
        return Arrays.stream(offsetResetStrategy)
            .filter(s -> s.toString().equalsIgnoreCase(offsetResetConfig))
            .map(s -> ReflectionUtils.invokeStaticMethod(offsetResetStrategyClassName,
                "valueOf", Collections.singletonList(s), String.class))
            .findAny()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "%s can not be set to %s. Valid values: [%s]",
                            "auto.offset.reset",
                            offsetResetConfig,
                            Arrays.stream(offsetResetStrategy)
                                .map(Object::toString)
                                .map(String::toLowerCase)
                                .collect(Collectors.joining(",")))));

    }

    public void addInEdge(StreamEdge inEdge) {
        checkState(
                inEdges.stream().noneMatch(inEdge::equals),
                "Adding not unique edge = %s to existing inEdges = %s",
                inEdge,
                inEdges);
        if (inEdge.getTargetId() != getId()) {
            throw new IllegalArgumentException("Destination id doesn't match the StreamNode id");
        } else {
            inEdges.add(inEdge);
        }
    }

    public void addOutEdge(StreamEdge outEdge) {
        checkState(
                outEdges.stream().noneMatch(outEdge::equals),
                "Adding not unique edge = %s to existing outEdges = %s",
                outEdge,
                outEdges);
        if (outEdge.getSourceId() != getId()) {
            throw new IllegalArgumentException("Source id doesn't match the StreamNode id");
        } else {
            outEdges.add(outEdge);
        }
    }

    public List<StreamEdge> getOutEdges() {
        return outEdges;
    }

    public List<StreamEdge> getInEdges() {
        return inEdges;
    }

    public List<Integer> getOutEdgeIndices() {
        List<Integer> outEdgeIndices = new ArrayList<Integer>();

        for (StreamEdge edge : outEdges) {
            outEdgeIndices.add(edge.getTargetId());
        }

        return outEdgeIndices;
    }

    public List<Integer> getInEdgeIndices() {
        List<Integer> inEdgeIndices = new ArrayList<Integer>();

        for (StreamEdge edge : inEdges) {
            inEdgeIndices.add(edge.getSourceId());
        }

        return inEdgeIndices;
    }

    public int getId() {
        return id;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    /**
     * Get the maximum parallelism for this stream node.
     *
     * @return Maximum parallelism
     */
    int getMaxParallelism() {
        return maxParallelism;
    }

    /**
     * Set the maximum parallelism for this stream node.
     *
     * @param maxParallelism Maximum parallelism to be set
     */
    void setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    public ResourceSpec getMinResources() {
        return minResources;
    }

    public ResourceSpec getPreferredResources() {
        return preferredResources;
    }

    public void setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
        this.minResources = minResources;
        this.preferredResources = preferredResources;
    }

    public void setManagedMemoryUseCaseWeights(
            Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            Set<ManagedMemoryUseCase> slotScopeUseCases) {
        managedMemoryOperatorScopeUseCaseWeights.putAll(operatorScopeUseCaseWeights);
        managedMemorySlotScopeUseCases.addAll(slotScopeUseCases);
    }

    public Map<ManagedMemoryUseCase, Integer> getManagedMemoryOperatorScopeUseCaseWeights() {
        return Collections.unmodifiableMap(managedMemoryOperatorScopeUseCaseWeights);
    }

    public Set<ManagedMemoryUseCase> getManagedMemorySlotScopeUseCases() {
        return Collections.unmodifiableSet(managedMemorySlotScopeUseCases);
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public void setBufferTimeout(Long bufferTimeout) {
        this.bufferTimeout = bufferTimeout;
    }

    @VisibleForTesting
    public StreamOperator<?> getOperator() {
        checkState(operatorFactory instanceof SimpleOperatorFactory);
        return (StreamOperator<?>) ((SimpleOperatorFactory) operatorFactory).getOperator();
    }

    public StreamOperatorFactory<?> getOperatorFactory() {
        return operatorFactory;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public String getOperatorDescription() {
        return operatorDescription;
    }

    public void setOperatorDescription(String operatorDescription) {
        this.operatorDescription = operatorDescription;
    }

    public void setSerializersIn(TypeSerializer<?>... typeSerializersIn) {
        checkArgument(typeSerializersIn.length > 0);
        // Unfortunately code above assumes type serializer can be null, while users of for example
        // getTypeSerializersIn would be confused by returning an array size of two with all
        // elements set to null...
        this.typeSerializersIn =
                Arrays.stream(typeSerializersIn)
                        .filter(typeSerializer -> typeSerializer != null)
                        .toArray(TypeSerializer<?>[]::new);
    }

    public TypeSerializer<?>[] getTypeSerializersIn() {
        return typeSerializersIn;
    }

    public TypeSerializer<?> getTypeSerializerOut() {
        return typeSerializerOut;
    }

    public void setSerializerOut(TypeSerializer<?> typeSerializerOut) {
        this.typeSerializerOut = typeSerializerOut;
    }

    public Class<? extends TaskInvokable> getJobVertexClass() {
        return jobVertexClass;
    }

    public InputFormat<?, ?> getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(InputFormat<?, ?> inputFormat) {
        this.inputFormat = inputFormat;
    }

    public OutputFormat<?> getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(OutputFormat<?> outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Nullable
    public String getSlotSharingGroup() {
        return slotSharingGroup;
    }

    public void setSlotSharingGroup(
            @Nullable
            String slotSharingGroup) {
        this.slotSharingGroup = slotSharingGroup;
    }

    public
    @Nullable
    String getCoLocationGroup() {
        return coLocationGroup;
    }

    public void setCoLocationGroup(
            @Nullable
            String coLocationGroup) {
        this.coLocationGroup = coLocationGroup;
    }

    public boolean isSameSlotSharingGroup(StreamNode downstreamVertex) {
        return (slotSharingGroup == null && downstreamVertex.slotSharingGroup == null)
                || (slotSharingGroup != null
                && slotSharingGroup.equals(downstreamVertex.slotSharingGroup));
    }

    @Override
    public String toString() {
        return operatorName + "-" + id;
    }

    public KeySelector<?, ?>[] getStatePartitioners() {
        return statePartitioners;
    }

    public void setStatePartitioners(KeySelector<?, ?>... statePartitioners) {
        checkArgument(statePartitioners.length > 0);
        this.statePartitioners = statePartitioners;
    }

    public TypeSerializer<?> getStateKeySerializer() {
        return stateKeySerializer;
    }

    public void setStateKeySerializer(TypeSerializer<?> stateKeySerializer) {
        this.stateKeySerializer = stateKeySerializer;
    }

    public String getTransformationUID() {
        return transformationUID;
    }

    void setTransformationUID(String transformationId) {
        this.transformationUID = transformationId;
    }

    public String getUserHash() {
        return userHash;
    }

    public void setUserHash(String userHash) {
        this.userHash = userHash;
    }

    public void addInputRequirement(
            int inputIndex, StreamConfig.InputRequirement inputRequirement) {
        inputRequirements.put(inputIndex, inputRequirement);
    }

    public Map<Integer, StreamConfig.InputRequirement> getInputRequirements() {
        return inputRequirements;
    }

    public Optional<OperatorCoordinator.Provider> getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        if (operatorFactory instanceof CoordinatedOperatorFactory) {
            return Optional.of(
                    ((CoordinatedOperatorFactory) operatorFactory)
                            .getCoordinatorProvider(operatorName, operatorID));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamNode that = (StreamNode) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Nullable
    public IntermediateDataSetID getConsumeClusterDatasetId() {
        return consumeClusterDatasetId;
    }

    public void setConsumeClusterDatasetId(
            @Nullable
            IntermediateDataSetID consumeClusterDatasetId) {
        this.consumeClusterDatasetId = consumeClusterDatasetId;
    }
}
