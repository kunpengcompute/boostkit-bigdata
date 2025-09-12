package com.huawei.omniruntime.flink.streaming.api.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

import com.huawei.omniruntime.flink.runtime.tasks.OmniOneInputStreamTaskV2;
import com.huawei.omniruntime.flink.runtime.tasks.OmniSourceOperatorStreamTaskV2;
import com.huawei.omniruntime.flink.runtime.tasks.OmniSourceStreamTaskV2;
import com.huawei.omniruntime.flink.runtime.tasks.OmniTwoInputStreamTaskV2;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.AbstractValidateOperatorStrategy;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateCalcOPStrategy;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateOperatorStrategyFactory;
import com.huawei.omniruntime.flink.utils.DescriptionUtil;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkState;


public final class OmniGraphOverride {
    private static final Logger LOG = LoggerFactory.getLogger(OmniGraphOverride.class);
    private static final Pattern SOURCE_REGEX = Pattern.compile("^Source");

    // patter to extract substring till the first "[" like   "Calc[..."   flink 1.19
    //private static final Pattern operatorNameRegex = Pattern.compile("^\\s*(.*?)\\[");
    // patter to extract substring till the first "(" like   "Calc(..."   flink 1.14
    private static final Pattern OPERATOR_NAME_REGEX = Pattern.compile("^\\s*(.*?)[\\[(]");

    // watermark op flink 1.16 name like WatermarkAssigner( ...
    private static final Pattern WATERMARK_OP_NAME_REGEX = Pattern.compile("^\\s*(.*?)\\(");

    private static final Set<String> SUPPORT_STREAM_OP_NAME = new HashSet<>(Arrays.asList(
            "StreamFilter",
            "StreamMap",
            "StreamGroupedReduceOperator",
            "StreamFlatMap",
            "StreamSource"));

    private static final Set<String> SUPPORT_OP_NAME = new HashSet<>();
    private static final Set<String> VALID_PARTITION_NAMES = new HashSet<>(
        Arrays.asList("ForwardPartitioner", "KeyGroupStreamPartitioner", "RescalePartitioner", "RebalancePartitioner",
            "GlobalPartitioner"));
    private static final String WRITER_NAME = "Writer";
    private static final String SINK_NAME = "Sink:";
    private static final String SINK_REGEX_PATTERN = ".*"
        + Pattern.quote(WRITER_NAME) + "\\s*$" + "|^" + SINK_NAME + ".*";
    private static final Pattern SINK_REGEX = Pattern.compile(SINK_REGEX_PATTERN);

    private static final Set<String> SUPPORT_KAFKA_SCHEMA_TPYE = new HashSet<>();

    private static boolean performanceMode = true;

    static {
        try {
            Map<String, String> envMap = System.getenv();
            if (!CollectionUtil.isNullOrEmpty(envMap) && envMap.containsKey("FLINK_PERFORMANCE")) {
                String flinkPerformance = envMap.getOrDefault("FLINK_PERFORMANCE", "true");
                LOG.info("flinkPerformance is {}", flinkPerformance);
                performanceMode = flinkPerformance.equals("true");
            }
        } catch (Exception exception) {
            LOG.warn("get env failed! ", exception);
        }
        if (performanceMode) {
            // performance mode supports all op temporarily.Some op will be proposed in the future.
            SUPPORT_OP_NAME.addAll(Arrays.asList(
                    "Calc",
                    "LookupJoin",
                    "WatermarkAssigner",
                    "StreamRecordTimestampInserter",
                    "ConstraintEnforcer"));
        } else {
            SUPPORT_OP_NAME.addAll(Arrays.asList(
                    "Calc",
                    "GroupAggregate",
                    "LocalGroupAggregate",
                    "GlobalGroupAggregate",
                    "IncrementalGroupAggregate",
                    "Join",
                    "LookupJoin",
                    "WindowAggregate",
                    "WindowJoin",
                    "GroupWindowAggregate",
                    "Deduplicate",
                    "Expand",
                    "GlobalWindowAggregate",
                    "LocalWindowAggregate",
                    "WatermarkAssigner",
                    "Rank",
                    "StreamRecordTimestampInserter",
                    "ConstraintEnforcer"));
        }
        SUPPORT_KAFKA_SCHEMA_TPYE.add("JsonRowDataDeserializationSchema");
    }

    /**
     * setStateBackend
     *
     * @param stateBackend backend
     */
    public static void setStateBackend(StateBackend stateBackend) {
        if (performanceMode && stateBackend != null && !(stateBackend instanceof HashMapStateBackend)) {
            SUPPORT_OP_NAME.clear();
            SUPPORT_OP_NAME.addAll(
                    Arrays.asList("Calc", "WatermarkAssigner", "StreamRecordTimestampInserter", "ConstraintEnforcer"));
        }
    }

    /**
     * validateVertexForOmniTask
     *
     * @param vertexEntry vertexEntry
     * @param chainInfos chainInfos
     * @param vertexConfigs vertexConfigs
     * @return validate result
     */
    public static boolean validateVertexForOmniTask(Map.Entry<Integer, JobVertex> vertexEntry,
                                                    Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos,
                                                    Map<Integer, Map<Integer, StreamConfig>> chainedConfigs,
                                                    Map<Integer, StreamConfig> vertexConfigs,
                                                    JobType jobType) {

        StreamConfig vertexConfig = new StreamConfig(vertexEntry.getValue().getConfiguration());
        Integer vertexID = vertexEntry.getKey();
        LOG.info("validateVertexForOmniTask : vertexID is {}, and vertexName {}",
            vertexID, vertexConfig.getOperatorName());

        JobVertex jobVertex = vertexEntry.getValue();

        if (validateVertexChainInfoForOmniTask(vertexID, chainInfos, chainedConfigs, jobVertex, vertexConfigs, jobType)) {
            LOG.info("validateVertexForOmniTask  true for : vertexID is {}, and vertexName {}", vertexID, vertexConfig.getOperatorName());
            vertexConfig.setUseOmniEnabled(true);
            return true;
        } else {
            LOG.info("validateVertexForOmniTask  false  for : vertexID is {}, and vertexName {}", vertexID, vertexConfig.getOperatorName());
            vertexConfig.setUseOmniEnabled(false);
            return false;
        }
    }

    private static boolean validateVertexChainInfoForOmniTask(Integer vertexID,
        Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos,
        Map<Integer, Map<Integer, StreamConfig>> chainedConfigs, JobVertex jobVertex,
        Map<Integer, StreamConfig> vertexConfigs, JobType jobType) {
        // walkthrough each operator
        StreamingJobGraphGenerator.OperatorChainInfo chainInfo = chainInfos.get(vertexID);
        if (chainInfo == null) {
            LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} is null", vertexID);
            return false;
        }

        StreamGraph streamGraph = chainInfo.getStreamGraph();
        List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
        for (StreamNode node : chainedNode) {
            if (validateNodeForOmniTask(vertexID, vertexConfigs, jobType, streamGraph, node)) {
                return false;
            }
        }

        vertexConfigs.get(vertexID).setJobType(jobType.getValue());
        if (jobType.equals(JobType.STREAM)) {
            StreamConfig config = vertexConfigs.get(vertexID);
            config.setTransitiveChainedTaskConfigsOptimized(chainedConfigs.get(vertexID));
            updateInvokableClass(jobVertex);
        }
        LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID  {} is {}", vertexID, true);
        return true;
    }

    private static boolean validateNodeForOmniTask(Integer vertexID, Map<Integer, StreamConfig> vertexConfigs,
        JobType jobType, StreamGraph streamGraph, StreamNode node) {
        String operatorName = node.getOperatorName();
        String operatorDescription = node.getOperatorDescription();
        switch (jobType) {
            case SQL:
                // check partitioner type
                if (validatePartitioner(vertexID, node, streamGraph, operatorName)) {
                    return true;
                }
                LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} "
                        + "has the operator {} with description {}",
                    vertexID, operatorName, operatorDescription);
                if (validateOperatorByNameForOmniTask(operatorName, operatorDescription,
                    node.getOperatorFactory())) {
                    LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} "
                            + ": the operator {} is SUITABLE for OmniTask", vertexID, operatorName);
                } else {
                    LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} "
                            + ": the operator {} is NOT SUITABLE for OmniTask", vertexID, operatorName);
                    return true;
                }
                break;
            case STREAM:
                StreamConfig streamConfig = vertexConfigs.get(node.getId());
                boolean result;
                try {
                    result = StreamNodeOptimized.getInstance().setExtraDescription(
                        node, streamConfig, streamGraph, jobType);
                } catch (NoSuchFieldException | IllegalAccessException | IOException | ClassNotFoundException e) {
                    throw new FlinkRuntimeException(
                        "Error occurs during the process of compatibility between new and old sinks or sources", e);
                }

                if (!result) {
                    LOG.info("setExtraDescription StreamNode of vertex ID  {} "
                        + ": the operator {} is NOT SUITABLE for OmniTask", vertexID, operatorName);
                    return true;
                }
                break;
            default:
                LOG.error("vertex ID  {} Invalid JobType.", vertexID);
                return true;
        }
        return false;
    }

    private static boolean validatePartitioner(Integer vertexID, StreamNode node,
                                               StreamGraph streamGraph, String operatorName) {
        boolean result;
        for (StreamEdge edge : streamGraph.getStreamEdges(node.getId())) {
            if (edge.getPartitioner() != null) {
                String partitionName = edge.getPartitioner().getClass().getSimpleName();
                LOG.info("Partition strategy for node {}: {}", node.getOperatorName(), partitionName);
                if (!VALID_PARTITION_NAMES.contains(partitionName)) {
                    LOG.info("validatePartitionStrategy for the operation {} is false ", operatorName);
                    result = false;
                    LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} is {}",
                            vertexID, result);
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public static JobType getJobType(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        JobType jobType = JobType.NULL;
        boolean onlySourceAndSink = true;
        List<String> inputTypeList = new ArrayList<>();
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            for (StreamNode node : chainedNode) {
                String operatorName = node.getOperatorName();
                if (isSource(operatorName)) {
                    jobType = getSourceJobType(node, operatorName, jobType, inputTypeList);
                    continue;
                }
                if (isSink(operatorName)) {
                    jobType = getSinkJobType(node, operatorName, jobType);
                    continue;
                }
                if (operatorName.contains(": Committer")) {
                    continue;
                }
                onlySourceAndSink = false;
                String opSimpleName = extractOperatorName(operatorName);
                if (SUPPORT_OP_NAME.contains(opSimpleName)) {
                    jobType = jobType.getCombinationsJobType(JobType.SQL);
                }

                if (node.getOperatorFactory() instanceof SimpleOperatorFactory) {
                    operatorName = node.getOperator().getClass().getSimpleName();
                }

                if (SUPPORT_STREAM_OP_NAME.contains(operatorName)) {
                    jobType = jobType.getCombinationsJobType(JobType.STREAM);
                }
            }
        }
        if (onlySourceAndSink && jobType.equals(JobType.SQL)) {
            ValidateCalcOPStrategy validate = new ValidateCalcOPStrategy();
            if (!inputTypeList.isEmpty() && !validate.validateDataTypes(Collections.singletonList(inputTypeList))) {
                jobType = JobType.NULL;
            }
        }
        return jobType;
    }

    private static JobType getSinkJobType(StreamNode node, String operatorName, JobType jobType) {
        TypeSerializer<?>[] typeSerializersIn = node.getTypeSerializersIn();
        if (typeSerializersIn == null || typeSerializersIn.length == 0) {
            throw new FlinkRuntimeException("Empty type serializer ins for operator " + operatorName);
        }
        JobType newJobType;
        if (typeSerializersIn[0] instanceof AbstractRowDataSerializer) {
            newJobType = jobType.getCombinationsJobType(JobType.SQL);
        } else {
            newJobType = jobType.getCombinationsJobType(JobType.STREAM);
        }
        return newJobType;
    }

    private static JobType getSourceJobType(StreamNode node, String operatorName, JobType jobType,
        List<String> inputTypeList) {
        TypeSerializer<?> typeSerializerOut = node.getTypeSerializerOut();
        if (typeSerializerOut == null) {
            throw new FlinkRuntimeException("Empty type serializer out for operator " + operatorName);
        }
        JobType newJobType;
        if (typeSerializerOut instanceof AbstractRowDataSerializer) {
            newJobType = jobType.getCombinationsJobType(JobType.SQL);
            buildInputTypes(inputTypeList, typeSerializerOut);
        } else {
            newJobType = jobType.getCombinationsJobType(JobType.STREAM);
        }
        return newJobType;
    }

    private static void buildInputTypes(List<String> inputTypeList, TypeSerializer<?> typeSerializerOut) {
        if (!(typeSerializerOut instanceof RowDataSerializer)) {
            return;
        }
        LogicalType[] types = ReflectionUtils.retrievePrivateField(typeSerializerOut, "types");
        for (LogicalType type : types) {
            if (type.getTypeRoot() == LogicalTypeRoot.ROW) {
                checkState(type instanceof RowType);
                List<RowType.RowField> subFields = ((RowType) type).getFields();
                for (RowType.RowField rowField : subFields) {
                    inputTypeList.add(DescriptionUtil.getFieldType(rowField.getType()));
                }
            } else {
                inputTypeList.add(DescriptionUtil.getFieldType(type));
            }
        }
    }

    private static void updateInvokableClass(JobVertex jobVertex) {
        if (jobVertex.getInvokableClassName().equals("org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask")) {
            jobVertex.setInvokableClass(OmniTwoInputStreamTaskV2.class);
        } else if (jobVertex.getInvokableClassName().equals("org.apache.flink.streaming.runtime.tasks.OneInputStreamTask")) {
            jobVertex.setInvokableClass(OmniOneInputStreamTaskV2.class);
        } else if (jobVertex.getInvokableClassName().equals("org.apache.flink.streaming.runtime.tasks.SourceStreamTask")) {
            jobVertex.setInvokableClass(OmniSourceStreamTaskV2.class);
        } else if (jobVertex.getInvokableClassName().equals("org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask")) {
            jobVertex.setInvokableClass(OmniSourceOperatorStreamTaskV2.class);
        } else {
            new Exception("unsupported class");
        }
    }

    // true for omniTask
    private static boolean validateOperatorByNameForOmniTask(String operatorName, String operatorDescription,
        StreamOperatorFactory operatorFactory) {
        if (isSource(operatorName)) {
            return validateSource(operatorDescription);
        } else if (isSink(operatorName)) {
            return validateSink(operatorName, operatorFactory);
        } else if (operatorName.contains(": Committer")) {
            return true;
        } else if (isPartitionCommitter(operatorName)) {
            return true;
        } else {
            String opSimpleName = extractOperatorName(operatorName);

            if (!SUPPORT_OP_NAME.contains(opSimpleName)) {
                return false;
            }
            if (operatorDescription.contains("INVALID")) {
                LOG.info("The operator description contains INVALID operator/expressions. ");
                return false;
            }
            Map<String, Object> jsonMap = toJsonMap(operatorDescription);

            AbstractValidateOperatorStrategy validateStrategy =
                ValidateOperatorStrategyFactory.getStrategy(opSimpleName);
            return validateStrategy.executeValidateOperator(jsonMap);
        }
    }

    private static boolean validateSink(String operatorName, StreamOperatorFactory operatorFactory) {
        if (performanceMode && (operatorName.contains("nexmark_q20") || operatorName.contains("nexmark_q9")
            || operatorName.contains("StreamingFileWriter"))) {
            return false;
        }
        if (!(operatorFactory instanceof SinkWriterOperatorFactory)) {
            return true;
        }
        Sink sink = ((SinkWriterOperatorFactory) operatorFactory).getSink();
        if (!sink.getClass().getSimpleName().equals("KafkaSink")) {
            return true;
        }
        Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
        Object valueSerializationSchema = ReflectionUtils
            .retrievePrivateField(recordSerializer, "valueSerialization");
        return "JsonRowDataSerializationSchema".equals(valueSerializationSchema.getClass().getSimpleName());
    }

    private static boolean validateSource(String operatorDescription) {
        if (!operatorDescription.contains("originDescription")) {
            return true;
        }
        Map<String, Object> jsonMap = toJsonMap(operatorDescription);
        if (!jsonMap.containsKey("deserializationSchema")
            || !(jsonMap.get("deserializationSchema") instanceof String)
            || !SUPPORT_KAFKA_SCHEMA_TPYE.contains((String) jsonMap.get("deserializationSchema"))) {
            return false;
        }
        if (!jsonMap.containsKey("hasMetadata") || !(jsonMap.get("hasMetadata") instanceof Boolean)
            || (Boolean) jsonMap.get("hasMetadata")) {
            return false;
        }
        return jsonMap.containsKey("watermarkStrategy") && (jsonMap.get("watermarkStrategy") instanceof String)
            && !((String) jsonMap.get("watermarkStrategy")).isEmpty();
    }

    private static Map<String, Object> toJsonMap(String operatorDescription) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = null;
        try {
            jsonMap = objectMapper.readValue(operatorDescription, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            LOG.warn("operatorDescription read jsonString to Map failed!", e);
        }
        return jsonMap;
    }

    private static boolean isSource(String operatorName) {
        Matcher matcher = SOURCE_REGEX.matcher(operatorName);
        return matcher.find();
    }

    private static boolean isSink(String operatorName) {
        Matcher matcher = SINK_REGEX.matcher(operatorName);
        return matcher.find();
    }

    private static boolean isPartitionCommitter(String operatorName) {
        return operatorName.equals("PartitionCommitter");
    }

    private static String extractOperatorName(String operatorName) {
        Matcher matcher = operatorName.contains("WatermarkAssigner") || operatorName.contains("GroupAggregate")
            ? WATERMARK_OP_NAME_REGEX.matcher(operatorName) : OPERATOR_NAME_REGEX.matcher(operatorName);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return "UNKNOWN_OPERATOR_IMPOSSIBLE_MATCH";
        }
    }
}
