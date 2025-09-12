package com.huawei.omniruntime.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


import static com.huawei.omniruntime.flink.streaming.api.graph.OmniGraphOverride.validateVertexForOmniTask;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OmniGraphOverrideTest {

    private static final String OPERATOR_NAME = "operatorName";

    private static final String CALC_OPERATOR_NAME = "Calc(select=[id, category, price])";

    private static final String AGG_OPERATOR_NAME = "GroupAggregate(groupBy=[id, category])";

    private static final String JOIN_OPERATOR_NAME = "Join(joinType=[InnerJoin])";

    private static final String SOURCE_OPERATOR_NAME = "Source";

    private static final String CALC_OPERATOR_DESC = "{\"originDescription\":null,\"inputTypes\":[\"BIGINT\",\"BIGINT\",\"BIGINT\",\"BIGINT\"]" +
            ",\"outputTypes\":[\"BIGINT\",\"BIGINT\",\"BIGINT\"],\"indices\":[{\"exprType\":\"FIELD_REFERENCE\"," +
            "\"dataType\":\"BIGINT\",\"colVal\":0},{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":\"BIGINT\",\"colVal\":1},{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":\"BIGINT\",\"colVal\":3}],\"condition\":null}";

    private static final String JOIN_OPERATOR_DESC = "{\"originDescription\":null,\"leftInputTypes\":[\"BIGINT\",\"BIGINT\"],\"rightInputTypes\":" +
            "[\"BIGINT\",\"BIGINT\"],\"outputTypes\":[\"BIGINT\",\"BIGINT\",\"BIGINT\",\"BIGINT\"],\"leftJoinKey\":[0]," +
            "\"rightJoinKey\":[0],\"nonEquiCondition\":null,\"joinType\":\"InnerJoin\",\"filterNulls\":[true]," +
            "\"leftInputSpec\":\"NoUniqueKey\",\"rightInputSpec\":\"NoUniqueKey\",\"leftUniqueKeys\":[],\"rightUniqueKeys\":[]}";

    private static final String AGG_OPERATOR_DESC = "{\"originDescription\":null,\"inputTypes\":[\"BIGINT\",\"BIGINT\",\"BIGINT\"]," +
            "\"outputTypes\":[\"BIGINT\",\"BIGINT\",\"BIGINT\"],\"grouping\":[0,1],\"aggInfoList\":" +
            "{\"aggregateCalls\":[{\"name\":\"MAX($2)\",\"aggregationFunction\":\"LongMaxAggFunction\"," +
            "\"argIndexes\":[2],\"consumeRetraction\":\"false\"}],\"accTypes\":[\"BIGINT\"],\"aggValueTypes\":[\"BIGINT\"],\"indexOfCountStar\":-1}}";

    private Map<Integer, JobVertex> vertexMap;

    private Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos;


    private void initVertex() {
        vertexMap = new HashMap<>();

        JobVertex source = new JobVertex("source");

        JobVertex target1 = new JobVertex("target1");
        target1.getConfiguration().setString(OPERATOR_NAME, SOURCE_OPERATOR_NAME);

        JobVertex target2 = new JobVertex("target2");
        target2.getConfiguration().setString(OPERATOR_NAME, JOIN_OPERATOR_NAME);
        connectJob(target2, source);

        JobVertex target3 = new JobVertex("target3");
        target3.getConfiguration().setString(OPERATOR_NAME, AGG_OPERATOR_NAME);
        connectJob(target3, source);

        JobVertex target4 = new JobVertex("target4");
        target3.getConfiguration().setString(OPERATOR_NAME, "sort");
        connectJob(target4, source);

        JobVertex target5 = new JobVertex("target5");
        target5.getConfiguration().setString(OPERATOR_NAME, "sink");
        connectJob(target5, source);

        vertexMap.put(1, target1);
        vertexMap.put(2, target2);
        vertexMap.put(3, target3);
        vertexMap.put(4, target4);
        vertexMap.put(5, target5);
    }

    private void connectJob(JobVertex target, JobVertex source) {
        target.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
//        target.connectDataSetAsInput(
//                source.getProducedDataSets().get(0), DistributionPattern.ALL_TO_ALL);
    }

    private void initChainInfos() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        chainInfos = new HashMap<>();

        StreamOperator<String> dummyOperator =
                new AbstractStreamOperator<String>() {
                    private static final long serialVersionUID = 1L;
                };

        List<StreamNode> chainedNodes = new ArrayList<>();
        buildStreamNode(1, dummyOperator, CALC_OPERATOR_NAME, CALC_OPERATOR_DESC, chainedNodes);
        buildStreamNode(2, dummyOperator, JOIN_OPERATOR_NAME, JOIN_OPERATOR_DESC, chainedNodes);
        StreamingJobGraphGenerator.OperatorChainInfo chainInfo = getOperatorChainInfo(chainedNodes);
        chainInfo.getAllChainedNodes().addAll(chainedNodes);
        chainInfos.put(2, chainInfo);

        List<StreamNode> chainedNodes2 = new ArrayList<>();
        buildStreamNode(3, dummyOperator, AGG_OPERATOR_NAME, AGG_OPERATOR_DESC, chainedNodes2);
        StreamingJobGraphGenerator.OperatorChainInfo chainInfo2 = getOperatorChainInfo(chainedNodes2);
        chainInfo2.getAllChainedNodes().addAll(chainedNodes2);
        chainInfos.put(3, chainInfo2);

        List<StreamNode> chainedNodes3 = new ArrayList<>();
        buildStreamNode(4, dummyOperator, SOURCE_OPERATOR_NAME, "source", chainedNodes3);
        StreamingJobGraphGenerator.OperatorChainInfo chainInfo3 = getOperatorChainInfo(chainedNodes3);
        chainInfo3.getAllChainedNodes().addAll(chainedNodes3);
        chainInfos.put(5, chainInfo3);
    }

    private void buildStreamNode(int id, StreamOperator<String> dummyOperator, String operatorName, String desc, List<StreamNode> chainedNodes) {
        StreamNode calcNode = new StreamNode(id, null, null, dummyOperator, operatorName, SourceStreamTask.class);
        calcNode.setOperatorDescription(desc);
        chainedNodes.add(calcNode);
    }

    private StreamingJobGraphGenerator.OperatorChainInfo getOperatorChainInfo(List<StreamNode> chainedNodes) throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException, NoSuchFieldException {
        Constructor<StreamingJobGraphGenerator.OperatorChainInfo> declaredConstructor = StreamingJobGraphGenerator.OperatorChainInfo.class.getDeclaredConstructor(int.class, Map.class, List.class, Map.class, StreamGraph.class);
        declaredConstructor.setAccessible(true);
        StreamGraph graph = new StreamGraph(new ExecutionConfig(), new CheckpointConfig(), SavepointRestoreSettings.none());
        Field field = graph.getClass().getDeclaredField("streamNodes");
        field.setAccessible(true);
        Map<Integer, StreamNode> streamNodes = new HashMap<>();
        for (StreamNode chainedNode : chainedNodes) {
            streamNodes.put(chainedNode.getId(), chainedNode);
        }
        field.set(graph, streamNodes);
        return declaredConstructor.newInstance(1, new HashMap<>(), new ArrayList<>(), new HashMap<>(), graph);
    }

    @BeforeEach
    public void setUp() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        initVertex();
        initChainInfos();
    }

    @Test
    public void testValidateVertexForOmniTaskForVertexIsBeginOrEnd() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(1));

        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForNullChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(4));

        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForSupportJoinChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(2));

        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertTrue(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForNotSupportJoinChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(2));
        chainInfos.get(2).getAllChainedNodes().get(1).setOperatorDescription(JOIN_OPERATOR_DESC.replace("InnerJoin", "LeftJoin"));
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        chainInfos.get(2).getAllChainedNodes().get(1).setOperatorDescription(JOIN_OPERATOR_DESC);
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForNotSupportDataTypeJoinChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(2));
        chainInfos.get(2).getAllChainedNodes().get(1).setOperatorDescription("{\"originDescription\":null,\"leftInputTypes\":[\"BIGINT\",\"DOUBLE\"],\"rightInputTypes\":" +
                "[\"BIGINT\",\"BIGINT\"],\"outputTypes\":[\"BIGINT\",\"BIGINT\",\"BIGINT\",\"BIGINT\"],\"joinType\":\"InnerJoin\",\"leftJoinKey\":[0,1],\"rightJoinKey\":[0]}");
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        chainInfos.get(2).getAllChainedNodes().get(1).setOperatorDescription(JOIN_OPERATOR_DESC);
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForSupportAggChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(3));

        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertTrue(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForNotSupportAggChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(3));
        chainInfos.get(3).getAllChainedNodes().get(0).setOperatorDescription(AGG_OPERATOR_DESC.replace("MAX", "COUNT"));
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        chainInfos.get(3).getAllChainedNodes().get(0).setOperatorDescription(AGG_OPERATOR_DESC);
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForNotSupportDataTypeAggChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(3));

        chainInfos.get(3).getAllChainedNodes().get(0).setOperatorDescription("{\"originDescription\":null,\"inputTypes\":[\"BIGINT\",\"BIGINT\"]," +
                "\"outputTypes\":[\"BIGINT\",\"DOUBLE\"],\"aggInfoList\":" +
                "{\"aggregateCalls\":[{\"name\":\"MAX($2)\",\"aggregationFunction\":\"LongMaxAggFunction\"," +
                "\"argIndexes\":[2],\"consumeRetraction\":\"false\"}],\"accTypes\":[\"BIGINT\"],\"aggValueTypes\":[\"BIGINT\"]}}");
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        chainInfos.get(3).getAllChainedNodes().get(0).setOperatorDescription(AGG_OPERATOR_DESC);
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForEmptyAggChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(3));

        chainInfos.get(3).getAllChainedNodes().get(0).setOperatorDescription("{\"originDescription\":null,\"inputTypes\":[\"BIGINT\"],\"outputTypes\":[\"BIGINT\"],\"grouping\":[0],\"aggInfoList\":{\"aggregateCalls\":[],\"accTypes\":[],\"aggValueTypes\":[],\"indexOfCountStar\":-1},\"generateUpdateBefore\":true}");
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        chainInfos.get(3).getAllChainedNodes().get(0).setOperatorDescription(AGG_OPERATOR_DESC);
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForNotSupportCalcChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(2));
        chainInfos.get(2).getAllChainedNodes().get(0).setOperatorDescription("{\"originDescription\":null,\"inputTypes\":[\"BIGINT\",\"BIGINT\"],\"outputTypes\":[\"BIGINT\",\"BIGINT\"],\"indices\":[{},{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":\"BIGINT\",\"colVal\":0}],\"condition\":{\"exprType\":\"BINARY\",\"returnType\":\"BOOLEAN\",\"operator\":\"GREATER_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":\"BIGINT\",\"colVal\":0},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":\"INTEGER\",\"isNull\":false,\"value\":1000}}}");
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        chainInfos.get(2).getAllChainedNodes().get(0).setOperatorDescription(CALC_OPERATOR_DESC);
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForNotSupportAllCalcChainInfo() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(2));
        chainInfos.get(2).getAllChainedNodes().get(0).setOperatorDescription("{\"originDescription\":null,\"inputTypes\":[\"BIGINT\",\"BIGINT\"],\"outputTypes\":[\"BIGINT\",\"BIGINT\"],\"indices\":[],\"condition\":{\"exprType\":\"BINARY\",\"returnType\":\"BOOLEAN\",\"operator\":\"GREATER_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":\"BIGINT\",\"colVal\":0},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":\"INTEGER\",\"isNull\":false,\"value\":1000}}}");
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        chainInfos.get(2).getAllChainedNodes().get(0).setOperatorDescription(CALC_OPERATOR_DESC);
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForSource() {
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(5));

        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);

        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    @Test
    public void testValidateVertexForOmniTaskForStateBackend() {
        // check state.backend: null
        Map.Entry<Integer, JobVertex> jobVertexEntry = Objects.requireNonNull(getJobVertexEntry(2));
        StreamConfig vertexConfig = getStreamConfig(jobVertexEntry.getValue());

        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);
        assertTrue(vertexConfig.isUseOmniEnabled());

        // check state.backend: hashmap
        vertexConfig.getConfiguration().setString("state.backend","hashmap");
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);
        assertTrue(vertexConfig.isUseOmniEnabled());

        // check state.backend: rocksdb
        vertexConfig.getConfiguration().setString("state.backend","rocksdb");
        validateVertexForOmniTask(jobVertexEntry, chainInfos,null,null,JobType.SQL);
        assertFalse(vertexConfig.isUseOmniEnabled());
    }

    private Map.Entry<Integer, JobVertex> getJobVertexEntry(Integer key) {
        for (Map.Entry<Integer, JobVertex> vertex : vertexMap.entrySet()) {
            if (Objects.equals(vertex.getKey(), key)) {
                return vertex;
            }
        }
        return null;
    }

    private StreamConfig getStreamConfig(JobVertex result) {
        return new StreamConfig(result.getConfiguration());
    }
}