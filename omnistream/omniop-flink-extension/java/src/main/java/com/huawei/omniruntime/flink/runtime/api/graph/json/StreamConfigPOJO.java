package com.huawei.omniruntime.flink.runtime.api.graph.json;

import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.OperatorDescriptorHelper;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.OperatorPOJO;

import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * StreamConfigPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class StreamConfigPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(StreamConfigPOJO.class);

    List<StreamEdgePOJO> outEdgesInOrder;

    // Graph based operator relevant configs
    List<NonChainedOutputPOJO> opNonChainedOutputs;
    List<StreamEdgePOJO> opChainedOutputs;

    // operator specific  configuration.
    // Notice, operatorPOJO is the properties  of Graph NODE, not the node itself
    OperatorPOJO operatorDescription;

    String operatorFactoryName; // Useful for troubleshooting

    List<StreamEdgePOJO> inStreamEdges;
    int numberOfNetworkInputs;

    HashMap<String, String> omniConf;

    public StreamConfigPOJO() {
    }

    ;

    public StreamConfigPOJO(StreamConfig streamConfig, ClassLoader cl) {
        buildOutEdgesInOrder(streamConfig, cl);

        // graph-based operator chain
        opNonChainedOutputs = new ArrayList<>();
        buildOpNonChainedOutputs(streamConfig, cl);

        opChainedOutputs = new ArrayList<>();
        buildOpChainedOutput(streamConfig, cl);

        numberOfNetworkInputs = streamConfig.getNumberOfNetworkInputs();

        inStreamEdges = new ArrayList<>();
        buildInStreamEdges(streamConfig, cl);

        if (streamConfig.getStreamOperatorFactory(cl) != null) {
            operatorFactoryName = streamConfig.getStreamOperatorFactory(cl).getClass().getCanonicalName();
        } else {
            operatorFactoryName = "UNKNOWN";
        }

        operatorDescription = OperatorDescriptorHelper.createOperatorDescriptor(streamConfig, cl);

        // omni information pojo
        String omniConfJson = streamConfig.getOmniConf();
        omniConf = JsonHelper.fromJson(omniConfJson, HashMap.class);
    }

    private void buildOutEdgesInOrder(StreamConfig streamConfig, ClassLoader cl) {
        List<StreamEdge> nonChainedOutputs = streamConfig.getOutEdgesInOrder(cl);
        this.outEdgesInOrder = new ArrayList<StreamEdgePOJO>();
        for (StreamEdge edge : nonChainedOutputs) {
            StreamEdgePOJO edgePOJO = new StreamEdgePOJO(edge);
            this.outEdgesInOrder.add(edgePOJO);
        }
    }

    public List<StreamEdgePOJO> getOutEdgesInOrder() {
        return outEdgesInOrder;
    }

    public void setOutEdgesInOrder(
            List<StreamEdgePOJO> outEdgesInOrder) {
        this.outEdgesInOrder = outEdgesInOrder;
    }

    public List<StreamEdgePOJO> getOpChainedOutputs() {
        return opChainedOutputs;
    }

    public void setOpChainedOutputs(List<StreamEdgePOJO> opChainedOutputs) {
        this.opChainedOutputs = opChainedOutputs;
    }

    public String getOperatorFactoryName() {
        return operatorFactoryName;
    }

    public void setOperatorFactoryName(String operatorFactoryName) {
        this.operatorFactoryName = operatorFactoryName;
    }

    public List<NonChainedOutputPOJO> getOpNonChainedOutputs() {
        return opNonChainedOutputs;
    }

    public void setOpNonChainedOutputs(List<NonChainedOutputPOJO> opNonChainedOutputs) {
        this.opNonChainedOutputs = opNonChainedOutputs;
    }

    public OperatorPOJO getOperatorDescription() {
        return operatorDescription;
    }

    public void setOperatorDescription(OperatorPOJO operatorDescription) {
        this.operatorDescription = operatorDescription;
    }

    public int getNumberOfNetworkInputs() {
        return numberOfNetworkInputs;
    }

    public void setNumberOfNetworkInputs(int numberOfNetworkInputs) {
        this.numberOfNetworkInputs = numberOfNetworkInputs;
    }

    public List<StreamEdgePOJO> getInStreamEdges() {
        return inStreamEdges;
    }

    public void setInStreamEdges(List<StreamEdgePOJO> inStreamEdges) {
        this.inStreamEdges = inStreamEdges;
    }

    public HashMap<String, String> getOmniConf() {
        return omniConf;
    }

    public void setOmniConf(HashMap<String, String> omniConf) {
        this.omniConf = omniConf;
    }

    private void buildOpNonChainedOutputs(StreamConfig operatorConfig, ClassLoader cl) {
        // create collectors for the network outputs
        for (NonChainedOutput streamOutput
                : operatorConfig.getOperatorNonChainedOutputs(cl)) {
            NonChainedOutputPOJO opNonChainedOutputPOJO = new NonChainedOutputPOJO(streamOutput);
            opNonChainedOutputs.add(opNonChainedOutputPOJO);
        }
    }

    private void buildOpChainedOutput(StreamConfig operatorConfig, ClassLoader cl) {
        // Create collectors for the chained outputs
        for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(cl)) {
            StreamEdgePOJO edgePOJO = new StreamEdgePOJO(outputEdge);
            opChainedOutputs.add(edgePOJO);
        }
    }

    private void buildInStreamEdges(StreamConfig operatorConfig, ClassLoader cl) {
        for (StreamEdge outputEdge : operatorConfig.getInPhysicalEdges(cl)) {
            StreamEdgePOJO edgePOJO = new StreamEdgePOJO(outputEdge);
            inStreamEdges.add(edgePOJO);
        }
    }


    @Override
    public String toString() {
        return "StreamConfigPOJO{"
                + "nonChainedOutputs=" + outEdgesInOrder
                + ", opNonChainedOutputs=" + opNonChainedOutputs
                + ", opChainedOutputs=" + opChainedOutputs
                + ", operator=" + operatorDescription
                + ", operatorFactoryName='" + operatorFactoryName + '\''
                + '}';
    }
}
