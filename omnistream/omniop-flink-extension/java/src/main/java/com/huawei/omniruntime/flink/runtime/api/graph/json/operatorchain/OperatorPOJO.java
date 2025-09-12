package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain;

import java.util.ArrayList;
import java.util.List;

/**
 * OperatorPOJO
 *
 * @version 1.0.0
 * @since 2025/02/20
 */

public class OperatorPOJO {
    private String name;
    private String id;
    private String description;  // dynamic json, need to parse again
    private List<TypeDescriptionPOJO> inputs;
    private TypeDescriptionPOJO output;
    private String operatorId;

    // Useful for troubleshooting
    private int vertexID;


    public OperatorPOJO() {
        this.inputs = new ArrayList<>(); // Initialize the list
        this.vertexID = -1;
    }

    public OperatorPOJO(String name, String id, String description, List<TypeDescriptionPOJO> inputs, TypeDescriptionPOJO output, String operatorId, int vertexID) {
        this.name = name;
        this.id = id;
        this.description = description;
        this.inputs = inputs;
        this.output = output;
        this.operatorId = operatorId;
        this.vertexID = vertexID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<TypeDescriptionPOJO> getInputs() {
        return inputs;
    }

    public void setInputs(List<TypeDescriptionPOJO> inputs) {
        this.inputs = inputs;
    }

    public TypeDescriptionPOJO getOutput() {
        return output;
    }

    public void setOutput(TypeDescriptionPOJO output) {
        this.output = output;
    }

    public int getVertexID() {
        return vertexID;
    }

    public void setVertexID(int vertexID) {
        this.vertexID = vertexID;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }
}
