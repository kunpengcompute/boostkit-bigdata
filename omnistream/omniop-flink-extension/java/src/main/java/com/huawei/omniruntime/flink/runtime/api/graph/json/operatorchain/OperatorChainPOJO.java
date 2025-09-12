package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain;

import static org.apache.flink.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * OperatorChainPOJO
 *
 * @since 2025-04-29
 */
public class OperatorChainPOJO {
    private List<OperatorPOJO> operators;

    // Default constructor
    public OperatorChainPOJO() {
        this.operators = new ArrayList<>(); // Initialize the list to avoid NullPointerExceptions
    }

    // Full argument constructor
    public OperatorChainPOJO(List<OperatorPOJO> operators) {
        this.operators = operators;
    }

    // Getter for operators
    public List<OperatorPOJO> getOperators() {
        return operators;
    }

    // Setter for operators
    public void setOperators(List<OperatorPOJO> operators) {
        this.operators = operators;
    }

    @Override
    public String toString() {
        return "OperatorChainPOJO{"
                + "operators=" + operators
                + '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        checkState(o instanceof OperatorChainPOJO, "o is not OperatorChainPOJO");
        OperatorChainPOJO that = (OperatorChainPOJO) o;
        return Objects.equals(operators, that.operators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operators);
    }
}