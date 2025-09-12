package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JobIDPOJO;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * TaskDeploymentDescriptorPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class TaskDeploymentDescriptorPOJO {
    private JobIDPOJO jobId;
    private List<ResultPartitionDeploymentDescriptorPOJO> producedPartitions;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private List<InputGateDeploymentDescriptorPOJO> inputGates;

    public TaskDeploymentDescriptorPOJO() {
    }

    public TaskDeploymentDescriptorPOJO(TaskDeploymentDescriptor tdd) {
        this.jobId = new JobIDPOJO(tdd.getJobId());
        this.producedPartitions = new ArrayList<>();
        for (ResultPartitionDeploymentDescriptor rpd : tdd.getProducedPartitions()) {
            producedPartitions.add(new ResultPartitionDeploymentDescriptorPOJO(rpd));
        }
        this.inputGates = new ArrayList<>();
        if (tdd.getInputGates() != null) {
            for (InputGateDeploymentDescriptor igd : tdd.getInputGates()) {
                inputGates.add(new InputGateDeploymentDescriptorPOJO(igd));
            }
        }
    }

    public TaskDeploymentDescriptorPOJO(
            JobIDPOJO jobId,
            List<ResultPartitionDeploymentDescriptorPOJO> producedPartitions,
            List<InputGateDeploymentDescriptorPOJO> inputGates) {
        this.jobId = jobId;
        this.producedPartitions = producedPartitions;
        this.inputGates = inputGates;
    }

    public JobIDPOJO getJobId() {
        return jobId;
    }

    public void setJobId(JobIDPOJO jobId) {
        this.jobId = jobId;
    }

    public List<ResultPartitionDeploymentDescriptorPOJO> getProducedPartitions() {
        return producedPartitions;
    }

    public void setProducedPartitions(
            List<ResultPartitionDeploymentDescriptorPOJO> producedPartitions) {
        this.producedPartitions = producedPartitions;
    }

    public List<InputGateDeploymentDescriptorPOJO> getInputGates() {
        return inputGates;
    }

    public void setInputGates(List<InputGateDeploymentDescriptorPOJO> inputGates) {
        this.inputGates = inputGates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        checkState(o instanceof TaskDeploymentDescriptorPOJO, "o is not TaskDeploymentDescriptorPOJO");
        TaskDeploymentDescriptorPOJO that = (TaskDeploymentDescriptorPOJO) o;
        return Objects.equals(jobId, that.jobId)
                && Objects.equals(producedPartitions, that.producedPartitions)
                && Objects.equals(inputGates, that.inputGates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, producedPartitions, inputGates);
    }

    @Override
    public String toString() {
        return "TaskDeploymentDescriptorPOJO{"
                + "jobId=" + jobId
                + ", producedPartitions=" + producedPartitions
                + ", inputGates=" + inputGates
                + '}';
    }
}