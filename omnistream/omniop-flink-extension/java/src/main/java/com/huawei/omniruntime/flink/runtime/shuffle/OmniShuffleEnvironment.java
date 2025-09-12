package com.huawei.omniruntime.flink.runtime.shuffle;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.List;

// TBD
/**
 * OmniShuffleEnvironment
 *
 * @since 2025-04-27
 */
public interface OmniShuffleEnvironment {
    /**
     * getNativeShuffleServiceRef
     *
     * @return long
     */
    long getNativeShuffleServiceRef();

    /**
     * createShuffleIOOwnerContext
     *
     * @param taskNameWithSubtaskAndId taskNameWithSubtaskAndId
     * @param executionId executionId
     * @return OmniShuffleIOOwnerContext
     */
    OmniShuffleIOOwnerContext createShuffleIOOwnerContext(
            String taskNameWithSubtaskAndId,
            ExecutionAttemptID executionId);

    /**
     * createResultPartitionWriters
     *
     * @param ownerContext ownerContext
     * @param resultPartitionDeploymentDescriptors resultPartitionDeploymentDescriptors
     * @return List
     */
    List createResultPartitionWriters(
            OmniShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors);
}
