package com.huawei.omniruntime.flink.runtime.shuffle;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * OmniShuffleEnvironmentShadow
 *
 * @since 2025-04-27
 */
public class OmniShuffleEnvironmentShadow implements OmniShuffleEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(OmniShuffleEnvironmentShadow.class);

    private long nativeShuffleEnvironmentAddress;


    public OmniShuffleEnvironmentShadow(long nativeShuffleServiceRef) {
        this.nativeShuffleEnvironmentAddress = nativeShuffleServiceRef;
    }


    public long getNativeShuffleEnvironmentAddress() {
        return nativeShuffleEnvironmentAddress;
    }

    @Override
    public long getNativeShuffleServiceRef() {
        return 0;
    }


    @Override
    public OmniShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID) {
        return new OmniShuffleIOOwnerContext(
                checkNotNull(ownerName),
                checkNotNull(executionAttemptID));
    }


    @Override
    public List createResultPartitionWriters(OmniShuffleIOOwnerContext ownerContext,
                                             List<ResultPartitionDeploymentDescriptor>
                                                     resultPartitionDeploymentDescriptors) {
        // call native implemeantion
        return null;
    }
}
