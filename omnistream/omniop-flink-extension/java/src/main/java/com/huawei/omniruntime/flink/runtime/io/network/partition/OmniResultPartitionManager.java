package com.huawei.omniruntime.flink.runtime.io.network.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmniResultPartitionManager
 *
 * @version 1.0.0
 * @since 2025/04/25
 */

public class OmniResultPartitionManager {
    private static final Logger LOG = LoggerFactory.getLogger(OmniResultPartitionManager.class);

    private long nativeResultPartitionManagerAddress;

    /**
     * registerResultPartition
     *
     * @param partition partition
     */
    public void registerResultPartition(OmniResultPartition partition) {
        long partitionAddress = partition.getNativeResultPartitionAddress();

        LOG.info("Registered {}.", partition);

        registerResultPartitionNative(nativeResultPartitionManagerAddress, partitionAddress);
        /**
         * synchronized (registeredPartitions) {
         * checkState(!isShutdown, "Result partition manager already shut down.");
         *
         * ResultPartition previous =
         * registeredPartitions.put(partition.getPartitionId(), partition);
         *
         * if (previous != null) {
         * throw new IllegalStateException("Result partition already registered.");
         * }
         *
         * LOG.debug("Registered {}.", partition);
         * } **/
    }

    private native void registerResultPartitionNative(
            long nativeResultPartitionManagerAddress,
            long nativePartitionAddress);
}
