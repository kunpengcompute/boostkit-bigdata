package com.huawei.omniruntime.flink.runtime.api.graph.json.common;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

/**
 * ResultPartitionTypeConverter
 *
 * @version 1.0.0
 * @since 2025/02/20
 */

public class ResultPartitionTypeConverter {
    // Constants for bit masks
    /**
     * PIPELINED_MASK
     */
    public static final int PIPELINED_MASK = 0x01;

    /**
     * BACK_PRESSURE_MASK
     */
    public static final int BACK_PRESSURE_MASK = 0x02;

    /**
     * BOUNDED_MASK
     */
    public static final int BOUNDED_MASK = 0x04;

    /**
     * PERSISTENT_MASK
     */
    public static final int PERSISTENT_MASK = 0x08;

    /**
     * RECONNECTABLE_MASK
     */
    public static final int RECONNECTABLE_MASK = 0x10;

    /**
     * PIPELINE
     */
    public static final int PIPELINE = encode(ResultPartitionType.PIPELINED);

    /**
     * PIPELINED_BOUNDED
     */
    public static final int PIPELINED_BOUNDED = encode(ResultPartitionType.PIPELINED_BOUNDED);

    /**
     * PIPELINED_APPROXIMATE
     */
    public static final int PIPELINED_APPROXIMATE = encode(ResultPartitionType.PIPELINED_APPROXIMATE);

    /**
     * BLOCKING_PERSISTENT
     */
    public static final int BLOCKING_PERSISTENT = encode(ResultPartitionType.BLOCKING_PERSISTENT);

    /**
     * BLOCKING
     */
    public static final int BLOCKING = encode(ResultPartitionType.BLOCKING);

    /**
     * Converts a set of boolean flags representing result partition type features into an integer representation.
     * Each boolean flag corresponds to a specific bit in the resulting integer.
     *
     * @param isPipelined     Indicates whether pipelining is enabled.
     * @param hasBackPressure Indicates whether back pressure is enabled.
     * @param isBounded       Indicates whether the partition is bounded.
     * @param isPersistent    Indicates whether the partition is persistent.
     * @param isReconnectable Indicates whether the partition is reconnectable.
     * @return An integer representing the combined state of the result partition type features.
     * Each bit corresponds to a specific flag (0: false, 1: true).
     */
    public static int encode(
            boolean isPipelined,
            boolean hasBackPressure,
            boolean isBounded,
            boolean isPersistent,
            boolean isReconnectable) {
        int flags = 0;

        if (isPipelined) {
            flags |= PIPELINED_MASK;
        }
        if (hasBackPressure) {
            flags |= BACK_PRESSURE_MASK;
        }
        if (isBounded) {
            flags |= BOUNDED_MASK;
        }
        if (isPersistent) {
            flags |= PERSISTENT_MASK;
        }
        if (isReconnectable) {
            flags |= RECONNECTABLE_MASK;
        }

        return flags;
    }

    /**
     * Converts a ResultPartitionType object into an integer representation of its features.
     *
     * @param partitionType The ResultPartitionType object to encode.
     * @return An integer representing the combined state of the result partition type features.
     * Each bit corresponds to a specific flag (0: false, 1: true).
     */
    public static int encode(ResultPartitionType partitionType) {
        if (partitionType == null) {
            throw new IllegalArgumentException("ResultPartitionType cannot be null."); // Or handle null as you see fit.
        }

        int flags = 0;

        if (partitionType.isPipelinedOrPipelinedBoundedResultPartition()) {
            flags |= PIPELINED_MASK;
        }
        if (true) {
            flags |= BACK_PRESSURE_MASK;
        }
        if (partitionType.isBounded()) {
            flags |= BOUNDED_MASK;
        }
        if (partitionType.isPersistent()) {
            flags |= PERSISTENT_MASK;
        }
        if (false) {
            flags |= RECONNECTABLE_MASK;
        }

        return flags;
    }
}