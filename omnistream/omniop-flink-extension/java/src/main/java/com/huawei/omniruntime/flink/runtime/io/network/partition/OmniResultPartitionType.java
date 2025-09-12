package com.huawei.omniruntime.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

/**
 * OmniResultPartitionType
 *
 * @version 1.0.0
 * @since 2025/04/25
 */
public class OmniResultPartitionType {
    /**
     * BLOCKING
     */
    public final byte BLOCKING = toPartitionType(false, false, false, false, true);

    /**
     * BLOCKING_PERSISTENT
     */
    public final byte BLOCKING_PERSISTENT = toPartitionType(false, false, false, true, true);

    /**
     * PIPELINED
     */
    public final byte PIPELINED = toPartitionType(true, true, false, false, false);

    /**
     * PIPELINED_BOUNDED
     */
    public final byte PIPELINED_BOUNDED = toPartitionType(true, true, true, false, false);

    /**
     * PIPELINED_APPROXIMATE
     */
    public final byte PIPELINED_APPROXIMATE = toPartitionType(true, true, true, false, true);

    /**
     * toPartitionType
     *
     * @param isPipelined isPipelined
     * @param hasBackPressure hasBackPressure
     * @param isBounded isBounded
     * @param isPersistent isPersistent
     * @param isReconnectable isReconnectable
     * @return byte
     */
    public static byte toPartitionType(boolean isPipelined,
                                       boolean hasBackPressure,
                                       boolean isBounded,
                                       boolean isPersistent,
                                       boolean isReconnectable) {
        byte result = 0;

        if (isPipelined) {
            result |= 0b00001; // Set the rightmost bit if b1 is true
        }
        if (hasBackPressure) {
            result |= 0b00010; // Set the second rightmost bit if b2 is true
        }
        if (isBounded) {
            result |= 0b00100; // Set the third rightmost bit if b3 is true
        }
        if (isPersistent) {
            result |= 0b01000; // Set the fourth rightmost bit if b4 is true
        }
        if (isReconnectable) {
            result |= 0b10000; // Set the fifth rightmost bit if b5 is true
        }

        return result;
    }

    /**
     * toPartitionTypeByte
     *
     * @param partitionType partitionType
     * @return byte
     */
    public static byte toPartitionTypeByte(ResultPartitionType partitionType) {
        return toPartitionType(partitionType.isPipelinedOrPipelinedBoundedResultPartition(), true,
                partitionType.isBounded(), partitionType.isPersistent(), false);
    }
}
