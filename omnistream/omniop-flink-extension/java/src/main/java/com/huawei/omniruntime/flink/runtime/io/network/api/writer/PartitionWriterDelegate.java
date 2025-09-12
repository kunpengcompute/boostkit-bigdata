package com.huawei.omniruntime.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

/**
 * The partition writer delegate provides the availability function for task processor, and it might
 * represent a single
 * {@link ResultPartitionWriter}
 * or multiple
 * {@link ResultPartitionWriter}
 * instances in specific
 * implementations.
 *
 * @version 1.0.0
 * @since 2025/04/24
 */
public interface PartitionWriterDelegate {
    /**
     * Returns the internal actual partittion writer instance based on the output index.
     *
     * @param outputIndex the index respective to the record writer instance.
     * @return {@link ResultPartitionWriter }
     */
    ResultPartitionWriter getPartitionWriter(int outputIndex);
}
