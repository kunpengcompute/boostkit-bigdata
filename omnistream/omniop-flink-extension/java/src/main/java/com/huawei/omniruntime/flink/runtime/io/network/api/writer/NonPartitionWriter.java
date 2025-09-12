package com.huawei.omniruntime.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

/**
 * NonPartitionWriter
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class NonPartitionWriter implements PartitionWriterDelegate {
    @Override
    public ResultPartitionWriter getPartitionWriter(int outputIndex) {
        throw new UnsupportedOperationException("No partition writer instance.");
    }
}
