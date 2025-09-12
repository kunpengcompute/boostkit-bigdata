package com.huawei.omniruntime.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

import java.util.List;

/**
 * MultiplePartitionWriters
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class MultiplePartitionWriters implements PartitionWriterDelegate {
    public MultiplePartitionWriters(List<ResultPartitionWriter> partitionWriters) {
        throw new UnsupportedOperationException("Implemented later.");
    }

    @Override
    public ResultPartitionWriter getPartitionWriter(int outputIndex) {
        throw new UnsupportedOperationException("Implemented later.");
    }
}
