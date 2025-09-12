package com.huawei.omniruntime.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

/**
 * SinglePartitionWriter
 *
 * @version 1.0.0
 * @since 2025/01/23
 */

public class SinglePartitionWriter implements PartitionWriterDelegate {
    private final ResultPartitionWriter writer;

    public SinglePartitionWriter(ResultPartitionWriter writer) {
        this.writer = writer;
    }


    @Override
    public ResultPartitionWriter getPartitionWriter(int outputIndex) {
        return this.writer;
    }
}
