package com.huawei.omniruntime.flink.streaming.runtime.io;

import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface BinaryDataOutput extends PushingAsyncDataInput.DataOutput {
    /** Writes the given serialized record to the target subpartition. */
    void emitRecord(ByteBuffer record, int targetSubpartition);
}