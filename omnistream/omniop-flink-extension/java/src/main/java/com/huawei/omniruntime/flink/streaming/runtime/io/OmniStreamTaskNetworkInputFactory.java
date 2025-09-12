package com.huawei.omniruntime.flink.streaming.runtime.io;

// partial code copied from StreamTaskNetworkInputFactory
// need to clean / re-verify later


import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.recovery.RescalingStreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;

import java.nio.ByteBuffer;
import java.util.function.Function;

/** Factory for {@link StreamTaskNetworkInput} and {@link RescalingStreamTaskNetworkInput}. */
public class OmniStreamTaskNetworkInputFactory {
    /**
     * Factory method for {@link StreamTaskNetworkInput} or {@link RescalingStreamTaskNetworkInput}
     * depending on {@link InflightDataRescalingDescriptor}.
     */
    public static <T> StreamTaskInput<T> create(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            InflightDataRescalingDescriptor rescalingDescriptorinflightDataRescalingDescriptor,
            Function<Integer, StreamPartitioner<?>> gatePartitioners,
            TaskInfo taskInfo,
            long omniStreamTaskRef,
            long omniInputProcessorRef,
            ByteBuffer outputBuffer,
            ByteBuffer outputBufferStatus) {
        return rescalingDescriptorinflightDataRescalingDescriptor.equals(
                InflightDataRescalingDescriptor.NO_RESCALE)
                ? new OmniStreamTaskNetworkInput<>(
                checkpointedInputGate,
                inputSerializer,
                ioManager,
                statusWatermarkValve,
                inputIndex,
                omniStreamTaskRef,
                omniInputProcessorRef,
                outputBuffer,
                outputBufferStatus)
                : new RescalingStreamTaskNetworkInput<>(
                checkpointedInputGate,
                inputSerializer,
                ioManager,
                statusWatermarkValve,
                inputIndex,
                rescalingDescriptorinflightDataRescalingDescriptor,
                gatePartitioners,
                taskInfo);
    }
}
