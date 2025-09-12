package com.huawei.omniruntime.flink.streaming.runtime.io;

import com.huawei.omniruntime.flink.runtime.tasks.OmniStreamTask;
import com.huawei.omniruntime.flink.core.memory.MemoryUtils;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link StreamTaskInput} that reads data from the {@link SourceOperator} and
 * returns the {@link DataInputStatus} to indicate whether the source state is available,
 * unavailable or finished.
 */
public class OmniStreamTaskSourceInput<T> implements StreamTaskInput<T>, CheckpointableInput {

    Logger logger = LoggerFactory.getLogger(OmniStreamTaskSourceInput.class);

    private final SourceOperator<T, ?> operator;
    private final long nativeStreamTaskRef;
    private final int inputGateIndex;
    private final AvailabilityHelper isBlockedAvailability = new AvailabilityHelper();
    private final List<InputChannelInfo> inputChannelInfos;
    private final int inputIndex;

    private ByteBuffer outputBufferStatus;
    private ByteBuffer resultBuffer;
    private long resultBufferAddress;
    private int resultLength;


    public OmniStreamTaskSourceInput(
            SourceOperator<T, ?> operator, long nativeStreamTaskRef, int inputGateIndex, int inputIndex, ByteBuffer outputBuffer, ByteBuffer outputBufferStatus) {
        this.operator = checkNotNull(operator);
        this.nativeStreamTaskRef = checkNotNull(nativeStreamTaskRef);
        this.inputGateIndex = inputGateIndex;
        inputChannelInfos = Collections.singletonList(new InputChannelInfo(inputGateIndex, 0));
        isBlockedAvailability.resetAvailable();
        this.inputIndex = inputIndex;

        this.resultBuffer = outputBuffer;
        this.resultBufferAddress = MemoryUtils.getByteBufferAddress(outputBuffer);
        this.outputBufferStatus = outputBufferStatus;
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        /**
         * Safe guard against best efforts availability checks. If despite being unavailable someone
         * polls the data from this source while it's blocked, it should return {@link
         * DataInputStatus.NOTHING_AVAILABLE}.
         */
        if (isBlockedAvailability.isApproximatelyAvailable()) {
            // hook data
            int resultStatus = emitNextNative(nativeStreamTaskRef);

            switch (resultStatus) {
                case 0: // MORE_AVAILABLE
                    readOutputStatus(output);
                    writeProcessedDataToTargetPartition(output);
                    return DataInputStatus.MORE_AVAILABLE;
                case 1:
                    return DataInputStatus.NOTHING_AVAILABLE;
                case 2:
                    return DataInputStatus.END_OF_RECOVERY;
                case 3:
                    return DataInputStatus.STOPPED;
                case 4:
                    return DataInputStatus.END_OF_DATA;
                case 5:
                    return DataInputStatus.END_OF_INPUT;
                default:
                    throw new Exception("error");
            }
        }

        return DataInputStatus.NOTHING_AVAILABLE;


    }

    private static native int emitNextNative(long nativeStreamTaskRef);

    private void readOutputStatus(DataOutput<T> output) {
        outputBufferStatus.position(0);//reset position to the start of the buffer

        long newResultBufferAddress = this.outputBufferStatus.getLong();
        logger.debug("old resultBufferAddress  = {} newResultBufferAddress  = {}", this.resultBufferAddress, newResultBufferAddress);
        if (newResultBufferAddress != this.resultBufferAddress) {
            this.resultBufferAddress = newResultBufferAddress;
            int capacity = this.outputBufferStatus.getInt();
            this.resultBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(resultBufferAddress,capacity);
            this.resultBuffer.order(ByteOrder.BIG_ENDIAN);
            outputBufferStatus.position(20); // owner
            outputBufferStatus.putInt(0);//output buffer is owned by java
        }
        this.outputBufferStatus.position(12);
        this.resultLength = outputBufferStatus.getInt();
    }

    private void writeProcessedDataToTargetPartition(DataOutput<T> output) throws Exception {
        resultBuffer.rewind();
        resultBuffer.limit(this.resultLength);
        if (output instanceof BinaryDataOutput) {

            BinaryDataOutput binaryDataOutput = (BinaryDataOutput) output;

            int limit = this.resultBuffer.limit();
            int elementIdx = 0;
            int newLimit = 0;

            while (elementIdx < limit) {
                int elementSize = this.resultBuffer.getInt(elementIdx);
                newLimit = elementSize + elementIdx + 4;
                this.resultBuffer.position(elementIdx);
                int channelNumber = resultBuffer.getInt(newLimit);
                this.resultBuffer.limit(newLimit);
                binaryDataOutput.emitRecord(this.resultBuffer, channelNumber);
                elementIdx = newLimit + 4;// bytes for partition
                //reset outputbuffer limit
                resultBuffer.limit(limit);
            }
        } else {
            throw new RuntimeException("output is not type of BinaryDataOutput" + output);
        }

    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        // jni: get SourceReader is available
        boolean available = getSourceReaderAvailableNative(nativeStreamTaskRef);
        if (available) {
            return isBlockedAvailability.getAvailableFuture();
        } else {
            return isBlockedAvailability.and(operator);
        }
    }

    public static native boolean getSourceReaderAvailableNative(long nativeStreamTaskRef);

    @Override
    public void blockConsumption(InputChannelInfo channelInfo) {
        isBlockedAvailability.resetUnavailable();
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) {
        isBlockedAvailability.getUnavailableToResetAvailable().complete(null);
    }

    @Override
    public List<InputChannelInfo> getChannelInfos() {
        return inputChannelInfos;
    }

    @Override
    public int getNumberOfInputChannels() {
        return inputChannelInfos.size();
    }

    /**
     * This method is used with unaligned checkpoints to mark the arrival of a first {@link
     * CheckpointBarrier}. For chained sources, there is no {@link CheckpointBarrier} per se flowing
     * through the job graph. We can assume that an imaginary {@link CheckpointBarrier} was produced
     * by the source, at any point of time of our choosing.
     *
     * <p>We are choosing to interpret it, that {@link CheckpointBarrier} for sources was received
     * immediately as soon as we receive either checkpoint start RPC, or {@link CheckpointBarrier}
     * from a network input. So that we can checkpoint state of the source and all of the other
     * operators at the same time.
     *
     * <p>Also we are choosing to block the source, as a best effort optimisation as: - either there
     * is no backpressure and the checkpoint "alignment" will happen very quickly anyway - or there
     * is a backpressure, and it's better to prioritize processing data from the network to speed up
     * checkpointing. From the cluster resource utilisation perspective, by blocking chained source
     * doesn't block any resources from being used, as this task running the source has a backlog of
     * buffered input data waiting to be processed.
     *
     * <p>However from the correctness point of view, {@link #checkpointStarted(CheckpointBarrier)}
     * and {@link #checkpointStopped(long)} methods could be empty no-op.
     */
    @Override
    public void checkpointStarted(CheckpointBarrier barrier) {
        blockConsumption(null);
    }

    @Override
    public void checkpointStopped(long cancelledCheckpointId) {
        resumeConsumption(null);
    }

    @Override
    public int getInputGateIndex() {
        return inputGateIndex;
    }

    @Override
    public void convertToPriorityEvent(int channelIndex, int sequenceNumber) throws IOException {}

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public void close() {
        // SourceOperator is closed via OperatorChain
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        return CompletableFuture.completedFuture(null);
    }

    public OperatorID getOperatorID() {
        return operator.getOperatorID();
    }

    public SourceOperator<T, ?> getOperator() {
        return operator;
    }
}