package com.huawei.omniruntime.flink.streaming.runtime.io;


// init code copoied from StreamTaskNetworkInput
// need to re-verify later

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.huawei.omniruntime.flink.core.memory.MemoryUtils;
import com.huawei.omniruntime.flink.runtime.tasks.OmniStreamTask;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.util.Preconditions.checkState;


/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link
 * CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link WatermarkStatus} events, and forwards them to event subscribers once the {@link
 * StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or that a
 * {@link WatermarkStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status elements must be protected by synchronizing on the
 * given lock object. This ensures that we don't call methods on a {@link StreamInputProcessor}
 * concurrently with the timer callback or other things.
 */
@Internal
public final class OmniStreamTaskNetworkInput<T>
        extends AbstractStreamTaskNetworkInput<
        T,
        SpillingAdaptiveSpanningRecordDeserializer<
                DeserializationDelegate<StreamElement>>> {

    Logger logger = LoggerFactory.getLogger(OmniStreamTaskNetworkInput.class);

    private Buffer currentBuffer;


    private ByteBuffer outputBuffer;

    private ByteBuffer outputBufferStatus;
    // long address 8, int capacity 4 , int resultLength (length in bytes) 4 , int num of element 4, int owner 4 (owner =0 java 1 cpp native
    private int resultLength;
    private long statusAddress;

    List<InputChannelInfo> channelInfos;

    private final long omniStreamTaskRef;
    private final long omniInputProcessorRef;
    private ByteBuffer resultBuffer;
    private long resultBufferAddress;

    private void readOutputStatus(DataOutput<T> output) {
        outputBufferStatus.position(0);//reset position to the start of the buffer

        long newResultBufferAddress = this.outputBufferStatus.getLong();
        logger.debug("old resultBufferAddress  = {} newResultBufferAddress  = {}", this.resultBufferAddress, newResultBufferAddress);
        if (newResultBufferAddress != this.resultBufferAddress) {
            this.resultBufferAddress = newResultBufferAddress;
            int capacity = this.outputBufferStatus.getInt();
            this.resultBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(resultBufferAddress, capacity);
            this.resultBuffer.order(ByteOrder.BIG_ENDIAN);
            outputBufferStatus.position(20); // owner
            outputBufferStatus.putInt(0);//output buffer is owned by java
        }
        this.outputBufferStatus.position(12);
        this.resultLength = outputBufferStatus.getInt();
    }

    // MORE_AVAILABLE
    private static final int RESULT_STATUS_MORE_AVAILABLE = 0;


    private static final int MASK_DataInputStatus = 0xFF;
    private static final int MASK_BUFFER_CONSUMED = 0xFF00;
    private static final int MASK_FULL_RECORD = 0xFF0000;
    private static final int MASK_BREAK_BATCH_EMITTING = 0xFF000000;
    private static final int AT_LEAST_ONE_RECORD_CONSUMED = 8;

    /**
     * The logger used by the StreamTask and its subclasses.
     */
    protected static final Logger LOG = LoggerFactory.getLogger(OmniStreamTaskNetworkInput.class);


    public OmniStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            long omniStreamTaskRef,
            long omniInputProcessorRef,
            ByteBuffer outputBuffer,
            ByteBuffer outputBufferStatus) {
        super(
                checkpointedInputGate,
                inputSerializer,
                statusWatermarkValve,
                inputIndex,
                getRecordDeserializers(checkpointedInputGate, ioManager)
        );
        this.outputBuffer = outputBuffer;
        this.outputBufferStatus = outputBufferStatus;
        this.omniStreamTaskRef = omniStreamTaskRef;
        this.omniInputProcessorRef = omniInputProcessorRef;
        this.channelInfos = checkpointedInputGate.getChannelInfos();
        resultBufferAddress = MemoryUtils.getByteBufferAddress(outputBuffer);
        resultBuffer = outputBuffer;
    }


    // Initialize one deserializer per input channel
    private static Map<
            InputChannelInfo,
            SpillingAdaptiveSpanningRecordDeserializer<
                    DeserializationDelegate<StreamElement>>>
    getRecordDeserializers(
            CheckpointedInputGate checkpointedInputGate, IOManager ioManager) {
        return checkpointedInputGate.getChannelInfos().stream()
                .collect(
                        toMap(
                                identity(),
                                unused ->
                                        new SpillingAdaptiveSpanningRecordDeserializer<>(
                                                ioManager.getSpillingDirectoriesPaths())));
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        for (Map.Entry<
                InputChannelInfo,
                SpillingAdaptiveSpanningRecordDeserializer<
                        DeserializationDelegate<StreamElement>>>
                e : recordDeserializers.entrySet()) {

            try {
                channelStateWriter.addInputData(
                        checkpointId,
                        e.getKey(),
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        e.getValue().getUnconsumedBuffer());
            } catch (IOException ioException) {
                throw new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, ioException);
            }
        }
        return checkpointedInputGate.getAllBarriersReceivedFuture(checkpointId);
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

        boolean isDataAvailable = false;
        long channelId = 0;
        long segmentAddress = 0;
        int numBytes = 0;
        int segmentOffset = 0;

        while (true) {
            if (isDataAvailable) {
                TNELProcessState result = TNELProcessBuffer(this.omniStreamTaskRef, this.omniInputProcessorRef, segmentAddress, segmentOffset, numBytes, channelId);
                int resultStatus = result.getResultStatus();

                if (output instanceof BinaryDataOutput) {
                    OmniStreamTask.StreamTaskNetworkBinaryOutput binaryDataOutput = (OmniStreamTask.StreamTaskNetworkBinaryOutput) output;
                    long inputNumber = result.getInputNumber();
                    binaryDataOutput.numRecordsInInc(inputNumber);
                } else {
                    throw new RuntimeException("output is not type of BinaryDataOutput" + output);
                }

                isDataAvailable = false;
                if ((resultStatus & MASK_BUFFER_CONSUMED) != 0) {
                    //NetworkBuffer has been consumed, recycle it,actually reduce its ref count
                    if (currentBuffer != null) {
                        currentBuffer.recycleBuffer();
                        currentBuffer = null;
                    }
                }

                //under this condition resultStatus & MASK_FULL_RECORD) != 0,if the last data in the buffer is not a full record,  go to buffer processing,
                // but question is that maybe native method always return partial record,under this case,email does not have chance to run. but does it really happen?
                //another question is for two input stream task, other processor maybe starved
                //so right now solution is to check if at least one record has been consumed, we go back to mailboxprocessor loop
                if ((resultStatus & AT_LEAST_ONE_RECORD_CONSUMED) != 0) {
                    readOutputStatus(output);
                    writeProcessedDataToTargetPartition(output);

                    if ((resultStatus & MASK_BREAK_BATCH_EMITTING) == 0) {
                        //LOG.info("continue");
                        continue;
                        // Short-circuiting to continue processing element in local loop
                    }
                    // otherwise, ask mailbox to continue process element
                    return DataInputStatus.MORE_AVAILABLE;
                }
            }


            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
            if (bufferOrEvent.isPresent()) {
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint
                // mode
                if (bufferOrEvent.get().isBuffer()) {

                    currentBuffer = bufferOrEvent.get().getBuffer();
                    Buffer buffer = currentBuffer;
                    // get the buffer address
                    segmentOffset = buffer.getMemorySegmentOffset();
                    MemorySegment segment = buffer.getMemorySegment();
                    numBytes = buffer.getSize();
                    segmentAddress = segment.getAddress();
                    // get the channel info
                    InputChannelInfo inputChannelInfo = bufferOrEvent.get().getChannelInfo();
                    channelId = (long) inputChannelInfo.getGateIdx() << 32
                            | inputChannelInfo.getInputChannelIdx();

                    isDataAvailable = true;
//                    TNELProcessBuffer(this.nOmniStreamTaskRef, address, offset, numBytes, channelInfo);

                } else {
                    LOG.debug(" processEvent(bufferOrEvent.get());");
                    DataInputStatus status = processEvent(bufferOrEvent.get());
                    LOG.debug(" processEvent(bufferOrEvent.get()); status: {}", status);
                    if (status == DataInputStatus.MORE_AVAILABLE) {
                        continue;
                    }
                    return status;
                }
            } else {
                LOG.debug(" checkpointedInputGate.isFinished();");
                if (checkpointedInputGate.isFinished()) {
                    LOG.debug(" Before checkState");
                    checkState(
                            checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available");
                    LOG.debug(" After checkState");
                    return DataInputStatus.END_OF_INPUT;
                }
                LOG.debug(" return checkpointedInputGate.isFinished();");
                return DataInputStatus.NOTHING_AVAILABLE;
            }
        }//while loop
    }


    @Override
    public void close() throws IOException {
        super.close();

        // cleanup the resources of the checkpointed input gate
        checkpointedInputGate.close();
    }


    //get outputBuffer ready for reading,now the buffer data structure is like this:
    /*
     * | Header (Fixed Size) | Body (Variable Size) |ChannelNumber (fixSize Size) | Header (Fixed Size) | Body (Variable Size) |ChannelNumber (fixSize Size) |...
     * |----------4B---------|--------N B-----------|----------4B-----------------|----------4B--------|--------N B------------|----------4B-----------------|...
     */

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


    // we do not need to pass in the output. during the  TNEL init, the output has been set to corresponding output with
    // record writer
    // result [0] the the size of bytebuffer be consumed.
    // TBD
    // if the bytebuffer for output is not big enough, the JNI will return the new ByteBuffer with serialized value, otherwise
    // the return vaule is null

    private native TNELProcessState TNELProcessBuffer(long omniStreamTaskRef, long omniInputProcessorRef, long address, int offset, int numBytes, long channelInfo);
//    private native int TNELProcessBufferV2(long nOmniStreamTaskRef, long address, int offset, int numBytes, long channelInfo);

//    private native int TNELProcessElement(long nOmniStreamTaskRef);

}
