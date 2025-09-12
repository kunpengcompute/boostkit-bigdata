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

package org.apache.flink.runtime.io.network.partition;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.netty.OmniCreditBasedSequenceNumberingViewReader;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This is
 * in contrast to implementations where records are written to a joint structure, from which the
 * subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 *
 * @since 2025-04-27
 */
public abstract class BufferWritingResultPartition extends ResultPartition {
    /**
     * The subpartitions of this partition. At least one.
     */
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    private final BufferBuilder[] unicastBufferBuilders;

    /**
     * For broadcast mode, a single BufferBuilder is shared by all subpartitions.
     */
    private BufferBuilder broadcastBufferBuilder;

    private TimerGauge backPressuredTimeMsPerSecond = new TimerGauge();

    private long nativeTaskRef = -1;

    private final List<OmniCreditBasedSequenceNumberingViewReader>
            omniCreditBasedSequenceNumberingViewReaderList = new ArrayList<>();
    private long totalWrittenBytes;

    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {
        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.subpartitions = checkNotNull(subpartitions);
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }

    @Override
    public void setup() throws IOException {
        super.setup();
    }

    /**
     * setupInternal
     */
    protected void setupInternal() {
        checkState(
                bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for"
                        + " this result partition.");
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }

    /**
     * flushSubpartition
     *
     * @param targetSubpartition targetSubpartition
     * @param finishProducers finishProducers
     */
    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    /**
     * flushAllSubpartitions
     *
     * @param finishProducers finishProducers
     */
    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        totalWrittenBytes += record.remaining();
        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishUnicastBufferBuilder(targetSubpartition);
            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishUnicastBufferBuilder(targetSubpartition);
        }
        // partial buffer, full record
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }
        // partial buffer, full record
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        checkInProduceState();
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        try (BufferConsumer eventBufferConsumer =
                     EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each channel of targetPartition
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        backPressuredTimeMsPerSecond = metrics.getHardBackPressuredTimePerSecond();
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        checkState(!isReleased(), "Partition released.");

        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

        // bind nativeTaskRef to availabilityListener if it is an instance of OmniCreditBasedSequenceNumberingViewReader
        bindOmniCreditBasedSequenceNumberingViewReaderToSubpartitionView(availabilityListener);

        LOG.debug("Created {}", readView);

        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            } catch (Throwable t) {
                // Catch this in order to ensure that release is called on all subpartitions
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        super.close();
    }

    private BufferBuilder appendUnicastDataForNewRecord(
            final ByteBuffer record, final int targetSubpartition) throws IOException {
        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];

        if (buffer == null) {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            addToSubpartition(buffer, targetSubpartition, 0);
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    private void addToSubpartition(BufferBuilder buffer, int targetSubpartition, int i)
            throws IOException {
        int desirableBufferSize =
                subpartitions[targetSubpartition].add(
                        buffer.createBufferConsumerFromBeginning(), i);

        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            buffer.trim(desirableBufferSize);
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0);
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        createBroadcastBufferConsumers(buffer, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(BufferBuilder buffer, int partialRecordBytes)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            for (ResultSubpartition subpartition : subpartitions) {
                subpartition.add(consumer.copy(), partialRecordBytes);
            }
        }
    }

    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        checkInProduceState();
        ensureUnicastMode();
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition)
            throws IOException {
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
        if (bufferBuilder != null) {
            return bufferBuilder;
        }

        backPressuredTimeMsPerSecond.markStart();
        try {
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            backPressuredTimeMsPerSecond.markEnd();
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    private void finishUnicastBufferBuilder(int targetSubpartition) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            numBytesOut.inc(bufferBuilder.finish());
            numBuffersOut.inc();
            unicastBufferBuilders[targetSubpartition] = null;
            bufferBuilder.close();
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
            finishUnicastBufferBuilder(channelIndex);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            numBytesOut.inc(broadcastBufferBuilder.finish() * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }

    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public TimerGauge getBackPressuredTimeMsPerSecond() {
        return backPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }


    private void bindOmniCreditBasedSequenceNumberingViewReaderToSubpartitionView(
            BufferAvailabilityListener availabilityListener) {
        if (availabilityListener instanceof OmniCreditBasedSequenceNumberingViewReader) {
            ((OmniCreditBasedSequenceNumberingViewReader) availabilityListener).setNativeTaskRef(nativeTaskRef);
            ((OmniCreditBasedSequenceNumberingViewReader) availabilityListener).setTaskName(this.getOwningTaskName());

            omniCreditBasedSequenceNumberingViewReaderList
                    .add((OmniCreditBasedSequenceNumberingViewReader) availabilityListener);
        }
    }

    public long getNativeTaskRef() {
        return nativeTaskRef;
    }

    public void setNativeTaskRef(long nativeTaskRef) {
        this.nativeTaskRef = nativeTaskRef;
    }

    /**
     * stopOmniCreditBasedSequenceNumberingViewReader
     */
    public void stopOmniCreditBasedSequenceNumberingViewReader() {
        int count = 3;
        // LOG.info("------------>stop OmniCreditBasedSequenceNumberingViewReader<-------------------------------");
        while (omniCreditBasedSequenceNumberingViewReaderList.isEmpty() && count > 0) {
            LOG.info("OmniCreditBasedSequenceNumberingViewReader is maybe not created, "
                            + "so wait for them to be created..........for task {}",
                    getOwningTaskName());
            try {
                Thread.sleep(1000);
                count--;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (omniCreditBasedSequenceNumberingViewReaderList.isEmpty()) {
            LOG.info("for this result partition, there is no remote reader view.......... for task {}",
                    getOwningTaskName());
        } else {
            for (OmniCreditBasedSequenceNumberingViewReader omniCreditBasedSequenceNumberingViewReader
                    : omniCreditBasedSequenceNumberingViewReaderList) {
                omniCreditBasedSequenceNumberingViewReader.stop();
            }
        }
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        long totalNumberOfBytes = 0L;

        for (ResultSubpartition subpartition : subpartitions) {
            totalNumberOfBytes += Math.max(0, subpartition.getTotalNumberOfBytesUnsafe());
        }

        return totalWrittenBytes - totalNumberOfBytes;
    }
}
