package com.huawei.omniruntime.flink.runtime.io.network.partition;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.io.network.buffer.OmniBufferPool;
import com.huawei.omniruntime.flink.runtime.io.network.buffer.OmniBufferPoolShadowFactory;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

/**
 * OmniResultPartition
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public abstract class OmniResultPartition implements ResultPartitionWriter {
    /**
     * LOG
     */
    protected static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

    /**
     * partitionId
     */
    protected final ResultPartitionID partitionId;


    /**
     * Type of this partition. Defines the concrete subpartition implementation to use.
     */
    protected final ResultPartitionType partitionType;

    /**
     * numSubpartitions
     */
    protected final int numSubpartitions;

    /**
     * Native Result Partition Address
     */
    protected long nativeResultPartitionAddress;

    /**
     * Runtime state
     */
    // - Runtime state --------------------------------------------------------
    protected OmniBufferPool bufferPool;

    /**
     * Used to compress buffer to reduce IO.
     */
    protected Counter numBytesOut = new SimpleCounter();

    /**
     * numBuffersOut
     */
    protected Counter numBuffersOut = new SimpleCounter();

    private boolean isFinished;
    private final String owningTaskName;
    private final int partitionIndex;
    private final int numTargetKeyGroups;
    private volatile Throwable cause;

    // do not need in Java Shadow ResultPartition
    // protected final OmniResultPartitionManager partitionManager;
    private final AtomicBoolean isReleased = new AtomicBoolean();

    // Always created by factory
    public OmniResultPartition(
            long nativeResultPartitionAddress,
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups
    ) {
        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(partitionIndex >= 0, "The partition index must be positive.");
        this.partitionIndex = partitionIndex;
        this.partitionId = checkNotNull(partitionId);
        this.partitionType = checkNotNull(partitionType);
        this.numSubpartitions = numSubpartitions;
        this.numTargetKeyGroups = numTargetKeyGroups;
    }

    private static OmniBufferPool getShadowBufferPoolFromNatvie(long nativeResultPartitionAddress) {
        long nativeBufferPoolAddress = getBufferPoolAddress(nativeResultPartitionAddress);
        return OmniBufferPoolShadowFactory.createBufferPool(nativeBufferPoolAddress);
    }

    // for now, get the address first and typeID TBC
    // long [0] address of BufferPool instance address
    // long [1] instance type of BufferPool,
    // type 0:
    // type 1: LocalBufferPool
    private static native long getBufferPoolAddress(long nativeResultPartitionAddress);

    public long getNativeResultPartitionAddress() {
        return nativeResultPartitionAddress;
    }

    /**
     * Registers a buffer pool with this result partition.
     *
     * <p>There is one pool for each result partition, which is shared by all its sub partitions.
     *
     * <p>The pool is registered with the partition *after* it as been constructed in order to
     * conform to the life-cycle of task registrations in the {@link TaskExecutor}.
     *
     * @throws IOException IOException
     */
    @Override
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in result partition setup logic: Already registered buffer pool.");

        setupNative(this.nativeResultPartitionAddress);
        this.bufferPool = getShadowBufferPoolFromNatvie(nativeResultPartitionAddress);
    }

    private native void setupNative(long nativeResultPartitionAddress);

    public String getOwningTaskName() {
        return owningTaskName;
    }

    @Override
    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    public int getNumberOfSubpartitions() {
        return numSubpartitions;
    }

    public OmniBufferPool getBufferPool() {
        return bufferPool;
    }

    /**
     * Returns the type of this result partition.
     *
     * @return result partition type
     */
    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    // ------------------------------------------------------------------------

    @Override
    public void notifyEndOfData(StopMode stopMode) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        throw new UnsupportedOperationException();
    }

    /**
     * The subpartition notifies that the corresponding downstream task have processed all the user
     * records.
     *
     * @param subpartition The index of the subpartition sending the notification.
     * @see EndOfData
     */
    public void onSubpartitionAllDataProcessed(int subpartition) {
    }

    /**
     * Finishes the result partition.
     *
     * <p>After this operation, it is not possible to add further data to the result partition.
     *
     * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
     *
     * @throws IOException IOException
     */
    @Override
    public void finish() throws IOException {
        checkInProduceState();

        isFinished = true;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    /**
     * release
     */
    public void release() {
        release(null);
    }

    @Override
    public void release(Throwable cause) {
        if (isReleased.compareAndSet(false, true)) {
            LOG.debug("{}: Releasing {}.", owningTaskName, this);

            // Set the error cause
            if (cause != null) {
                this.cause = cause;
            }

            releaseInternal();
        }
    }

    /**
     * Releases all produced data including both those stored in memory and persisted on disk.
     */
    protected abstract void releaseInternal();

    private void closeBufferPool() {
        if (bufferPool != null) {
            // TBD
        }
    }

    @Override
    public void close() {
        closeBufferPool();
    }

    @Override
    public void fail(
            @Nullable
            Throwable throwable) {
        // TBD
        // the task canceler thread will call this method to early release the output buffer pool
        closeBufferPool();
    }

    public Throwable getFailureCause() {
        return cause;
    }

    @Override
    public int getNumTargetKeyGroups() {
        return numTargetKeyGroups;
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        numBytesOut = metrics.getNumBytesOutCounter();
        numBuffersOut = metrics.getNumBuffersOutCounter();
    }

    /**
     * Whether this partition is released.
     * <p>A partition is released when each subpartition is either consumed and communication is
     * closed by consumer or failed. A partition is also released if task is cancelled.
     *
     * @return boolean
     */
    @Override
    public boolean isReleased() {
        return isReleased.get();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return bufferPool.getAvailableFuture();
    }

    @Override
    public String toString() {
        return "ResultPartition "
                + partitionId.toString()
                + " ["
                + partitionType
                + ", "
                + numSubpartitions
                + " subpartitions]";
    }

    // ------------------------------------------------------------------------

    /**
     * Notification when a subpartition is released.
     *
     * @param subpartitionIndex subpartitionIndex
     */
    void onConsumedSubpartition(int subpartitionIndex) {
        if (isReleased.get()) {
            return;
        }

        LOG.debug(
                "{}: Received release notification for subpartition {}.", this, subpartitionIndex);
    }

    // ------------------------------------------------------------------------

    /**
     * checkInProduceState
     *
     * @throws IllegalStateException IllegalStateException
     */
    protected void checkInProduceState() throws IllegalStateException {
        checkState(!isFinished, "Partition already finished.");
    }
}
