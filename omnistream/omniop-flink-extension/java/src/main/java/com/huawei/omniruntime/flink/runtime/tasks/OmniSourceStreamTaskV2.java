package com.huawei.omniruntime.flink.runtime.tasks;

import com.huawei.omniruntime.flink.core.memory.MemoryUtils;
import com.huawei.omniruntime.flink.streaming.runtime.io.BinaryDataOutput;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class OmniSourceStreamTaskV2<OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
        extends OmniStreamTask<OUT, OP> implements NativeStreamTask {

    private static final Logger LOG = LoggerFactory.getLogger(OmniSourceStreamTaskV2.class);

    private final LegacySourceFunctionThread sourceThread;
    private final Object lock;

    private volatile boolean externallyInducedCheckpoints;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    PushingAsyncDataInput.DataOutput output;
    private ByteBuffer resultBuffer;
    private long resultBufferAddress;

    private enum FinishingReason {
        END_OF_DATA(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_DRAIN(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_NO_DRAIN(StopMode.NO_DRAIN);

        private final StopMode stopMode;

        FinishingReason(StopMode stopMode) {
            this.stopMode = stopMode;
        }

        StopMode toStopMode() {
            return this.stopMode;
        }
    }

    /**
     * Indicates whether this Task was purposefully finished, in this case we want to ignore
     * exceptions thrown after finishing, to ensure shutdown works smoothly.
     *
     * <p>Moreover we differentiate drain and no drain cases to see if we need to call finish() on
     * the operators.
     */
    private volatile FinishingReason finishingReason = FinishingReason.END_OF_DATA;

    public OmniSourceStreamTaskV2(Environment env) throws Exception {
        this(env, new Object());
    }

    private OmniSourceStreamTaskV2(Environment env, Object lock) throws Exception {
        super(
                env,
                null,
                FatalExitExceptionHandler.INSTANCE,
                StreamTaskActionExecutor.synchronizedExecutor(lock));
        this.lock = Preconditions.checkNotNull(lock);
        this.sourceThread = new LegacySourceFunctionThread();

        getEnvironment().getMetricGroup().getIOMetricGroup().setEnableBusyTime(false);
    }

    @Override
    public void init() throws Exception {
        super.init();
        // we check if the source is actually inducing the checkpoints, rather
        // than the trigger
        SourceFunction<?> source = mainOperator.getUserFunction();

        Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);
        Counter numRecordsOut = Objects.requireNonNull(operatorChain.getTailOperator()).getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        output = createBinaryDataOutput(numRecordsIn, numRecordsOut);
        ByteBuffer outputBuffer = this.getOutputBuffer();
        resultBufferAddress = MemoryUtils.getByteBufferAddress(outputBuffer);
        resultBuffer = outputBuffer;

        if (source instanceof ExternallyInducedSource) {
            externallyInducedCheckpoints = true;

            ExternallyInducedSource.CheckpointTrigger triggerHook =
                    new ExternallyInducedSource.CheckpointTrigger() {

                        @Override
                        public void triggerCheckpoint(long checkpointId) throws FlinkException {
                            // TODO - we need to see how to derive those. We should probably not
                            // encode this in the
                            // TODO -   source's trigger message, but do a handshake in this task
                            // between the trigger
                            // TODO -   message from the master, and the source's trigger
                            // notification
                            final CheckpointOptions checkpointOptions =
                                    CheckpointOptions.forConfig(
                                            CheckpointType.CHECKPOINT,
                                            CheckpointStorageLocationReference.getDefault(),
                                            configuration.isExactlyOnceCheckpointMode(),
                                            configuration.isUnalignedCheckpointsEnabled(),
                                            configuration.getAlignedCheckpointTimeout().toMillis());
                            final long timestamp = System.currentTimeMillis();

                            final CheckpointMetaData checkpointMetaData =
                                    new CheckpointMetaData(checkpointId, timestamp, timestamp);

                            try {
                                OmniSourceStreamTaskV2.super
                                        .triggerCheckpointAsync(
                                                checkpointMetaData, checkpointOptions)
                                        .get();
                            } catch (RuntimeException e) {
                                throw e;
                            } catch (Exception e) {
                                throw new FlinkException(e.getMessage(), e);
                            }
                        }
                    };

            ((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
        }
        getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .gauge(
                        MetricNames.CHECKPOINT_START_DELAY_TIME,
                        this::getAsyncCheckpointStartDelayNanos);
        recordWriter.setMaxOverdraftBuffersPerGate(0);
    }

    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        operatorChain.getMainOperatorOutput().emitWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    protected void cleanUpInternal() {
        // does not hold any resources, so no cleanup needed
    }

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

        controller.suspendDefaultAction();

        // Against the usual contract of this method, this implementation is not step-wise but
        // blocking instead for
        // compatibility reasons with the current source interface (source functions run as a loop,
        // not in steps).
        sourceThread.setTaskDescription(getName());

        sourceThread.start();

        sourceThread
                .getCompletionFuture()
                .whenComplete(
                        (Void ignore, Throwable sourceThreadThrowable) -> {
                            if (sourceThreadThrowable != null) {
                                mailboxProcessor.reportThrowable(sourceThreadThrowable);
                            } else {
                                mailboxProcessor.suspend();
                            }
                        });
    }

    // called by jni from c++
    public void process() {
        LOG.info("source stream task called from cpp successfully");
        resetOutputBufferAndRecordWriter(getOmniStreamTaskRef());
        readOutputStatus();
        writeProcessedDataToTargetPartition(output);
    }

    private void readOutputStatus() {
        ByteBuffer outputBufferStatus = getOutputBufferStatus();
        outputBufferStatus.position(0);//reset position to the start of the buffer

        long newResultBufferAddress = outputBufferStatus.getLong();
        LOG.info("old resultBufferAddress  = {} newResultBufferAddress  = {}", this.resultBufferAddress, newResultBufferAddress);
        if (newResultBufferAddress != this.resultBufferAddress) {
            this.resultBufferAddress = newResultBufferAddress;
            int capacity = outputBufferStatus.getInt();
            this.resultBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(resultBufferAddress, capacity);
            this.resultBuffer.order(ByteOrder.BIG_ENDIAN);
            outputBufferStatus.position(20); // owner
            outputBufferStatus.putInt(0);//output buffer is owned by java
        }
        outputBufferStatus.position(12);
        this.resultLength = outputBufferStatus.getInt();
        this.numberOfElement = outputBufferStatus.getInt();
    }

    // get outputBuffer ready for reading, now the buffer data structure is like this:
    /*
     * | Header (Fixed Size) | Body (Variable Size) |ChannelNumber (fixSize Size) | Header (Fixed Size) | Body (Variable Size) |ChannelNumber (fixSize Size) |...
     * |----------4B---------|--------N B-----------|----------4B-----------------|----------4B--------|--------N B------------|----------4B-----------------|...
     */

    private void writeProcessedDataToTargetPartition(PushingAsyncDataInput.DataOutput output) {
        resultBuffer.rewind();
        resultBuffer.limit(this.resultLength);
        if (output instanceof BinaryDataOutput) {

            BinaryDataOutput binaryDataOutput = (BinaryDataOutput) output;

            int limit = outputBuffer.limit();
            int elementIdx = 0;
            int newLimit = 0;

            while (elementIdx < limit) {
                int elementSize = resultBuffer.getInt(elementIdx);
                newLimit = elementSize + elementIdx + 4;
                resultBuffer.position(elementIdx);
                int channelNumber = resultBuffer.getInt(newLimit);
                resultBuffer.limit(newLimit);
                binaryDataOutput.emitRecord(resultBuffer, channelNumber);
                elementIdx = newLimit + 4;// bytes for partition
                //reset outputbuffer limit
                resultBuffer.limit(limit);
            }
        } else {
            throw new RuntimeException("output is not type of BinaryDataOutput" + output);
        }
    }

    @Override
    protected void cancelTask() {
        if (stopped.compareAndSet(false, true)) {
            cancelOperator();
        }
    }

    private void cancelOperator() {
        try {
            if (mainOperator != null) {
                //omni
                //mainOperator.cancel();
                cancelStreamSource();
            }
        } finally {
            if (sourceThread.isAlive()) {
                interruptSourceThread();
            } else if (!sourceThread.getCompletionFuture().isDone()) {
                // sourceThread not alive and completion future not done means source thread
                // didn't start and we need to manually complete the future
                sourceThread.getCompletionFuture().complete(null);
            }
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        super.maybeInterruptOnCancel(toInterrupt, taskName, timeout);
        interruptSourceThread();
    }

    private void interruptSourceThread() {
        // Nothing need to do if the source is finished on restore
        if (operatorChain != null && operatorChain.isTaskDeployedAsFinished()) {
            return;
        }

        if (sourceThread.isAlive()) {
            sourceThread.interrupt();
        }
    }

    @Override
    protected CompletableFuture<Void> getCompletionFuture() {
        return sourceThread.getCompletionFuture();
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (!externallyInducedCheckpoints) {
            if (isSynchronousSavepoint(checkpointOptions.getCheckpointType())) {
                return triggerStopWithSavepointAsync(checkpointMetaData, checkpointOptions);
            } else {
                return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
            }
        } else if (checkpointOptions.getCheckpointType().equals(CheckpointType.FULL_CHECKPOINT)) {
            // see FLINK-25256
            throw new IllegalStateException(
                    "Using externally induced sources, we can not enforce taking a full checkpoint."
                            + "If you are restoring from a snapshot in NO_CLAIM mode, please use"
                            + " either CLAIM or LEGACY mode.");
        } else {
            // we do not trigger checkpoints here, we simply state whether we can trigger them
            synchronized (lock) {
                return CompletableFuture.completedFuture(isRunning());
            }
        }
    }

    private boolean isSynchronousSavepoint(SnapshotType snapshotType) {
        return snapshotType.isSavepoint() && ((SavepointType) snapshotType).isSynchronous();
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        mainMailboxExecutor.execute(
                () ->
                        stopOperatorForStopWithSavepoint(
                                checkpointMetaData.getCheckpointId(),
                                ((SavepointType) checkpointOptions.getCheckpointType())
                                        .shouldDrain()),
                "stop legacy source for stop-with-savepoint --drain");
        return sourceThread
                .getCompletionFuture()
                .thenCompose(
                        ignore ->
                                super.triggerCheckpointAsync(
                                        checkpointMetaData, checkpointOptions));
    }

    private void stopOperatorForStopWithSavepoint(long checkpointId, boolean drain) {
        setSynchronousSavepoint(checkpointId);
        finishingReason =
                drain
                        ? FinishingReason.STOP_WITH_SAVEPOINT_DRAIN
                        : FinishingReason.STOP_WITH_SAVEPOINT_NO_DRAIN;
        if (mainOperator != null) {
            //omni
            //mainOperator.stop();
            cancelStreamSource();
        }
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        if (!externallyInducedCheckpoints) {
            super.declineCheckpoint(checkpointId);
        }
    }

    /**
     * Runnable that executes the source function in the head operator.
     */
    private class LegacySourceFunctionThread extends Thread {

        private final CompletableFuture<Void> completionFuture;

        LegacySourceFunctionThread() {
            this.completionFuture = new CompletableFuture<>();
        }

        @Override
        public void run() {
            try {
                if (!operatorChain.isTaskDeployedAsFinished()) {
                    LOG.debug(
                            "Legacy source {} skip execution since the task is finished on restore",
                            getTaskNameWithSubtaskAndId());
                    //omni
                    //mainOperator.run(lock, operatorChain);
                    runStreamSource();
                }
                completeProcessing();
                completionFuture.complete(null);
            } catch (Throwable t) {
                // Note, t can be also an InterruptedException
                if (isCanceled()
                        && ExceptionUtils.findThrowable(t, InterruptedException.class)
                        .isPresent()) {
                    completionFuture.completeExceptionally(new CancelTaskException(t));
                } else {
                    completionFuture.completeExceptionally(t);
                }
            }
        }

        private void completeProcessing() throws InterruptedException, ExecutionException {
            if (!isCanceled() && !isFailing()) {
                mainMailboxExecutor
                        .submit(
                                () -> {
                                    // theoretically the StreamSource can implement BoundedOneInput,
                                    // so we need to call it here
                                    final StopMode stopMode = finishingReason.toStopMode();
                                    if (stopMode == StopMode.DRAIN) {
                                        operatorChain.endInput(1);
                                    }
                                    endData(stopMode);
                                },
                                "SourceStreamTask finished processing data.")
                        .get();
            }
        }

        public void setTaskDescription(final String taskDescription) {
            setName("Legacy Source Thread - " + taskDescription);
        }

        /**
         * @return future that is completed once this thread completes. If this task {@link
         * #isFailing()} and this thread is not alive (e.g. not started) returns a normally
         * completed future.
         */
        CompletableFuture<Void> getCompletionFuture() {
            return isFailing() && !isAlive()
                    ? CompletableFuture.completedFuture(null)
                    : completionFuture;
        }
    }

    void runStreamSource() {
        runStreamSource(getOmniStreamTaskRef());
    }

    void cancelStreamSource() {
        cancelStreamSource(getOmniStreamTaskRef());
    }

    public native long runStreamSource(long omniStreamTaskRef);

    public native long cancelStreamSource(long omniStreamTaskRef);

    public native void resetOutputBufferAndRecordWriter(long omniStreamTaskRef);

    @Override
    public void binkNativeTaskAddress(long nativeTaskAddress) {
        LOG.info("bind native stream task address: {}", nativeTaskAddress);
        this.omniTaskRef = nativeTaskAddress;
    }

//    @Override
//    public void restore() {
//        nativeRestore(this.nativeSourceOperatorStreamTaskAddress);
//    }
//    private native void nativeRestore(long nativeSourceOperatorStreamTaskAddress);

}
