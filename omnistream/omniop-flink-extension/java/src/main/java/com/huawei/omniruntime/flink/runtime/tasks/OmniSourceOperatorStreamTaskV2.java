package com.huawei.omniruntime.flink.runtime.tasks;

import com.huawei.omniruntime.flink.streaming.runtime.io.OmniStreamTaskSourceInput;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskExternallyInducedSourceInput;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorEventDispatcherImpl;
import org.apache.flink.streaming.runtime.tasks.StreamTaskFinishedOnRestoreSourceInput;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class OmniSourceOperatorStreamTaskV2<T> extends OmniStreamTask<T, SourceOperator<T, ?>> implements NativeStreamTask {

    private static final Logger LOG = LoggerFactory.getLogger(OmniSourceOperatorStreamTaskV2.class);

    private PushingAsyncDataInput.DataOutput output;
    /**
     * Contains information about all checkpoints where RPC from checkpoint coordinator arrives
     * before the source reader triggers it. (Common case)
     */
    private SortedMap<Long, UntriggeredCheckpoint> untriggeredCheckpoints = new TreeMap<>();
    /**
     * Contains the checkpoints that are triggered by the source but the RPC from checkpoint
     * coordinator has yet to arrive. This may happen if the barrier is inserted as an event into
     * the data plane by the source coordinator and the (distributed) source reader reads that event
     * before receiving Flink's checkpoint RPC. (Rare case)
     */
    private SortedSet<Long> triggeredCheckpoints = new TreeSet<>();
    /**
     * Blocks input until the RPC call has been received that corresponds to the triggered
     * checkpoint. This future must only be accessed and completed in the mailbox thread.
     */
    private CompletableFuture<Void> waitForRPC = FutureUtils.completedVoidFuture();
    /** Only set for externally induced sources. See also {@link #isExternallyInducedSource()}. */
    private StreamTaskExternallyInducedSourceInput<T> externallyInducedSourceInput;

    private long nativeSourceOperatorRef;

    public OmniSourceOperatorStreamTaskV2(Environment env) throws Exception {
        super(env);
    }

    @Override
    public void init() throws Exception {
        super.init();
        final SourceOperator<T, ?> sourceOperator = this.mainOperator;
        // reader initialization, which cannot happen in the constructor due to the
        // lazy metric group initialization. We do this here now, rather than
        // later (in open()) so that we can access the reader when setting up the
        // input processors
        sourceOperator.initReader();

        final SourceReader<T, ?> sourceReader = sourceOperator.getSourceReader();
        final StreamTaskInput<T> input;

        if (operatorChain.isTaskDeployedAsFinished()) {
            input = new StreamTaskFinishedOnRestoreSourceInput<>(sourceOperator, 0, 0);
        } else if (sourceReader instanceof ExternallyInducedSourceReader) {
            externallyInducedSourceInput =
                    new StreamTaskExternallyInducedSourceInput<>(
                            sourceOperator,
                            this::triggerCheckpointForExternallyInducedSource,
                            0,
                            0);

            input = externallyInducedSourceInput;
        } else {
            // hook
            long nativeStreamTaskRef = getOmniStreamTaskRef();
            nativeSourceOperatorRef = getNativeSourceOperator(nativeStreamTaskRef);
            ByteBuffer outputBuffer = this.getOutputBuffer();
            ByteBuffer outputBufferStatus = this.getOutputBufferStatus();
            input = new OmniStreamTaskSourceInput<>(sourceOperator, nativeStreamTaskRef, 0, 0, outputBuffer, outputBufferStatus);
        }

        overrideEventHandler();

        // different logic
        Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);
        Counter numRecordsOut = Objects.requireNonNull(operatorChain.getTailOperator()).getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

        output = createBinaryDataOutput(numRecordsIn, numRecordsOut);

        inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);

        getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .gauge(
                        MetricNames.CHECKPOINT_START_DELAY_TIME,
                        this::getAsyncCheckpointStartDelayNanos);
    }

    private List<String> convertToHexStringList(ArrayList<byte[]> splits) {
        List<String> hexStringList = new ArrayList<>();
        for (byte[] bytes : splits) {
            StringBuilder hexString = new StringBuilder();
            for (byte b : bytes) {
                String hex = Integer.toHexString(0xFF & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            hexStringList.add(hexString.toString());
        }
        return hexStringList;
    }

    private String eventToJsonString(OperatorEvent event) throws NoSuchFieldException, IllegalAccessException, JsonProcessingException {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        Map<String, Object> fieldMap = new LinkedHashMap<>();
        jsonMap.put("field", fieldMap);
        if (event instanceof WatermarkAlignmentEvent) {
            jsonMap.put("type", "WatermarkAlignmentEvent");
            fieldMap.put("maxWatermark", ((WatermarkAlignmentEvent) event).getMaxWatermark());
        } else if (event instanceof AddSplitEvent) {
            jsonMap.put("type", "AddSplitEvent");
            Field serializerVersion = AddSplitEvent.class.getDeclaredField("serializerVersion");
            Field splits = AddSplitEvent.class.getDeclaredField("splits");
            serializerVersion.setAccessible(true);
            splits.setAccessible(true);
            fieldMap.put("serializerVersion", serializerVersion.get(event));
            ArrayList<byte[]> splitList = (ArrayList<byte[]>) splits.get(event);
            List<String> hexStringList = convertToHexStringList(splitList);
            fieldMap.put("splits", hexStringList);
        } else if (event instanceof SourceEventWrapper) {
            // TODO currently, do not support HybridSourceReader here
            jsonMap.put("type", "SourceEventWrapper");
        } else if (event instanceof NoMoreSplitsEvent) {
            jsonMap.put("type", "NoMoreSplitsEvent");
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
        return objectMapper.writeValueAsString(jsonMap);
    }

    private void overrideEventHandler() throws NoSuchFieldException, IllegalAccessException {
        OperatorID operatorID = mainOperator.getOperatorID();
        OperatorEventDispatcher operatorEventDispatcher = operatorChain.getOperatorEventDispatcher();
        if (!(operatorEventDispatcher instanceof OperatorEventDispatcherImpl)) {
            throw new IllegalStateException("OperatorEventDispatcher is not an instance of OperatorEventDispatcherImpl");
        }
        Class<? extends OperatorEventDispatcherImpl> dispatcherClass = ((OperatorEventDispatcherImpl) operatorEventDispatcher).getClass();
        Field field = dispatcherClass.getDeclaredField("handlers");
        field.setAccessible(true);
        Map<OperatorID, OperatorEventHandler> handlers = (Map<OperatorID, OperatorEventHandler>)field.get(operatorEventDispatcher);
        handlers.remove(operatorID);
        operatorEventDispatcher.registerEventHandler(operatorID, evt -> {
            String desc = null;
            try {
                desc = eventToJsonString(evt);
            } catch (NoSuchFieldException | IllegalAccessException | JsonProcessingException e) {
                // TODO what kind of exception to throw
                throw new RuntimeException(e.getMessage());
            }
            handleOperatorEvent(nativeSourceOperatorRef, desc);
        });
    }

    private static native long getNativeSourceOperator(long omniStreamTaskRef);

    private static native void handleOperatorEvent(long omniSourceOperatorRef, String eventDesc);

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (!isExternallyInducedSource()) {
            return triggerCheckpointNowAsync(checkpointMetaData, checkpointOptions);
        }
        CompletableFuture<Boolean> triggerFuture = new CompletableFuture<>();
        // immediately move RPC to mailbox so we don't need to synchronize fields
        mainMailboxExecutor.execute(
                () ->
                        triggerCheckpointOnExternallyInducedSource(
                                checkpointMetaData, checkpointOptions, triggerFuture),
                "SourceOperatorStreamTask#triggerCheckpointAsync(%s, %s)",
                checkpointMetaData,
                checkpointOptions);
        return triggerFuture;
    }

    private boolean isExternallyInducedSource() {
        return externallyInducedSourceInput != null;
    }

    private void triggerCheckpointOnExternallyInducedSource(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CompletableFuture<Boolean> triggerFuture) {
        assert (mailboxProcessor.isMailboxThread());
        if (!triggeredCheckpoints.remove(checkpointMetaData.getCheckpointId())) {
            // common case: RPC is received before source reader triggers checkpoint
            // store metadata and options for later
            untriggeredCheckpoints.put(
                    checkpointMetaData.getCheckpointId(),
                    new UntriggeredCheckpoint(checkpointMetaData, checkpointOptions));
            triggerFuture.complete(isRunning());
        } else {
            // trigger already received (rare case)
            FutureUtils.forward(
                    triggerCheckpointNowAsync(checkpointMetaData, checkpointOptions),
                    triggerFuture);

            cleanupOldCheckpoints(checkpointMetaData.getCheckpointId());
        }
    }

    private CompletableFuture<Boolean> triggerCheckpointNowAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (isSynchronous(checkpointOptions.getCheckpointType())) {
            return triggerStopWithSavepointAsync(checkpointMetaData, checkpointOptions);
        } else {
            return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
        }
    }

    private boolean isSynchronous(SnapshotType checkpointType) {
        return checkpointType.isSavepoint() && ((SavepointType) checkpointType).isSynchronous();
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {

        CompletableFuture<Void> operatorFinished = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    setSynchronousSavepoint(checkpointMetaData.getCheckpointId());
                    FutureUtils.forward(
                            mainOperator.stop(
                                    ((SavepointType) checkpointOptions.getCheckpointType())
                                            .shouldDrain()
                                            ? StopMode.DRAIN
                                            : StopMode.NO_DRAIN),
                            operatorFinished);
                },
                "stop Flip-27 source for stop-with-savepoint");

        return operatorFinished.thenCompose(
                (ignore) -> super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions));
    }

    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        output.emitWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        cleanupCheckpoint(checkpointId);
        super.declineCheckpoint(checkpointId);
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(
            long checkpointId, long latestCompletedCheckpointId) {
        mainMailboxExecutor.execute(
                () -> cleanupCheckpoint(checkpointId), "Cleanup checkpoint %d", checkpointId);
        return super.notifyCheckpointAbortAsync(checkpointId, latestCompletedCheckpointId);
    }

    @Override
    public Future<Void> notifyCheckpointSubsumedAsync(long checkpointId) {
        mainMailboxExecutor.execute(
                () -> cleanupCheckpoint(checkpointId), "Cleanup checkpoint %d", checkpointId);
        return super.notifyCheckpointSubsumedAsync(checkpointId);
    }

    // --------------------------

    private void triggerCheckpointForExternallyInducedSource(long checkpointId) {
        UntriggeredCheckpoint untriggeredCheckpoint = untriggeredCheckpoints.remove(checkpointId);
        if (untriggeredCheckpoint != null) {
            // common case: RPC before external sources induces it
            triggerCheckpointNowAsync(
                    untriggeredCheckpoint.getMetadata(),
                    untriggeredCheckpoint.getCheckpointOptions());
            cleanupOldCheckpoints(checkpointId);
        } else {
            // rare case: external source induced first
            triggeredCheckpoints.add(checkpointId);
            if (waitForRPC.isDone()) {
                waitForRPC = new CompletableFuture<>();
                externallyInducedSourceInput.blockUntil(waitForRPC);
            }
        }
    }

    /**
     * Cleanup any orphaned checkpoint before the given currently triggered checkpoint. These
     * checkpoint may occur when the checkpoint is cancelled but the RPC is lost. Note, to be safe,
     * checkpoint X is only removed when both RPC and trigger for a checkpoint Y>X is received.
     */
    private void cleanupOldCheckpoints(long checkpointId) {
        assert (mailboxProcessor.isMailboxThread());
        triggeredCheckpoints.headSet(checkpointId).clear();
        untriggeredCheckpoints.headMap(checkpointId).clear();

        maybeResumeProcessing();
    }

    /** Resumes processing if it was blocked before or else is a no-op. */
    private void maybeResumeProcessing() {
        assert (mailboxProcessor.isMailboxThread());

        if (triggeredCheckpoints.isEmpty()) {
            waitForRPC.complete(null);
        }
    }

    /** Remove temporary data about a canceled checkpoint. */
    private void cleanupCheckpoint(long checkpointId) {
        assert (mailboxProcessor.isMailboxThread());
        triggeredCheckpoints.remove(checkpointId);
        untriggeredCheckpoints.remove(checkpointId);

        maybeResumeProcessing();
    }

    // ---------------------------

    /** Implementation of {@link PushingAsyncDataInput.DataOutput} that wraps a specific {@link Output}. */
    public static class AsyncDataOutputToOutput<T> implements PushingAsyncDataInput.DataOutput<T> {

        private final Output<StreamRecord<T>> output;
        private final InternalSourceReaderMetricGroup metricGroup;
        @Nullable
        private final WatermarkGauge inputWatermarkGauge;
        private final Counter numRecordsOut;

        public AsyncDataOutputToOutput(
                Output<StreamRecord<T>> output,
                InternalSourceReaderMetricGroup metricGroup,
                @Nullable WatermarkGauge inputWatermarkGauge) {

            this.output = checkNotNull(output);
            this.numRecordsOut = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
            this.inputWatermarkGauge = inputWatermarkGauge;
            this.metricGroup = metricGroup;
        }

        @Override
        public void emitRecord(StreamRecord<T> streamRecord) {
            numRecordsOut.inc();
            metricGroup.recordEmitted(streamRecord.getTimestamp());
            output.collect(streamRecord);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            output.emitLatencyMarker(latencyMarker);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            long watermarkTimestamp = watermark.getTimestamp();
            if (inputWatermarkGauge != null) {
                inputWatermarkGauge.setCurrentWatermark(watermarkTimestamp);
            }
            metricGroup.watermarkEmitted(watermarkTimestamp);
            output.emitWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            output.emitWatermarkStatus(watermarkStatus);
        }
    }

    private static class UntriggeredCheckpoint {
        private final CheckpointMetaData metadata;
        private final CheckpointOptions checkpointOptions;

        private UntriggeredCheckpoint(
                CheckpointMetaData metadata, CheckpointOptions checkpointOptions) {
            this.metadata = metadata;
            this.checkpointOptions = checkpointOptions;
        }

        public CheckpointMetaData getMetadata() {
            return metadata;
        }

        public CheckpointOptions getCheckpointOptions() {
            return checkpointOptions;
        }
    }



    @Override
    public void binkNativeTaskAddress(long nativeTaskAddress) {
        LOG.info("bind native stream task address: {}", nativeTaskAddress);
        this.omniTaskRef = nativeTaskAddress;
    }
}
