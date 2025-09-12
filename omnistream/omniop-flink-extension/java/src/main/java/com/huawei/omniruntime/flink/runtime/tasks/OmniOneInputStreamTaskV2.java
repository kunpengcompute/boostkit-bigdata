package com.huawei.omniruntime.flink.runtime.tasks;

import com.huawei.omniruntime.flink.streaming.runtime.io.OmniStreamTaskNetworkInputFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.SortingDataInput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.streaming.api.graph.StreamConfig.requiresSorting;
import static org.apache.flink.util.Preconditions.checkState;


public class OmniOneInputStreamTaskV2<IN, OUT> extends OmniStreamTask<OUT, OneInputStreamOperator<IN, OUT>> implements NativeStreamTask {

    private static final Logger LOG = LoggerFactory.getLogger(OmniOneInputStreamTaskV2.class);

    @Nullable
    private CheckpointBarrierHandler checkpointBarrierHandler;

    protected final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

    public OmniOneInputStreamTaskV2(Environment env) throws Exception {
        super(env);
    }

    @Override
    public void init() throws Exception {
        super.init();
        StreamConfig configuration = getConfiguration();
        int numberOfInputs = configuration.getNumberOfNetworkInputs();

        if (numberOfInputs > 0) {
            CheckpointedInputGate inputGate = createCheckpointedInputGate();
            Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);
            Counter numRecordsOut = Objects.requireNonNull(operatorChain.getTailOperator()).getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

            List<InputChannelInfo> channelInfos = inputGate.getChannelInfos();


            //create OmniInputProcessor
            long inputProcessorRef = this.createNativeOmniInputProcessor(channelInfos, 0);
            PushingAsyncDataInput.DataOutput<IN> output = createBinaryDataOutput(numRecordsIn, numRecordsOut);

            StreamTaskInput<IN> input = createTaskInput(inputGate, omniStreamTaskRef, inputProcessorRef);


            StreamConfig.InputConfig[] inputConfigs =
                    configuration.getInputs(getUserCodeClassLoader());
            StreamConfig.InputConfig inputConfig = inputConfigs[0];
            if (requiresSorting(inputConfig)) {
                checkState(
                        !configuration.isCheckpointingEnabled(),
                        "Checkpointing is not allowed with sorted inputs.");
                input = wrapWithSorted(input);
            }

            getEnvironment()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .reuseRecordsInputCounter(numRecordsIn);

            inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);
        }
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge);
        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge::getValue);

    }

    private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate, long omniStreamTaskRef, long omniInputProcessorRef) {
        StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(inputGate.getNumberOfInputChannels());

        TypeSerializer<IN> inSerializer =
                configuration.getTypeSerializerIn1(getUserCodeClassLoader());

        return OmniStreamTaskNetworkInputFactory.create(
                inputGate,
                inSerializer,
                getEnvironment().getIOManager(),
                statusWatermarkValve,
                0,
                getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                gateIndex ->
                        configuration
                                .getInPhysicalEdges(getUserCodeClassLoader())
                                .get(gateIndex)
                                .getPartitioner(),
                getEnvironment().getTaskInfo(),
                omniStreamTaskRef,
                omniInputProcessorRef,
                this.getOutputBuffer(),
                this.getOutputBufferStatus());
    }

    private StreamTaskInput<IN> wrapWithSorted(StreamTaskInput<IN> input) {
        ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        return new SortingDataInput<>(
                input,
                configuration.getTypeSerializerIn(input.getInputIndex(), userCodeClassLoader),
                configuration.getStateKeySerializer(userCodeClassLoader),
                configuration.getStatePartitioner(input.getInputIndex(), userCodeClassLoader),
                getEnvironment().getMemoryManager(),
                getEnvironment().getIOManager(),
                getExecutionConfig().isObjectReuseEnabled(),
                configuration.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        getJobConfiguration(),
                        userCodeClassLoader),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                this,
                getExecutionConfig());
    }

    protected CheckpointedInputGate createCheckpointedInputGate() {
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        checkpointBarrierHandler =
                InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        configuration,
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        new List[]{Arrays.asList(inputGates)},
                        Collections.emptyList(),
                        mainMailboxExecutor,
                        systemTimerService);

        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        new List[]{Arrays.asList(inputGates)},
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration);

        return Arrays.asList(checkpointedInputGates).get(0);
    }

    @Override
    public void binkNativeTaskAddress(long nativeTaskAddress) {
        LOG.info("bind native stream task address: {}", nativeTaskAddress);
        this.omniTaskRef = nativeTaskAddress;
    }

}
