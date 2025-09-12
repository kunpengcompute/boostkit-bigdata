package com.huawei.omniruntime.flink.runtime.tasks;

import com.huawei.omniruntime.flink.streaming.runtime.io.OmniStreamTwoInputProcessorFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

public class OmniTwoInputStreamTaskV2<IN1, IN2, OUT> extends OmniStreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> implements NativeStreamTask {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTwoInputStreamTaskV2.class);

    @Nullable
    private CheckpointBarrierHandler checkpointBarrierHandler;
    protected final WatermarkGauge input1WatermarkGauge;
    protected final WatermarkGauge input2WatermarkGauge;
    protected final MinWatermarkGauge minInputWatermarkGauge;

    public OmniTwoInputStreamTaskV2(Environment env) throws Exception {
        super(env);

        input1WatermarkGauge = new WatermarkGauge();
        input2WatermarkGauge = new WatermarkGauge();
        minInputWatermarkGauge = new MinWatermarkGauge(input1WatermarkGauge, input2WatermarkGauge);
    }

    @Override
    public void init() throws Exception {
        super.init();

        StreamConfig configuration = getConfiguration();
        ClassLoader userClassLoader = getUserCodeClassLoader();

        int numberOfInputs = configuration.getNumberOfNetworkInputs();

        ArrayList<IndexedInputGate> inputList1 = new ArrayList<>();
        ArrayList<IndexedInputGate> inputList2 = new ArrayList<>();

        List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

        for (int i = 0; i < numberOfInputs; i++) {
            int inputType = inEdges.get(i).getTypeNumber();
            IndexedInputGate reader = getEnvironment().getInputGate(i);
            switch (inputType) {
                case 1:
                    inputList1.add(reader);
                    break;
                case 2:
                    inputList2.add(reader);
                    break;
                default:
                    throw new RuntimeException("Invalid input type number: " + inputType);
            }
        }

        createInputProcessor(
                inputList1, inputList2, gateIndex -> inEdges.get(gateIndex).getPartitioner());

        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_1_WATERMARK, input1WatermarkGauge);
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_2_WATERMARK, input2WatermarkGauge);
        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
    }


    protected void createInputProcessor(
            List<IndexedInputGate> inputGates1,
            List<IndexedInputGate> inputGates2,
            Function<Integer, StreamPartitioner<?>> gatePartitioners) {

        // create an input instance for each input
        checkpointBarrierHandler =
                InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        configuration,
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        new List[]{inputGates1, inputGates2},
                        Collections.emptyList(),
                        mainMailboxExecutor,
                        systemTimerService);
        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        new List[]{inputGates1, inputGates2},
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration);

        checkState(checkpointedInputGates.length == 2);


        long leftProcessorRef = this.createNativeOmniInputProcessor(checkpointedInputGates[0].getChannelInfos(), 1);
        long rightProcessorRef = this.createNativeOmniInputProcessor(checkpointedInputGates[1].getChannelInfos(), 2);


        //before create inputProcessor, we need to create omni native task


        // Create OmniStreamTaskNetworkInputFactory
        inputProcessor = OmniStreamTwoInputProcessorFactory.create(
                this,
                checkpointedInputGates,
                getEnvironment().getIOManager(),
                getEnvironment().getMemoryManager(),
                getEnvironment().getMetricGroup().getIOMetricGroup(),
                mainOperator,
                input1WatermarkGauge,
                input2WatermarkGauge,
                operatorChain,
                getConfiguration(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getJobConfiguration(),
                getExecutionConfig(),
                getUserCodeClassLoader(),
                setupNumRecordsInCounter(mainOperator),
                getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                gatePartitioners,
                getEnvironment().getTaskInfo(),
                leftProcessorRef,
                rightProcessorRef,
                this);
    }

    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.ofNullable(this.checkpointBarrierHandler);
    }

    @Override
    public void binkNativeTaskAddress(long nativeTaskAddress) {
        LOG.info("bind native task address: {}", nativeTaskAddress);
        this.omniTaskRef = nativeTaskAddress;
    }
}
