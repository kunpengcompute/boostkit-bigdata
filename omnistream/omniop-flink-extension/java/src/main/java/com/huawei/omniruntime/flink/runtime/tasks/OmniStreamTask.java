package com.huawei.omniruntime.flink.runtime.tasks;

import com.huawei.omniruntime.flink.runtime.io.network.api.writer.MultiplePartitionWriters;
import com.huawei.omniruntime.flink.runtime.io.network.api.writer.NonPartitionWriter;
import com.huawei.omniruntime.flink.runtime.io.network.api.writer.PartitionWriterDelegate;
import com.huawei.omniruntime.flink.runtime.io.network.api.writer.SinglePartitionWriter;
import com.huawei.omniruntime.flink.streaming.runtime.io.BinaryDataOutput;
import com.huawei.omniruntime.flink.streaming.runtime.task.OmniPartitionExtractor;
import com.huawei.omniruntime.flink.streaming.runtime.task.StreamTaskUtils;
import com.huawei.omniruntime.flink.utils.JsonUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;

import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.*;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.huawei.omniruntime.flink.core.memory.MemoryUtils.getByteBufferAddress;


public abstract class OmniStreamTask<OUT, OP extends StreamOperator<OUT>> extends StreamTask<OUT, OP> {

    protected static final Logger LOG = LoggerFactory.getLogger(OmniStreamTask.class);


    protected long omniTaskRef;
    protected long omniStreamTaskRef;


    // the buffer for native operator chain to put result element's serialized value in
    // the init capacity is can be 1k. It can grow during the process
    protected ByteBuffer outputBuffer;
    protected long outputBufferAddress;   // the address of outputBuffer
    protected int outputBufferCapacity;

    // for JNI to return value, should be changed to directbytebuff later, avoid pass object/array through JNI
    //private int []result = new int[1];

    protected ByteBuffer outputBufferStatus;
    // long address , int capacity, int resultLength (length in bytes) , int num of element
    protected int resultLength;
    protected int numberOfElement;
    protected long statusAddress;
//    List<Long> inputProcessorRefs = new ArrayList<>();
    protected PartitionWriterDelegate partitionWriter;

    protected OmniStreamTask(Environment env) throws Exception {
        super(env);
        this.partitionWriter = createPartitionWriterDelegate(configuration, env);
    }

    protected OmniStreamTask(Environment environment, @Nullable TimerService timerService, Thread.UncaughtExceptionHandler uncaughtExceptionHandler, StreamTaskActionExecutor actionExecutor, TaskMailbox mailbox) throws Exception {
        super(environment, timerService, uncaughtExceptionHandler, actionExecutor, mailbox);
        this.partitionWriter = createPartitionWriterDelegate(configuration, environment);
    }

    protected OmniStreamTask(Environment environment, @Nullable TimerService timerService, Thread.UncaughtExceptionHandler uncaughtExceptionHandler, StreamTaskActionExecutor actionExecutor) throws Exception {
        super(environment, timerService, uncaughtExceptionHandler, actionExecutor);
        this.partitionWriter = createPartitionWriterDelegate(configuration, environment);
    }

    protected OmniStreamTask(Environment environment, @Nullable TimerService timerService, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws Exception {
        super(environment, timerService, uncaughtExceptionHandler);
        this.partitionWriter = createPartitionWriterDelegate(configuration, environment);
    }

    protected OmniStreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
        super(env, timerService);
        this.partitionWriter = createPartitionWriterDelegate(configuration, env);
    }


    public void init() throws Exception {
        initDirectBuffers();
        createNativeOmniStreamTask();
    }


    public PushingAsyncDataInput.DataOutput createBinaryDataOutput(Counter numRecordsIn) {
        return new StreamTaskNetworkBinaryOutput(this.partitionWriter, numRecordsIn);
    }

    public PushingAsyncDataInput.DataOutput createBinaryDataOutput(Counter numRecordsIn, Counter numRecordsOut) {
        return new StreamTaskNetworkBinaryOutput(this.partitionWriter, numRecordsIn, numRecordsOut);
    }

    public void initDirectBuffers() {

        // the init outputBuffer size is 2 times of configured memory segment size
        this.outputBufferCapacity = (int) TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue().getBytes() * 2;
        this.outputBuffer = ByteBuffer.allocateDirect(this.outputBufferCapacity);
        this.outputBufferAddress = getByteBufferAddress(outputBuffer);

        ByteOrder nativeOrder = ByteOrder.nativeOrder();

        if (nativeOrder == ByteOrder.BIG_ENDIAN) {
            System.out.println("Native byte order is big-endian");
        } else if (nativeOrder == ByteOrder.LITTLE_ENDIAN) {
            System.out.println("Native byte order is little-endian");
        } else {
            System.out.println("Unknown byte order");
        }

        /*
        * long address 8, int capacity 4 , int resultLength (length in bytes) 4 , int num of element 4, int output buffer owner 4 (owner =0 java 1 cpp native
        * uintptr_t outputBuffer_;
        * int32_t capacity_;
        * int32_t outputSize; // byte size
        * int32_t numberElement; // number of element_ of outputToOut
        * int32_t ownership;  // 0 outputBuffer owned by java, 1 stands  owned by cpp native
        * */
        this.outputBufferStatus = ByteBuffer.allocateDirect(32);
        this.outputBufferStatus.order(ByteOrder.nativeOrder());
        this.statusAddress = getByteBufferAddress(outputBufferStatus);
        outputBufferStatus.putLong(outputBufferAddress);
        outputBufferStatus.putInt(this.outputBufferCapacity);
        outputBufferStatus.position(20); // owner
        outputBufferStatus.putInt(0);  //output buffer is owned by java
    }



    public void createNativeOmniStreamTask() throws NoSuchFieldException, IllegalAccessException {
        //Omni TNEL
        LOG.info("Max Parallelism: {}, Subtask Index: {}, Number of Parallel Subtasks: {}", this.getEnvironment().getTaskInfo().getMaxNumberOfParallelSubtasks(), this.getEnvironment().getTaskInfo().getIndexOfThisSubtask(), this.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks());
        Iterable<StreamOperatorWrapper<?, ?>> allOperators = operatorChain.getAllOperators();
        String omniOperatorChain = OmniOperatorChain.prepareJSONConfigurationFromAllOps(allOperators, getEnvironment().getJobID());
        String partitionInfo = OmniPartitionExtractor.extractPartitionInfo(getRecordWriter());
        // The string is json string which should be later NTDD, more information than operator chain
        String nativeStreamTaskConfig = JsonUtils.mergeJsonStringsTogether(omniOperatorChain, partitionInfo);
//        String inputChannelInfo = StreamTaskUtils.convertChannelInfoToJson(channelInfos);
//        nativeStreamTaskConfig = JsonUtils.mergeJsonStringsTogether(nativeStreamTaskConfig, inputChannelInfo);


        LOG.info("Calling createNativeStreamTask: statusAddress: {}, outputBufferAddress  {}, outputBufferCapacity {}",
                statusAddress, outputBufferAddress, outputBufferCapacity);
        LOG.info("{},Calling createNativeStreamTask: config: {}",this, nativeStreamTaskConfig);
        try{
            this.omniStreamTaskRef = createNativeStreamTask(nativeStreamTaskConfig, statusAddress, omniTaskRef);

        }catch (Exception e){
            LOG.error("create native stream task error:",e);
            throw e;
        }
        LOG.info("After Calling createNativeStreamTask: nOmniStreamTaskRef: {}",
                omniStreamTaskRef);
    }

    public long getOmniStreamTaskRef() {
        return omniStreamTaskRef;
    }


    public long createNativeOmniInputProcessor(List<InputChannelInfo> channelInfos, int operatorMethodIndicator) {
        String inputChannelInfo = StreamTaskUtils.convertChannelInfoToJson(channelInfos);

        long inputProcessorRef = createNativeOmniInputProcessor(omniStreamTaskRef, inputChannelInfo, operatorMethodIndicator);
        return inputProcessorRef;
    }

    @SuppressWarnings("unchecked")
    public RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> getRecordWriter() {
        try {
            Field recordWritersField = StreamTask.class.getDeclaredField("recordWriter");
            recordWritersField.setAccessible(true);
            return (RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>>) recordWritersField.get(this);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access recordWriter field", e);
        }
    }




    @VisibleForTesting
    public static PartitionWriterDelegate createPartitionWriterDelegate(
            StreamConfig configuration, Environment environment) {
        List<ResultPartitionWriter> partitionWriters =
                createResultPartitionWriters(configuration, environment);
        if (partitionWriters.size() == 1) {
            return new SinglePartitionWriter(partitionWriters.get(0));
        } else if (partitionWriters.size() == 0) {
            return new NonPartitionWriter();
        } else {
            return new MultiplePartitionWriters(partitionWriters);
        }
    }

    private static List<ResultPartitionWriter> createResultPartitionWriters(
            StreamConfig configuration, Environment environment) {
        List<ResultPartitionWriter> partitionWriters =
                new ArrayList<>();
        List<StreamEdge> outEdgesInOrder =
                configuration.getOutEdgesInOrder(
                        environment.getUserCodeClassLoader().asClassLoader());

        for (int i = 0; i < outEdgesInOrder.size(); i++) {
//            StreamEdge edge = outEdgesInOrder.get(i);
            partitionWriters.add(environment.getWriter(i));
        }
        return partitionWriters;
    }


    private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
            StreamEdge edge,
            int outputIndex,
            Environment environment,
            String taskName,
            long bufferTimeout) {

        StreamPartitioner<OUT> outputPartitioner = null;

        // Clones the partition to avoid multiple stream edges sharing the same stream partitioner,
        // like the case of https://issues.apache.org/jira/browse/FLINK-14087.
        try {
            outputPartitioner =
                    InstantiationUtil.clone(
                            (StreamPartitioner<OUT>) edge.getPartitioner(),
                            environment.getUserCodeClassLoader().asClassLoader());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }

        LOG.debug(
                "Using partitioner {} for output {} of task {}",
                outputPartitioner,
                outputIndex,
                taskName);

        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }

        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        .setChannelSelector(outputPartitioner)
                        .setTimeout(bufferTimeout)
                        .setTaskName(taskName)
                        .build(bufferWriter);
        output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
        return output;
    }

    @Override
    public final void cleanUp(Throwable throwable) throws Exception {
        super.cleanUp(throwable); // shutdown should be signalled already
        removeNativeStreamTask(this.omniStreamTaskRef);
        LOG.info("Removed the native stream task at {}", this.omniStreamTaskRef);
    }

    /**
     * The network binary data output directly writing data to result Partition implementation used for processing stream elements from {@link
     * StreamTaskNetworkInput} in one input processor.
     */
    public static class StreamTaskNetworkBinaryOutput implements BinaryDataOutput {

        protected final PartitionWriterDelegate writer;
        protected final Counter numRecordsIn;
        protected final Counter numRecordsOut;


        public StreamTaskNetworkBinaryOutput(PartitionWriterDelegate writer, Counter numRecordsIn) {
            this.writer = writer;
            this.numRecordsIn = numRecordsIn;
            this.numRecordsOut = new SimpleCounter();
        }

        public StreamTaskNetworkBinaryOutput(PartitionWriterDelegate writer, Counter numRecordsIn, Counter numRecordsOut) {
            this.writer = writer;
            this.numRecordsIn = numRecordsIn;
            this.numRecordsOut = numRecordsOut;
        }

        @Override
        public void emitRecord(StreamRecord streamRecord) throws Exception {
            throw new UnsupportedOperationException("not support actually");
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            throw new UnsupportedOperationException("not support actually");
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            throw new UnsupportedOperationException("not support actually");
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            throw new UnsupportedOperationException("not support actually");
        }

//        @Override
//        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
//            throw new UnsupportedOperationException("not support actually");
//        }

        @Override
        public void emitRecord(ByteBuffer record, int targetSubpartition) {
            numRecordsOut.inc();
            try {
                this.writer.getPartitionWriter(0).emitRecord(record, targetSubpartition);
            } catch (Exception e) {
                // LOG.error(e.toString());
            }
        }

        public void numRecordsInInc(long inputNumber) {
            numRecordsIn.inc(inputNumber);
        }

        public void numRecordsOutInc(long outputNumbner) {
            numRecordsOut.inc(outputNumbner);
        }
    }


    public ByteBuffer getOutputBufferStatus() {
        return outputBufferStatus;
    }

    public void setOutputBufferStatus(ByteBuffer outputBufferStatus) {
        this.outputBufferStatus = outputBufferStatus;
    }

    public ByteBuffer getOutputBuffer() {
        return outputBuffer;
    }

    public void setOutputBuffer(ByteBuffer outputBuffer) {
        this.outputBuffer = outputBuffer;
    }

    public long getOutputBufferAddress() {
        return outputBufferAddress;
    }

    public void setOutputBufferAddress(long outputBufferAddress) {
        this.outputBufferAddress = outputBufferAddress;
    }

    public int getOutputBufferCapacity() {
        return outputBufferCapacity;
    }

    public void setOutputBufferCapacity(int outputBufferCapacity) {
        this.outputBufferCapacity = outputBufferCapacity;
    }

    public int getResultLength() {
        return resultLength;
    }

    public void setResultLength(int resultLength) {
        this.resultLength = resultLength;
    }

    public int getNumberOfElement() {
        return numberOfElement;
    }

    public void setNumberOfElement(int numberOfElement) {
        this.numberOfElement = numberOfElement;
    }

    public long getStatusAddress() {
        return statusAddress;
    }

    public void setStatusAddress(long statusAddress) {
        this.statusAddress = statusAddress;
    }



    private static native long createNativeStreamTask(String ntdd, long statusAddress, long omniTaskRef);

    public static native long createNativeOmniInputProcessor(long omniStreamTaskRef, String inputChannelInfo, int operatorMethodIndicator);

    public static native long removeNativeStreamTask(long omniStreamTaskRef);

}
