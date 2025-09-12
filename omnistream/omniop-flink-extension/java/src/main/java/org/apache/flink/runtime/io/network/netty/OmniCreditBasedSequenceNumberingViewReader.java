package org.apache.flink.runtime.io.network.netty;

import static com.huawei.omniruntime.flink.core.memory.MemoryUtils.getByteBufferAddress;
import static org.apache.flink.runtime.io.network.api.StopMode.DRAIN;

import com.huawei.omniruntime.flink.core.memory.MemoryUtils;
import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.ResultPartitionIDPOJO;
import com.huawei.omniruntime.flink.runtime.io.network.buffer.NativeBufferRecycler;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OmniCreditBasedSequenceNumberingViewReader
 *
 * @since 2025-04-27
 */
public class OmniCreditBasedSequenceNumberingViewReader
        extends CreditBasedSequenceNumberingViewReader implements Runnable {
    private static final EndOfData END_OF_DATA = new EndOfData(DRAIN);
    private static final Logger LOG = LoggerFactory.getLogger(OmniCreditBasedSequenceNumberingViewReader.class);

    private long nativeTaskRef = -1;
    private volatile long nativeCreditBasedSequenceNumberingViewReaderRef = -1;
    private String taskName;
    private final Object requestLock = new Object();

    private ByteBuffer outputBuffer;
    private long outputBufferAddress;   // the address of outputBuffer
    private int outputBufferCapacity;
    private long statusAddress;

    private volatile boolean running = true;

    AtomicInteger sequenceNumber = new AtomicInteger(0);

    // for JNI to return value, should be changed to directbytebuff later, avoid pass object/array through JNI

    private ByteBuffer outputBufferStatus;
    private List<BufferInfo> bufferInfos = new ArrayList<>();

    private int subPartitionIndex;
    private String partitionId;
    private volatile boolean flushed = false;
    private volatile int numCreditsAvailable;
    private volatile int initialCredit;
    private Executor executor = Executors.newSingleThreadExecutor();

    OmniCreditBasedSequenceNumberingViewReader(
            InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
        super(receiverId, initialCredit, requestQueue);
        this.initialCredit = initialCredit;
        numCreditsAvailable = initialCredit;
        initDirectBuffers();
    }

    /**
     * initDirectBuffers
     */
    public void initDirectBuffers() {
        // the init outputBuffer size  with an initial size of 128
        // [ elementNum(default 10) * (elementAddress + elementLength)  10 * (8+4) = 120 powerOfTwo(120) = 128 ]
        // in the future maybe we can change the size of outputBuffer dynamically,
        // but  for the vectorBatch data type scenario,
        // the fixed size of outputBuffer  is not a problem
        this.outputBufferCapacity = 128;
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

        // long address 8, int capacity 4 ,
        // int resultLength (length in bytes) 4 ,
        // int num of element 4, int output buffer owner 4 (owner =0 java 1 cpp native
        this.outputBufferStatus = ByteBuffer.allocateDirect(32);
        this.outputBufferStatus.order(ByteOrder.nativeOrder());
        this.statusAddress = getByteBufferAddress(outputBufferStatus);
        outputBufferStatus.putLong(outputBufferAddress);
        outputBufferStatus.putInt(this.outputBufferCapacity);
        outputBufferStatus.position(20); // owner
        outputBufferStatus.putInt(0);  // output buffer is owned by java
    }

    public void getNativeReaderRef() {
        executor.execute(() -> {
            long count = 0L;
            while (nativeCreditBasedSequenceNumberingViewReaderRef == -1) {
                try {
                    count++;
                    LOG.info("count num is {}", count);
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    LOG.error("Error sleeping", e);
                }
            }

            firstDataAvailableNotification(nativeCreditBasedSequenceNumberingViewReaderRef);
            LOG.info("call first notification  for task: {} ## {} successfully..........", taskName.substring(0,
                    15), subPartitionIndex);
        });
    }

    @Override
    public void requestSubpartitionView(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            int subPartitionIndex)
            throws IOException {
        synchronized (requestLock) {
            LOG.info("start requestSubpartitionView");
            try {
                super.requestSubpartitionView(partitionProvider, resultPartitionId, subPartitionIndex);

            ResultPartitionIDPOJO resultPartitionIDPOJO = new ResultPartitionIDPOJO(resultPartitionId);
            JSONObject jsonObject = new JSONObject(resultPartitionIDPOJO);
            String parititonIdString = jsonObject.toString();
            this.subPartitionIndex = subPartitionIndex;
            this.partitionId = parititonIdString;
//            Thread.currentThread().setName("Netty Server for task: " + taskName);


                LOG.info("requestSubpartitionView for task: {} ## {}", taskName.substring(0, 15), subPartitionIndex);
                nativeCreditBasedSequenceNumberingViewReaderRef = createNativeCreditBasedSequenceNumberingViewReader(
                        nativeTaskRef, statusAddress, partitionId, subPartitionIndex);
                LOG.info("requestSubpartitionView for task: {} ## {} create result = {},{}",
                        taskName.substring(0, 15),
                        subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef,
                        this.hashCode());
            } catch (Exception e) {
                LOG.warn("Error in requestSubpartitionView, but we let it go", e);
                throw new PartitionNotFoundException(resultPartitionId);
            } catch (Error e) {
                LOG.error("Error in requestSubpartitionView, but we let it go", e);
                throw new PartitionNotFoundException(resultPartitionId);
            }
            Thread thread = new Thread(this);
            thread.start();

            getNativeReaderRef();
        }
        // start the thread
    }


    @Override
    public void run() {
        Thread.currentThread().setName("OmniCreditBasedSequenceNumberingViewReader::::["
                                        + taskName.substring(0, 15) + "##]" + subPartitionIndex);
        int count = 0;
        while (running) {
            try {
                if (nativeCreditBasedSequenceNumberingViewReaderRef != -1) {
                    if (!flushed && (numCreditsAvailable - bufferInfos.size()) > 0) {
                        int res = checkIfDataAvailableAndNotifyNetty();
                        LOG.info("nativeCreditBasedSequenceNumberingViewReaderRef can flush: " + "{} ## {},"
                                + " got " + "data" + " size = {} and numCreditsAvailable = {},native ref = "
                                + "{}", taskName.substring(0, 15), subPartitionIndex, res,
                                numCreditsAvailable, nativeCreditBasedSequenceNumberingViewReaderRef);
                        if (res == 0) {
                            Thread.sleep(20);
                        }
                    } else {
                        count++;
                        if (count % 500 == 0) {
                            LOG.info("nativeCreditBasedSequenceNumberingViewReaderRef can not flush: {} "
                                    + "##{}, numCreditsAvailable = {}, bufferInfos size = {} and "
                                    + "flushed= {} and native " + "ref = {}", taskName.substring(0,
                                    15), subPartitionIndex, numCreditsAvailable, bufferInfos.size(),
                                    flushed, nativeCreditBasedSequenceNumberingViewReaderRef);
                            count = 0;
                        }
                        Thread.sleep(20);
                    }
                } else {
                    LOG.error("nativeCreditBasedSequenceNumberingViewReaderRef is -1 for task: {} ## {},{},{}",
                            taskName.substring(0, 15), subPartitionIndex, this.hashCode(),
                            this.nativeCreditBasedSequenceNumberingViewReaderRef);
                    nativeCreditBasedSequenceNumberingViewReaderRef =
                            createNativeCreditBasedSequenceNumberingViewReader(nativeTaskRef, statusAddress,
                                    partitionId, subPartitionIndex);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                LOG.error("InterruptedException in OmniCreditBasedSequenceNumberingViewReader", e);
            }
        }
    }

    private synchronized int checkIfDataAvailableAndNotifyNetty() {
        if (nativeCreditBasedSequenceNumberingViewReaderRef == -1) {
            return 0;
        }
        int res = getAvailabilityAndBacklog(nativeCreditBasedSequenceNumberingViewReaderRef, getNumCreditsAvailable());
        if (res > 0) {
            flushed = true;
            notifyDataAvailableForNetty();
        }
        return res;
    }

    /**
     * notifyDataAvailable
     */
    public void notifyDataAvailable() {
        LOG.info("Thread = {} do nothing notifyDataAvailable for task: {} ## {} and native red = {}",
                Thread.currentThread().getName(), taskName.substring(0, 15),
                subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef);
    }

    /**
     * notifyDataAvailableForNetty
     */
    public void notifyDataAvailableForNetty() {
        LOG.info("Thread = {} do super notifyDataAvailable for task: {} ## {} and native ref = {}",
                Thread.currentThread().getName(), taskName.substring(0, 15),
                subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef);

        super.notifyDataAvailable();
    }

    public void setNativeTaskRef(long nativeTaskRef) {
        this.nativeTaskRef = nativeTaskRef;
    }

    public long getNativeTaskRef() {
        return nativeTaskRef;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * getAvailabilityAndBacklog
     *
     * @return ResultSubpartitionView.AvailabilityWithBacklog
     */
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        if (nativeCreditBasedSequenceNumberingViewReaderRef == -1) {
            return new ResultSubpartitionView.AvailabilityWithBacklog(false, 0);
        }
        int backlog = this.bufferInfos.size();

        boolean isAvailable = false;
        if (getNumCreditsAvailable() > 0) {
            isAvailable = true;
        } else {
            LOG.info("OmniCreditBasedSequenceNumberingViewReader IS not available, backlog = {} and "
                    + "available = {} of {}## {} and native ref = {}, thread = {}", backlog, isAvailable,
                    taskName.substring(0, 15), subPartitionIndex,
                    nativeCreditBasedSequenceNumberingViewReaderRef, Thread.currentThread().getName());
        }
        return new ResultSubpartitionView.AvailabilityWithBacklog(isAvailable, backlog);
    }


    static class BufferInfo {
        private NetworkBuffer networkBuffer;
        private long address;
        private int length;

        public BufferInfo(NetworkBuffer networkBuffer, long address, int length) {
            this.networkBuffer = networkBuffer;
            this.address = address;
            this.length = length;
        }

        public long getAddress() {
            return address;
        }

        public int getLength() {
            return length;
        }

        public NetworkBuffer getNetworkBuffer() {
            return networkBuffer;
        }
    }


    @Override
    public InputChannel.BufferAndAvailability getNextBuffer() throws IOException {
        if (numCreditsAvailable > 0) {
            InputChannel.BufferAndAvailability bufferAndAvailability = doGetNextBuffer();

            if (bufferAndAvailability != null) {
                return bufferAndAvailability;
            } else {
                // get next buffer from c++
                int readElementNum = getNextBuffer(nativeCreditBasedSequenceNumberingViewReaderRef);
                if (readElementNum > 0) {
                    // decode the buffer
                    decodeReadBuffer(readElementNum);
                    return doGetNextBuffer();
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    private InputChannel.BufferAndAvailability doGetNextBuffer() throws IOException {
        if (bufferInfos.size() > 0) {
            flushed = false;
            numCreditsAvailable--;
            BufferInfo bufferInfo = bufferInfos.remove(0);
            Buffer.DataType nextDataType = bufferInfos.size() > 0 ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.NONE;

            return new InputChannel.BufferAndAvailability(
                    bufferInfo.getNetworkBuffer(),
                    nextDataType,
                    bufferInfos.size(),
                    sequenceNumber.getAndIncrement());
        } else {
            return null;
        }
    }

    private void decodeReadBuffer(int readElementNum) {
        outputBuffer.position(0);
        outputBuffer.order(ByteOrder.LITTLE_ENDIAN);
        // decode the buffer
        for (int i = 0; i < readElementNum; i++) {
            long address = outputBuffer.getLong();
            int length = outputBuffer.getInt();
            // this means this is an event
            if (address == -1) {
                int eventType = length;
                AbstractEvent event = getEventById(eventType);
                LOG.info("[{}###{}]----------------got a event with Event type: {} for natve ref {}",
                        taskName.substring(0, 15),
                        subPartitionIndex, event, nativeCreditBasedSequenceNumberingViewReaderRef);

                try {
                    ByteBuffer eventBuffer = EventSerializer.toSerializedEvent(event);
                    MemorySegment memorySegment = MemorySegmentFactory.wrap(eventBuffer.array());
                    NetworkBuffer buffer = new NetworkBuffer(
                            memorySegment,
                            FreeingBufferRecycler.INSTANCE,
                            Buffer.DataType.EVENT_BUFFER);
                    buffer.setReaderIndex(0);
                    buffer.setSize(eventBuffer.remaining());
                    bufferInfos.add(new BufferInfo(buffer, address, length));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                ByteBuffer resultBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, length);
                resultBuffer.order(ByteOrder.BIG_ENDIAN);
                MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(resultBuffer);
                NativeBufferRecycler nativeBufferRecycler =
                        NativeBufferRecycler.getInstance(nativeCreditBasedSequenceNumberingViewReaderRef);
                nativeBufferRecycler.registerMemorySegment(memorySegment, address);

                NetworkBuffer buffer = new NetworkBuffer(memorySegment, nativeBufferRecycler);

                buffer.setReaderIndex(0);
                buffer.setSize(length);
                bufferInfos.add(new BufferInfo(buffer, address, length));
            }
        }
        // reset the outputBuffer
        outputBuffer.clear();
    }


    private AbstractEvent getEventById(int type) {
        if (type == 0) {
            return EndOfPartitionEvent.INSTANCE;
        } else if (type == 1) {
            return null;
        } else if (type == 2) {
            return EndOfSuperstepEvent.INSTANCE;
        } else if (type == 5) {
            return EndOfChannelStateEvent.INSTANCE;
        } else if (type == 8) {
            return END_OF_DATA;
        } else {
            return null;
        }
    }

    /**
     * stop
     */
    public void stop() {
        running = false;
        int res = checkIfDataAvailableAndNotifyNetty();
        while (res > 0) {
            LOG.info("OmniCreditBasedSequenceNumberingViewReader of {}## "
                            + "{}............................find data during stop......data size= "
                            + "{}................................",
                    taskName.substring(0, 15),
                    subPartitionIndex, res);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            res = checkIfDataAvailableAndNotifyNetty();
        }

        NativeBufferRecycler.unRegisterInstance(nativeCreditBasedSequenceNumberingViewReaderRef);

        if (nativeCreditBasedSequenceNumberingViewReaderRef != -1) {
            destroyNativeNettyBufferPool(nativeCreditBasedSequenceNumberingViewReaderRef);
        }
        LOG.info("OmniCreditBasedSequenceNumberingViewReader of {}## {} and native ref = {}"
                        + "............................is stopped......................................",
                taskName.substring(0, 15), subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef);

        setNativeCreditBasedSequenceNumberingViewReaderRef(-1);
    }

    private synchronized void setNativeCreditBasedSequenceNumberingViewReaderRef(long value) {
        nativeCreditBasedSequenceNumberingViewReaderRef = value;
    }


    /**
     * releaseAllResources
     *
     * @throws IOException IOException
     */
    public void releaseAllResources() throws IOException {
        super.releaseAllResources();
        LOG.info("--------------releaseAllResources for task: {} ## {} and native ref = {}",
                taskName.substring(0, 15), subPartitionIndex,
                nativeCreditBasedSequenceNumberingViewReaderRef);
        stop();
    }


    /**
     * resumeConsumption
     */
    public void resumeConsumption() {
        if (initialCredit == 0) {
            // reset available credit if no exclusive buffer is available at the
            // consumer side for all floating buffers must have been released
            numCreditsAvailable = 0;
        }
        super.resumeConsumption();
    }

    /**
     * needAnnounceBacklog
     *
     * @return boolean
     */
    public boolean needAnnounceBacklog() {
        return initialCredit == 0 && numCreditsAvailable == 0;
    }

    /**
     * addCredit
     *
     * @param creditDeltas creditDeltas
     */
    public void addCredit(int creditDeltas) {
        LOG.info("got a credit from client for task: {} ## {}, credit = {} , addedCredit = {}",
                taskName.substring(0, 15),
                subPartitionIndex,
                numCreditsAvailable,
                creditDeltas);

        numCreditsAvailable += creditDeltas;
    }

    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    /**
     * getAvailabilityAndBacklog
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     * @param numCreditsAvailable numCreditsAvailable
     * @return int
     */
    public native int getAvailabilityAndBacklog(
            long nativeCreditBasedSequenceNumberingViewReaderRef,
            int numCreditsAvailable);

    /**
     * getNextBuffer
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     * @return int
     */
    public native int getNextBuffer(long nativeCreditBasedSequenceNumberingViewReaderRef);

    // create native credit based sequence numbering view reader, this should be a native method
    /**
     * createNativeCreditBasedSequenceNumberingViewReader
     *
     * @param nativeTaskRef nativeTaskRef
     * @param statusAddress statusAddress
     * @param partitionId partitionId
     * @param subPartitionIndex subPartitionIndex
     * @return long
     */
    public native long createNativeCreditBasedSequenceNumberingViewReader(
            long nativeTaskRef,
            long statusAddress,
            String partitionId,
            int subPartitionIndex);

    /**
     * callFirstDataAvailableNotification
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     */
    public native void firstDataAvailableNotification(long nativeCreditBasedSequenceNumberingViewReaderRef);

    /**
     * destroyNativeNettyBufferPool
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     */
    private native void destroyNativeNettyBufferPool(long nativeCreditBasedSequenceNumberingViewReaderRef);
}