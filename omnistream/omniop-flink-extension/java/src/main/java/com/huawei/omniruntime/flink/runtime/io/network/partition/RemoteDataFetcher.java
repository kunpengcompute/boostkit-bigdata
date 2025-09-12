package com.huawei.omniruntime.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.OmniRemoteInputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * RemoteDataFetcher
 *
 * @version 1.0.0
 * @since 2025/04/25
 */
public class RemoteDataFetcher implements Runnable {
    /**
     * LOG
     */
    protected static final Logger LOG = LoggerFactory.getLogger(RemoteDataFetcher.class);

    long nativeTaskRef;

    List<OmniRemoteInputChannel> remoteInputChannels;
    private String taskName;
    private volatile boolean running = true;

    public RemoteDataFetcher(List<OmniRemoteInputChannel> remoteInputChannels, long nativeTaskRef, String taskName) {
        this.remoteInputChannels = remoteInputChannels;
        this.nativeTaskRef = nativeTaskRef;
        this.taskName = taskName;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("RemoteDataFetcher-----> for task: " + taskName);
        buildRemoteConnection();
        while (running) {
            try {
                boolean hasDataSent = sendData();
                if (!hasDataSent) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                LOG.error("Error sleeping", e);
            }
        }
    }

    /**
     * finishRunning
     */
    public void finishRunning() {
        LOG.info("stop RemoteDataFetcher thread......................................for {}", taskName);
        LOG.info("before stop RemoteDataFetcher thread check "
                        + "if data is still available.............................for {}", taskName);
        running = false;
        while (sendData()) {
            LOG.debug("data is still available, keep sending data....................................for {}", taskName);
        }
        int received = remoteInputChannels.stream().filter(r->r.isDataReceived()).collect(Collectors.toList()).size();
        int notreceived = remoteInputChannels.stream().filter(r->!r.isDataReceived()).collect(Collectors.toList()).size();

        LOG.info(" stop RemoteDataFetcher thread completely......................................for {},---->received = {},--- notreceived = {}", taskName,received,notreceived);
    }

    /**
     * buildRemoteConnection
     */
    public void buildRemoteConnection() {
        LOG.info("buildRemoteConnection for task: {} with {} remote channels", taskName, remoteInputChannels.size());
        for (OmniRemoteInputChannel remoteInputChannel : remoteInputChannels) {
            if (!remoteInputChannel.isConnected()) {
                try {
                    LOG.info("buildRemoteConnection for task: {} remotechannel = {}", taskName,
                            remoteInputChannel.getRemoteInputChannel());
                    remoteInputChannel.getRemoteInputChannel().requestSubpartition();
                    remoteInputChannel.setConnected(true);
                } catch (IOException | InterruptedException e) {
                    LOG.error("Error requesting subpartition", e);
                }
            }
        }
    }

    /**
     * sendData
     *
     * @return boolean
     */
    public boolean sendData() {
        boolean hasData = false;
        for (OmniRemoteInputChannel remoteInputChannel : remoteInputChannels) {
            if (remoteInputChannel.isConnected()) {
                try {
                    Optional<InputChannel.BufferAndAvailability> bufferAndAvailabilityOptional =
                            remoteInputChannel.getNextBuffer();
                    if (bufferAndAvailabilityOptional.isPresent()) {
                        InputChannel.BufferAndAvailability bufferAndAvailability = bufferAndAvailabilityOptional.get();
                        Buffer buffer = bufferAndAvailability.buffer();
                        int inputGateIndex = remoteInputChannel.getGateIndex();
                        int channelIndex = remoteInputChannel.getChannelIndex();
                        // sent buffer to C++ side
                        boolean isBuffer = buffer.isBuffer();
                        long bufferAddress = -1;
                        int bufferLength = 0;
                        if (!isBuffer) {
                            // event
                            ByteBuffer eventBuffer = ByteBuffer.wrap(buffer.getMemorySegment().getHeapMemory());
                            eventBuffer.order(ByteOrder.BIG_ENDIAN);
                            int eventType = eventBuffer.getInt();
                            bufferLength = eventType;
                            LOG.info("{}###{} dataFetcher got an event  ::: Event type: {}",
                                    taskName, remoteInputChannel.getChannelIndex(), eventType);
                        } else {
                            bufferAddress = buffer.getMemorySegment().getAddress();
                            bufferLength = bufferAndAvailability.buffer().getSize();
                        }
                        int sequenceNumber = bufferAndAvailability.getSequenceNumber();

                        this.notifyRemoteDataAvailable(
                                nativeTaskRef,
                                inputGateIndex,
                                channelIndex,
                                bufferAddress,
                                bufferLength,
                                sequenceNumber);

                        buffer.recycleBuffer();
                        hasData = true;
                        remoteInputChannel.setDataReceived(true);
                    }
                } catch (IOException | RuntimeException | InterruptedException e) {
                    LOG.error("Error getting next buffer for {}", taskName, e);
                    running = false;
                }
            }
        }
        return hasData;
    }


    /**
     * notifyRemoteDataAvailable
     *
     * @param nativeTaskRef nativeTaskRef
     * @param inputGateIndex inputGateIndex
     * @param channelIndex channelIndex
     * @param bufferAddress bufferAddress
     * @param bufferLength bufferLength
     * @param sequenceNumber sequenceNumber
     */
    public native void notifyRemoteDataAvailable(
            long nativeTaskRef,
            int inputGateIndex,
            int channelIndex,
            long bufferAddress,
            int bufferLength,
            int sequenceNumber);
}
